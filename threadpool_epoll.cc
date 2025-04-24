#include <vector>
#include <chrono>
#include <csignal>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/sysinfo.h>

#include "mysql/plugin.h"
#include "my_thread.h"
#include "sql/sql_thd_internal_api.h"
#include "mysql/thread_pool_priv.h"


static void* tp_thread_start(void *p);
static MYSQL_PLUGIN threadpool_epoll_plugin;

static unsigned int my_total_threadpools = 0;
static MYSQL_SYSVAR_UINT(
  total_threadpools, my_total_threadpools,
  PLUGIN_VAR_READONLY | PLUGIN_VAR_OPCMDARG,
  "Total EPOLL threadpools. Zero defaults to number cores available",
  NULL, NULL, 0, 0, 0xFFFF, 0);

static unsigned int my_max_threads_per_pool = 1;
static MYSQL_SYSVAR_UINT(
  max_threads_per_pool, my_max_threads_per_pool,
  PLUGIN_VAR_READONLY | PLUGIN_VAR_OPCMDARG,
  "Maximum threads per pool",
  NULL, NULL, 8, 1, 0xFFFF, 0);

static unsigned int my_min_waiting_threads_per_pool = 2;
static MYSQL_SYSVAR_UINT(
  min_waiting_threads_per_pool, my_min_waiting_threads_per_pool,
  PLUGIN_VAR_READONLY | PLUGIN_VAR_OPCMDARG,
  "Minimum threads waiting for client io per pool",
  NULL, NULL, 2, 1, 0xFFFF, 0);

static SYS_VAR *threadpool_epoll_system_variables[] = {
    MYSQL_SYSVAR(total_threadpools),
    MYSQL_SYSVAR(max_threads_per_pool),
    MYSQL_SYSVAR(min_waiting_threads_per_pool),
    nullptr};

using namespace std;

struct Threadpool {
  int epfd = -1;                  /* epoll fd used by pool */
  int evfd = -1;                  /* eventfd used to shutdown the threads */

  struct Threads_state {
    uint32_t count = 0;           /* total threads */
    uint32_t epoll_waiting = 0;   /* total threads waiting on epoll */
    uint32_t lock_waiting = 0;    /* total threads waiting on a lock */
    uint32_t padding = 0;         /* Make Threads_state 16 bytes */
  };

  atomic<Threads_state> threads_state;

  Threadpool();
  ~Threadpool();

  // do nothing copy constructor allows us to be contained in a vector;
  Threadpool(const Threadpool &);

  // do nothing assignment operator allows us to be contained in a vector;
  Threadpool& operator=(const Threadpool&);

  int initialize();
  void teardown();
  int spawn_thread();
  void thread_loop();
};

struct Tp_ep_client_event {
  Threadpool &tp;
  THD *thd;
  bool do_handshake;
  bool in_lock_wait;

  Tp_ep_client_event(Threadpool &tp_in, THD *thd_in)
    : tp(tp_in), thd(thd_in), do_handshake(true), in_lock_wait(false) {}

  void readd_to_epoll() {
    epoll_event evt;
    evt.events = EPOLLIN | EPOLLONESHOT;
    evt.data.ptr = this;
    if (epoll_ctl(tp.epfd, EPOLL_CTL_MOD, thd_get_fd(thd), &evt)) {
      // this shouldn't happen
      std::raise(SIGABRT);
    }
  }

  void del_from_epoll() {
    if (epoll_ctl(tp.epfd, EPOLL_CTL_DEL, thd_get_fd(thd), nullptr)) {
      // this shouldn't happen
      std::raise(SIGABRT);
    }
  }

  void process() {
    char thread_top = 0;
    if (do_handshake) {
      do_handshake = false;
      thd_init(thd, &thread_top);
      if (!thd_prepare_connection(thd)) {
        // successfully processed. re-add to epoll
        readd_to_epoll();
        return;
      }
      increment_aborted_connects();
      del_from_epoll();
    } else {
      thd_set_thread_stack(thd, &thread_top);
      thd_store_globals(thd);
      if (thd_connection_alive(thd) && !do_command(thd)) {
        // successfully processed. re-add to epoll
        readd_to_epoll();
        return;
      }
      del_from_epoll();
      end_connection(thd);
    }
    close_connection(thd, 0, false, false);
    remove_ssl_err_thread_state();
    destroy_thd(thd, false);
    delete this;
    dec_connection_count();
  }
};



Threadpool::Threadpool() {
}

Threadpool::~Threadpool() {
  epoll_ctl(epfd, EPOLL_CTL_DEL, evfd, NULL);
  if (evfd >= 0) close(evfd);
  if (epfd >= 0) close(epfd);
}

// a do-nothing copy constructor ia so we can embed inside aa vector
Threadpool::Threadpool(const Threadpool &) {
}
// a do-nothing assignment operator ia so we can embed inside aa vector
Threadpool& Threadpool::operator=(const Threadpool&) {
  return *this;
}

int Threadpool::initialize() {
  if ((epfd = epoll_create(1)) == -1)
    return errno;
  
  if ((evfd = eventfd(0, 0)) == -1)
    return errno;
  
  epoll_event epev;
  epev.events = EPOLLIN;
  epev.data.ptr = nullptr;
  if (epoll_ctl(epfd, EPOLL_CTL_ADD, evfd, &epev) == -1)
    return errno;

  for (size_t i = 0; i < my_min_waiting_threads_per_pool; i++) {
    Threads_state state_old, state_new;
    do {
      state_old = state_new = threads_state.load();
      state_new.count++;
    } while (!threads_state.compare_exchange_weak(state_old, state_new,
                                                  memory_order_relaxed));
    int res = spawn_thread();
    if (res) {
      // thread not created, decrement count and return error
      do {
        state_old = state_new = threads_state.load();
        state_new.count--;
      } while (!threads_state.compare_exchange_weak(state_old, state_new,
                                                    memory_order_relaxed));
      return res;
    }
  }
  return 0;
}

void Threadpool::teardown() {
  while (threads_state.load().count) {
    if (threads_state.load().count == 1) {
      // this might be our client thread doing the teardown. if so skip it.
      THD *thd = thd_get_current_thd();
      if (thd) {
        Tp_ep_client_event *event = (Tp_ep_client_event*)thd_get_scheduler_data(thd);
        if (event && &event->tp == this)
          return;
      }
    }
    uint64_t val = 1;
    if (write(evfd, &val, sizeof(val)) != 8) 
      std::raise(SIGABRT);
  }
}


int Threadpool::spawn_thread() {
  my_thread_handle thread;
  my_thread_attr_t attr;
  my_thread_attr_init(&attr);
  my_thread_attr_setdetachstate(&attr, MY_THREAD_CREATE_DETACHED);
  int res = my_thread_create(&thread, &attr, tp_thread_start, this);
  my_thread_attr_destroy(&attr);
  return res;
}

void Threadpool::thread_loop() {
  epoll_event evt;
  Threads_state state_old, state_new;
  do {
    state_old = state_new = threads_state.load();
    state_new.epoll_waiting++;
  } while (!threads_state.compare_exchange_weak(state_old, state_new,
                                                memory_order_relaxed));
  while (true) {
    int cnt = epoll_wait(epfd, &evt, 1, -1);
    if (cnt == -1) {
      if (errno == EINTR) {
        continue;
      } else {
        my_plugin_log_message(&threadpool_epoll_plugin, MY_ERROR_LEVEL,
          "unexpected errno %d from epoll_wait. raising SIGABRT", errno);
        std::raise(SIGABRT);
      }
    }
    if (evt.data.ptr == nullptr) {
      // this is our server shutdown event, read the eventfd, decrement count
      // and quit thread
      uint64_t val;
      if (read(evfd, &val, sizeof(val)) != 8) 
        std::raise(SIGABRT);
      do {
        state_old = state_new = threads_state.load();
        state_new.count--;
        state_new.epoll_waiting--;
      } while (!threads_state.compare_exchange_weak(state_old, state_new,
                                                    memory_order_relaxed));
      return;
    }
    bool bspawn_thread;
    do {
      state_old = state_new = threads_state.load();
      state_new.epoll_waiting--;
      if (state_new.epoll_waiting < my_min_waiting_threads_per_pool &&
          state_new.count < my_max_threads_per_pool) {
        bspawn_thread = true;
        state_new.count++;
      } else {
        bspawn_thread = false;
      }
    } while (!threads_state.compare_exchange_weak(state_old, state_new,
                                                  memory_order_relaxed));
    if (bspawn_thread) {
      int res = spawn_thread();
      if (res) {
        my_plugin_log_message(&threadpool_epoll_plugin, MY_ERROR_LEVEL,
          "errno %d from spawn_thread. raising SIGABRT", errno);
        // couldn't spawn thread likely to to low resources. Decrement count
        // since no thread was create
        do {
          state_old = state_new = threads_state.load();
          state_new.count--;
        } while (!threads_state.compare_exchange_weak(state_old, state_new,
                                                      memory_order_relaxed));
      }
    }

    ((Tp_ep_client_event*)evt.data.ptr)->process();
    
    // keeps us in the range of min_threads_epoll_waiting or 
    // min_threads_epoll_waiting + 1
    bool thread_die;
    do {
      state_old = state_new = threads_state.load();
      if (state_new.epoll_waiting + 1 > my_min_waiting_threads_per_pool) {
        // threads_epoll_waiting would become 2 or more than my_min_waiting_threads_per_pool.
        // So thread should die.
        state_new.count--;
        thread_die = true;
      } else {
        state_new.epoll_waiting++;
        thread_die = false;
      }
    } while (!threads_state.compare_exchange_weak(state_old, state_new,
                                                  memory_order_relaxed));
    if (thread_die) return;
  }
}


static void* tp_thread_start(void *p) {
  Threadpool *tp = (Threadpool*)p;

  if (my_thread_init()) {
    my_plugin_log_message(&threadpool_epoll_plugin, MY_ERROR_LEVEL,
      "Threadpool thread failed in my_thread_init()");
    Threadpool::Threads_state state_old, state_new;
    do {
      state_old = state_new = tp->threads_state;
      state_new.count--;
    } while (!tp->threads_state.compare_exchange_weak(state_old, state_new,
                                                      memory_order_relaxed));
    return nullptr;
  }

  tp->thread_loop();

  my_thread_end();
  return nullptr;
}

static vector<Threadpool> threadpools;
static atomic<size_t> next_threadpool;

bool tp_ep_add_connection(Channel_info *channel_info) {
  // first assign this connection to a threadpool
  size_t next;
  size_t nextnext;
  do {
     next = next_threadpool.load(memory_order_relaxed);
     nextnext = next + 1;
     if (nextnext == threadpools.size()) nextnext = 0;
  } while (!next_threadpool.compare_exchange_weak(next, nextnext,
                                                  memory_order_relaxed));

  Threadpool &tp = threadpools[next];
  THD *thd = create_thd(channel_info);
  if (thd == nullptr) {
    increment_aborted_connects();
    Connection_handler_manager::dec_connection_count();
    return true;
  }
  destroy_channel_info(channel_info);

  Tp_ep_client_event *tp_ep_client_event = new Tp_ep_client_event(tp, thd);
  thd_set_scheduler_data(thd, tp_ep_client_event);
  epoll_event evt;
  evt.events = EPOLLOUT | EPOLLONESHOT;
  evt.data.ptr = tp_ep_client_event;
  if (epoll_ctl(tp.epfd, EPOLL_CTL_ADD, thd_get_fd(thd), &evt)) {
    my_plugin_log_message(&threadpool_epoll_plugin, MY_ERROR_LEVEL,
      "Error %d in tp_ep_add_connection", errno);
  }

  return false;
}

void tp_ep_end() {
  for (Threadpool &tp : threadpools) tp.teardown();
}

Connection_handler_functions tp_ep_conn_handler = {
  UINT_MAX32,
  tp_ep_add_connection,
  tp_ep_end
};


void tp_ep_thd_wait_begin(THD *thd, int wait_type) {
  if (!thd) {
    return;
  }
  switch (wait_type) {
    case THD_WAIT_ROW_LOCK:
    case THD_WAIT_GLOBAL_LOCK:
    case THD_WAIT_META_DATA_LOCK:
    case THD_WAIT_TABLE_LOCK:
    case THD_WAIT_USER_LOCK:
    {
      /*
      This code prevents all possible client threadpool threads being stuck in
      waiting for locks and therefore a client that can clear the lock(s) doesn't
      have an available client thread to continue it's transaction, creating a
      resource deadlock. So when the count of locked threads is the same as
      threads count, create another thread so the holder(s) of the lock(s) has a
      chance to continue its transaction and unstick the server.
      */
      Tp_ep_client_event *event = (Tp_ep_client_event*)thd_get_scheduler_data(thd);
      if (event) {
        Threadpool &tp = event->tp;
        event->in_lock_wait = true; // mark as waiting for lock
        Threadpool::Threads_state state_old, state_new;
        bool bspawn_thread;
        do {
          state_old = state_new = tp.threads_state;
          state_new.lock_waiting++;
          if (state_new.count == state_new.lock_waiting) {
            state_new.count++;
            bspawn_thread = true;
          } else {
            bspawn_thread = false;
          }
        } while (!tp.threads_state.compare_exchange_weak(state_old, state_new,
                                                         memory_order_relaxed));

        if (bspawn_thread) {
          // if all threads are waiting on locks, spawn another thread
          // so we can process the connection(s) holding the lock(s)
          int res = tp.spawn_thread();
          if (res) {
            my_plugin_log_message(&threadpool_epoll_plugin, MY_ERROR_LEVEL,
              "Error %d in tp_ep_thd_wait_begin()", res);
            do {
              state_old = state_new = tp.threads_state;
              state_new.count--;
            } while (!tp.threads_state.compare_exchange_weak(state_old, state_new,
                                                             memory_order_relaxed));
          }
        }
      }
      break;
    }
    default:
      break;
  }
}

void tp_ep_thd_wait_end(THD *thd) {
  if (!thd) {
    return;
  }
  Tp_ep_client_event *event = (Tp_ep_client_event*)thd_get_scheduler_data(thd);
  if (event && event->in_lock_wait) {
    Threadpool &tp = event->tp;
    event->in_lock_wait = false;
    Threadpool::Threads_state state_old, state_new;
    do {
      state_old = state_new = tp.threads_state;
      state_new.lock_waiting--;
    } while (!tp.threads_state.compare_exchange_weak(state_old, state_new,
                                                     memory_order_relaxed));
  }
}

void tp_ep_post_kill_notification(THD *thd [[maybe_unused]]) {

}

THD_event_functions tp_ep_thd_event = {
  tp_ep_thd_wait_begin,
  tp_ep_thd_wait_end,
  tp_ep_post_kill_notification
};


static int tp_ep_plugin_init(MYSQL_PLUGIN plugin_ref) {
  DBUG_TRACE;
  threadpool_epoll_plugin = plugin_ref;

  if (my_total_threadpools == 0) {
    my_total_threadpools = sysconf(_SC_NPROCESSORS_ONLN);
  }

  if (my_max_threads_per_pool < my_min_waiting_threads_per_pool) {
    my_min_waiting_threads_per_pool = my_max_threads_per_pool;
  }

  threadpools.resize(my_total_threadpools);

  for (Threadpool &tp : threadpools) {
    int err = tp.initialize();
    if (err) {
      my_plugin_log_message(&threadpool_epoll_plugin, MY_ERROR_LEVEL,
        "errno %d from Threadpool::initialize()", errno);
      goto errhandle;
    }
  }

  if (my_connection_handler_set(&tp_ep_conn_handler, &tp_ep_thd_event))
    goto errhandle;
  return 0;
errhandle:
  threadpools.resize(0);
  return 1;
}

static int tp_ep_plugin_deinit(MYSQL_PLUGIN plugin_ref) {
  threadpools.resize(0);
  return 0;
}

struct st_mysql_daemon threadpool_epoll_plugin_daemom = {MYSQL_DAEMON_INTERFACE_VERSION};

mysql_declare_plugin(threadpool_epoll) {
    MYSQL_DAEMON_PLUGIN,
    &threadpool_epoll_plugin_daemom,
    "threadpool_epoll",
    "Damien Katz",
    "threadpool and epoll connection handler",
    PLUGIN_LICENSE_PROPRIETARY,
    tp_ep_plugin_init,    /* Plugin Init */
    nullptr,              /* Plugin Check uninstall */
    tp_ep_plugin_deinit,  /* Plugin Deinit */
    0x0100,               /* 1.0 */
    nullptr,              /* status variables */
    threadpool_epoll_system_variables,              /* system variables */
    nullptr,              /* config options */
    0,                    /* flags */
} mysql_declare_plugin_end;

