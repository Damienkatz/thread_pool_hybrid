#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <vector>
#include <chrono>
#include <csignal>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/sysinfo.h>

#include "mysql/plugin.h"
#include "my_thread.h"
#include "sql/sql_thd_internal_api.h"
#include "mysql/thread_pool_priv.h"

using namespace std;

static MYSQL_PLUGIN threadpool_epoll_plugin;

/******************************************************************************
 * Plugin global variables
 ******************************************************************************/

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
  NULL, NULL, 16, 1, 0xFFFF, 0);

static unsigned int my_min_waiting_threads_per_pool = 4;
static MYSQL_SYSVAR_UINT(
  min_waiting_threads_per_pool, my_min_waiting_threads_per_pool,
  PLUGIN_VAR_READONLY | PLUGIN_VAR_OPCMDARG,
  "Minimum threads waiting for client io per pool",
  NULL, NULL, 4, 1, 0xFFFF, 0);

static SYS_VAR *threadpool_epoll_system_variables[] = {
    MYSQL_SYSVAR(total_threadpools),
    MYSQL_SYSVAR(max_threads_per_pool),
    MYSQL_SYSVAR(min_waiting_threads_per_pool),
    nullptr};

/******************************************************************************
 * Threadpool manages a collection of threads. Threads are per connection
 * until my_max_threads_per_pool reached in which case the threads use
 * epoll_wait.
 ******************************************************************************/

struct Threadpool {
  int epfd = -1;                  /* epoll fd used by pool */
  int evfd = -1;                  /* eventfd used to shutdown the threads */

  struct Threads_state {
    uint32_t count = 0;           /* total threads in Threadpool*/
    uint32_t epoll_waiting = 0;   /* total threads waiting on epoll in Threadpool */
    uint32_t lock_waiting = 0;    /* total threads waiting on a lock in Threadpool */
    uint32_t connection_count = 0;/* total clients connected to the Threadpool */
  };

  atomic<Threads_state> threads_state;

  atomic<bool> shutdown;

  Threadpool();
  ~Threadpool();

  // do nothing copy constructor allows us to be contained in a vector;
  Threadpool(const Threadpool &);

  // do nothing assignment operator allows us to be contained in a vector;
  Threadpool& operator=(const Threadpool&);

  int initialize();
  void shutdown_threads();
  int spawn_thread();
  bool use_thread_per_conn();

  static void* thread_start(void*);
  void thread_loop();
};


/******************************************************************************
 * Tp_ep_client_event class + definitions
 ******************************************************************************/

struct Tp_ep_client_event {
  Threadpool &tp;
  THD *thd;
  bool do_handshake;
  bool in_lock_wait;

  Tp_ep_client_event(Threadpool &tp_in, THD *thd_in)
      : tp(tp_in), thd(thd_in), do_handshake(true), in_lock_wait(false) {
  }

  ~Tp_ep_client_event() {
    Threadpool::Threads_state state_old, state_new;
    do {
      state_old = state_new = tp.threads_state.load();
      state_new.connection_count--;
    } while (!tp.threads_state.compare_exchange_weak(state_old, state_new,
                                                     memory_order_relaxed));

    destroy_thd(thd, false);
    dec_connection_count();
  }

  void readd_to_epoll() {
    epoll_event evt;
    evt.events = EPOLLIN | EPOLLRDHUP | EPOLLONESHOT;
    evt.data.ptr = this;
    if (epoll_ctl(tp.epfd, EPOLL_CTL_MOD, thd_get_fd(thd), &evt)) {
      my_plugin_log_message(&threadpool_epoll_plugin, MY_ERROR_LEVEL,
        "unexpected errno %d from epoll_ctl(EPOLL_CTL_MOD,...). raising SIGABRT", errno);
      std::raise(SIGABRT);
    }
  }

  void del_from_epoll() {
    if (epoll_ctl(tp.epfd, EPOLL_CTL_DEL, thd_get_fd(thd), nullptr)) {
      // normal when fd closed. clean up code will free the
      // Tp_ep_client_event
    }
  }

  void process() {
    char thread_top = 0;
    if (do_handshake) {
      do_handshake = false;
      thd_init(thd, &thread_top);
      if (!thd_prepare_connection(thd)) {
        if (tp.use_thread_per_conn()) {
          // Successful handshake, we have enough threads available.
          // go the use_thread_per_conn path
          goto use_thread_per_conn;
        } else {
          // Successful handshake. more connections than threads. use epoll
          readd_to_epoll();
          return;
        }
      }
      // failed handshake, error path.
      increment_aborted_connects();
      del_from_epoll();
    } else {
      // we get here from epoll_wait having done the handshake previously, 
      // and now to process a command,
      thd_set_thread_stack(thd, &thread_top);
      thd_store_globals(thd);
      goto do_command;
use_thread_per_conn:
      {
      // Here we wait for the descriptor to become read ready or transition
      // to epoll mode
      pollfd pfd[] = {{thd_get_fd(thd), POLLIN | POLLRDHUP, 0},
                      {tp.evfd, POLLIN | POLLRDHUP, 0}};
      int res = poll(pfd, 2, -1);
      if (res == -1) {
        if (errno == EINTR) {
          // interrupt error, wait on poll again.
          goto use_thread_per_conn;
        } else {
          // don't know this error. abort
          my_plugin_log_message(&threadpool_epoll_plugin, MY_ERROR_LEVEL,
            "unexpected errno %d from poll. raising SIGABRT", errno);
          std::raise(SIGABRT);
        }
      }
      if (pfd[1].revents != 0) {
        // clear the event
        uint64_t val;
        if (read(tp.evfd, &val, sizeof(val))) {
          // placate compiler -Wunused-result
        }
      }
      if (pfd[0].revents == 0) {
          // switch to using epoll
          readd_to_epoll();
          return;
      }
      }
do_command:
      if (thd_connection_alive(thd) && !do_command(thd)) {
        // successfully processed. 
        if (tp.use_thread_per_conn()) {
          // enough threads for use_thread_per_conn
          goto use_thread_per_conn;
        } else {
          // switch to using epoll
          readd_to_epoll();
          return;
        }
      }
      // this is the error/stop path
      del_from_epoll();
      end_connection(thd);
    }
    // close the connection, destroy the thd, delete ourself
    close_connection(thd, 0, false, false);
    remove_ssl_err_thread_state();
    delete this;
  }
};


/******************************************************************************
 * Threadpool member definitions
 ******************************************************************************/

Threadpool::Threadpool() : shutdown(false) {
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

bool Threadpool::use_thread_per_conn() {
  if (shutdown.load()) return false;
  return threads_state.load().connection_count <= my_max_threads_per_pool;
}

int Threadpool::initialize() {
  if ((epfd = epoll_create1(EPOLL_CLOEXEC)) == -1)
    return errno;
  
  if ((evfd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK)) == -1)
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

void Threadpool::shutdown_threads() {
  shutdown.store(true);
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
    if (write(evfd, &val, sizeof(val))) {
      // placate compiler -Wunused-result
    }
  }
}

int Threadpool::spawn_thread() {
  my_thread_handle thread;
  my_thread_attr_t attr;
  my_thread_attr_init(&attr);
  my_thread_attr_setdetachstate(&attr, MY_THREAD_CREATE_DETACHED);
  int res = my_thread_create(&thread, &attr, thread_start, this);
  my_thread_attr_destroy(&attr);
  return res;
}

void* Threadpool::thread_start(void *p) {
  Threadpool *tp = (Threadpool*)p;

  if (my_thread_init()) {
    my_plugin_log_message(&threadpool_epoll_plugin, MY_ERROR_LEVEL,
      "Threadpool thread failed in my_thread_init()");
    std::raise(SIGABRT);
  }
  my_thread_self_setname("tp_ep_worker");

  tp->thread_loop();

  my_thread_end();
  return nullptr;
}

void Threadpool::thread_loop() {
  while (true) {
    epoll_event evt;
    Threads_state state_old, state_new;
    // keeps epoll_waiting in the range of my_min_waiting_threads_per_pool or 
    // my_min_waiting_threads_per_pool + 1
    bool thread_die;
    do {
      state_old = state_new = threads_state.load();
      if (shutdown.load() ||
          state_new.epoll_waiting > my_min_waiting_threads_per_pool) {
        // state_new.epoll_waiting would become 2 or more than my_min_waiting_threads_per_pool.
        // So thread should die.
        state_new.count--;
        thread_die = true;
      } else {
        state_new.epoll_waiting++;
        thread_die = false;
      }
    } while (!threads_state.compare_exchange_weak(state_old, state_new,
                                                  memory_order_relaxed));
    if (thread_die)
      return;

wait_again:
    int cnt = epoll_wait(epfd, &evt, 1, -1);
    if (cnt == -1) {
      if (errno == EINTR) {
        // interrupted, wait again
        goto wait_again;
      } else {
        my_plugin_log_message(&threadpool_epoll_plugin, MY_ERROR_LEVEL,
          "unexpected errno %d from epoll_wait. raising SIGABRT", errno);
        std::raise(SIGABRT);
      }
    }
    if (evt.data.ptr == nullptr) {
      if (shutdown.load()) {
        // this is our server shutdown/plugin unload event, read the eventfd,
        // decrement count and quit thread
        uint64_t val;
        if (read(evfd, &val, sizeof(val))) {
          // placate compiler -Wunused-result
        }
        do {
          state_old = state_new = threads_state.load();
          state_new.count--;
          state_new.epoll_waiting--;
        } while (!threads_state.compare_exchange_weak(state_old, state_new,
                                                      memory_order_relaxed));
        return;
      } else {
        // signal to switch to epoll mode. We are already there.
        // read the event so we don't churn
        uint64_t val;
        if (read(evfd, &val, sizeof(val))) {
          // placate compiler -Wunused-result
        }
        goto wait_again;
      }
    }

    bool spawnthread;
    do {
      // we got a regular event. We'll be preoccupied with processing it,
      // so see if we should spawn another thread before we do.
      state_old = state_new = threads_state.load();
      state_new.epoll_waiting--;
      if (!shutdown.load() &&
          state_new.epoll_waiting < my_min_waiting_threads_per_pool &&
          state_new.count < my_max_threads_per_pool) {
        spawnthread = true;
        state_new.count++;
      } else {
        spawnthread = false;
      }
    } while (!threads_state.compare_exchange_weak(state_old, state_new,
                                                  memory_order_relaxed));
    if (spawnthread) {
      int res = spawn_thread();
      if (res) {
        my_plugin_log_message(&threadpool_epoll_plugin, MY_ERROR_LEVEL,
          "errno %d from spawn_thread. raising SIGABRT", errno);
        // couldn't spawn thread likely to to low resources. kill server
        std::raise(SIGABRT);
      }
    }

    ((Tp_ep_client_event*)evt.data.ptr)->process();
  }
}

static vector<Threadpool> threadpools;
static atomic<size_t> next_threadpool;

/******************************************************************************
 * Connection_handler_functions
 ******************************************************************************/

static bool tp_ep_add_connection(Channel_info *channel_info) {
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
    dec_connection_count();
    return true;
  }
  destroy_channel_info(channel_info);
 
  Tp_ep_client_event *client_event = new Tp_ep_client_event(tp, thd);

  Threadpool::Threads_state state_old, state_new;
  do {
    state_old = state_new = tp.threads_state.load();
    state_new.connection_count++; // decremented in dtor of client_event
  } while (!tp.threads_state.compare_exchange_weak(state_old, state_new,
                                                   memory_order_relaxed));
  thd_set_scheduler_data(thd, client_event);
  epoll_event evt;
  evt.events = EPOLLOUT | EPOLLRDHUP| EPOLLONESHOT;
  evt.data.ptr = client_event;
  if (epoll_ctl(tp.epfd, EPOLL_CTL_ADD, thd_get_fd(thd), &evt)) {
    my_plugin_log_message(&threadpool_epoll_plugin, MY_ERROR_LEVEL,
      "Error %d in Tp_ep_client_event constructor", errno);
    delete client_event;
  }
  if (state_new.connection_count == my_max_threads_per_pool + 1) {
    // signal switch to epoll to any threads waiting in poll
    uint64_t val = 1;
    if (write(tp.evfd, &val, sizeof(val))) {
      // placate compiler -Wunused-result
    }
  }

  return false;
}

static void tp_ep_end() {
  // stop all threads
  for (Threadpool &tp : threadpools) tp.shutdown_threads();
}

Connection_handler_functions tp_ep_conn_handler = {
  (uint)get_max_connections(),
  tp_ep_add_connection,
  tp_ep_end
};

/******************************************************************************
 * THD_event_functions
 *****************************************************************************/

static void tp_ep_thd_wait_begin(THD *thd, int wait_type) {
  if (!thd) return;

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

static void tp_ep_thd_wait_end(THD *thd) {
  if (!thd) return;

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

static void tp_ep_post_kill_notification(THD *thd) {
  Tp_ep_client_event *event = (Tp_ep_client_event*)thd_get_scheduler_data(thd);
  if (event) {
    shutdown(thd_get_fd(thd), SHUT_RDWR);
  }
}

THD_event_functions tp_ep_thd_event = {
  tp_ep_thd_wait_begin,
  tp_ep_thd_wait_end,
  tp_ep_post_kill_notification
};

/******************************************************************************
 * Connection handler module initializer/denitializer
 ******************************************************************************/

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

static int tp_ep_plugin_deinit(MYSQL_PLUGIN plugin_ref [[maybe_unused]]) {
  // stop all threads
  for (Threadpool &tp : threadpools) tp.shutdown_threads();
  // free the thread pools
  threadpools.resize(0);
  (void)my_connection_handler_reset();
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
    tp_ep_plugin_init,          /* Plugin Init */
    nullptr,              /* Plugin Check uninstall */
    tp_ep_plugin_deinit,        /* Plugin Deinit */
    0x0100,               /* 1.0 */
    nullptr,              /* status variables */
    threadpool_epoll_system_variables,  /* system variables */
    nullptr,              /* config options */
    0,                    /* flags */
} mysql_declare_plugin_end;

