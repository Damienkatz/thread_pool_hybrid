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
  int epfd = -1;
  int evfd = -1;                        /* used to shutdown the threadpool */
  atomic<size_t> threads_count;         /* total threads */
  atomic<size_t> threads_epoll_waiting; /* total threads waiting on epoll */
  atomic<size_t> threads_lock_waiting;  /* total threads waiting on a lock */

  Threadpool(const Threadpool &);
  Threadpool();
  Threadpool& operator=(const Threadpool&);
  ~Threadpool();
  int initialize();
  void teardown();
  void thread_loop();
};

struct TpEpEvent {
  Threadpool &tp;
  TpEpEvent(Threadpool &tp_in) : tp(tp_in) {}
  // This method processes events. If it returns true it means
  // the server is shutting down.
  virtual bool process() = 0;
  virtual ~TpEpEvent() {}
};

struct TpEpClientEvent : public TpEpEvent {
  THD *thd;
  bool do_handshake;
  bool in_lock_wait;

  TpEpClientEvent(Threadpool &tp_in, THD *thd_in)
    : TpEpEvent(tp_in), thd(thd_in), do_handshake(true), in_lock_wait(false) {}
  
  ~TpEpClientEvent() {}

  void readd_to_epoll() {
    epoll_event evt;
    evt.events = EPOLLIN | EPOLLRDHUP | EPOLLONESHOT;
    evt.data.ptr = this;
    if (epoll_ctl(tp.epfd, EPOLL_CTL_MOD, thd_get_fd(thd), &evt)) {
      exit(1);
    }
  }

  void del_from_epoll() {
    if (epoll_ctl(tp.epfd, EPOLL_CTL_DEL, thd_get_fd(thd), nullptr)) {
      exit(1);
    }
  }

  bool process() override {
    char thread_top = 0;
    if (do_handshake) {
      do_handshake = false;
      thd_init(thd, &thread_top);
      if (!thd_prepare_connection(thd)) {
        // successfully processed. re-add to epoll
        readd_to_epoll();
        return false;
      }
      increment_aborted_connects();
      del_from_epoll();
    } else {
      thd_set_thread_stack(thd, &thread_top);
      thd_store_globals(thd);
      if (thd_connection_alive(thd) && !do_command(thd)) {
        // successfully processed. re-add to epoll
        readd_to_epoll();
        return false;
      }
      del_from_epoll();
      end_connection(thd);
    }
    close_connection(thd, 0, false, false);
    remove_ssl_err_thread_state();
    destroy_thd(thd, false);
    delete this;
    dec_connection_count();
    return false;
  }
};

struct TpEpShutdownEvent : public TpEpEvent {
  TpEpShutdownEvent(Threadpool &tp_in) : TpEpEvent(tp_in) {}

  bool process() override {
    epoll_event evt;
    evt.events = EPOLLIN | EPOLLONESHOT;
    evt.data.ptr = this;
    if (epoll_ctl(tp.epfd, EPOLL_CTL_MOD, tp.evfd, &evt)) {
      my_plugin_log_message(&threadpool_epoll_plugin, MY_ERROR_LEVEL,
        "errno %d from epoll_ctl(...) in TpEpShutdownEvent::process()", errno);
    }
    return true;
  }
};



Threadpool::Threadpool(const Threadpool &) {
}

Threadpool::Threadpool() {
}

Threadpool& Threadpool::operator=(const Threadpool&) {
  return *this;
}

Threadpool::~Threadpool() {
  //teardown();
}

int Threadpool::initialize() {
  if ((epfd = epoll_create(1)) == -1)
    return errno;
  
  if ((evfd = eventfd(0, 0)) == -1)
    return errno;
  
  TpEpShutdownEvent* tp_ep_shutdown_event = new TpEpShutdownEvent(*this);
  epoll_event epev;
  epev.events = EPOLLIN | EPOLLONESHOT;
  epev.data.ptr = tp_ep_shutdown_event;
  if (epoll_ctl(epfd, EPOLL_CTL_ADD, evfd, &epev) == -1)
    return errno;

  for (size_t i = 0; i < my_min_waiting_threads_per_pool; i++) {
    my_thread_handle thread;
    my_thread_attr_t attr;
    my_thread_attr_init(&attr);
    my_thread_attr_setdetachstate(&attr, MY_THREAD_CREATE_DETACHED);
    int res = my_thread_create(&thread, &attr, tp_thread_start, this);
    my_thread_attr_destroy(&attr);
    if (res)
      return errno;
  }
  return 0;
}

void Threadpool::teardown() {
  while(threads_count) {
    size_t val = 1;
    static_assert(sizeof(val) == 8);
    assert(write(evfd, &val, sizeof(val)) == 8);
  }
  epoll_ctl(epfd, EPOLL_CTL_DEL, evfd, NULL);
  if (evfd >= 0) close(evfd);
  if (epfd >= 0) close(epfd);
}


void Threadpool::thread_loop() {
  epoll_event evt;
  threads_epoll_waiting.fetch_add(1, memory_order_relaxed);
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
    assert(cnt == 1);
    size_t waiting;
    bool spawn_thread;
    do {
      spawn_thread = false;
      waiting = threads_epoll_waiting;
      if (waiting - 1 < my_min_waiting_threads_per_pool &&
          threads_count < my_max_threads_per_pool) {
        spawn_thread = true;
      }
    } while(!threads_epoll_waiting.compare_exchange_weak(waiting, waiting - 1,
                                                         memory_order_relaxed));
    if (spawn_thread) {
      my_thread_handle thread;
      my_thread_attr_t attr;
      my_thread_attr_init(&attr);
      my_thread_attr_setdetachstate(&attr, MY_THREAD_CREATE_DETACHED);
      int res = my_thread_create(&thread, &attr, tp_thread_start, this);
      my_thread_attr_destroy(&attr);
      if (res) {
        my_plugin_log_message(&threadpool_epoll_plugin, MY_ERROR_LEVEL,
          "errno %d from spawn thread. raising SIGABRT", errno);
        std::raise(SIGABRT);
      }
    }

    if (((TpEpEvent*)evt.data.ptr)->process())
      return; // server shutdown
    
    // keeps us in the range of min_threads_epoll_waiting or 
    // min_threads_epoll_waiting + 1
    do {
      waiting = threads_epoll_waiting;
      if (waiting > my_min_waiting_threads_per_pool)
        return; // thread should die
    } while(!threads_epoll_waiting.compare_exchange_weak(waiting, waiting + 1,
                                                         memory_order_relaxed));
  }
}


static void* tp_thread_start(void *p) {
  Threadpool &tp = *(Threadpool*)p;

  if (my_thread_init()) {
    my_plugin_log_message(&threadpool_epoll_plugin, MY_ERROR_LEVEL,
      "Threadpool thread failed in my_thread_init()");
    return nullptr;
  }
  
  tp.threads_count.fetch_add(1, memory_order_relaxed);

  tp.thread_loop();

  tp.threads_count.fetch_sub(1, memory_order_relaxed);
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
  } while(!next_threadpool.compare_exchange_weak(next, nextnext,
                                                 memory_order_relaxed));

  Threadpool &tp = threadpools[next];
  THD *thd = create_thd(channel_info);
  if (thd == nullptr) {
    increment_aborted_connects();
    Connection_handler_manager::dec_connection_count();
    return true;
  }
  destroy_channel_info(channel_info);

  TpEpClientEvent *tp_ep_client_event = new TpEpClientEvent(tp, thd);
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
  threadpools.resize(0);
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
      TpEpClientEvent *event = (TpEpClientEvent*)thd_get_scheduler_data(thd);
      if (event) {
        event->tp.threads_lock_waiting.fetch_add(1, memory_order_relaxed);
        event->in_lock_wait = true;
        if (event->tp.threads_lock_waiting == event->tp.threads_count) {
          // if all threads are waiting on locks, spawn another thread
          // so we can process the connection(s) holding the lock(s)
          my_thread_handle thread;
          my_thread_attr_t attr;
          my_thread_attr_init(&attr);
          my_thread_attr_setdetachstate(&attr, MY_THREAD_CREATE_DETACHED);
          int res = my_thread_create(&thread, &attr, tp_thread_start, &event->tp);
          my_thread_attr_destroy(&attr);
          if (res) {
            my_plugin_log_message(&threadpool_epoll_plugin, MY_ERROR_LEVEL,
              "Error %d in tp_ep_thd_wait_begin()", errno);
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
  TpEpClientEvent *event = (TpEpClientEvent*)thd_get_scheduler_data(thd);
  if (event && event->in_lock_wait) {
    event->in_lock_wait = false;
    event->tp.threads_lock_waiting.fetch_sub(1, memory_order_relaxed);
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
    nullptr,              /* Plugin Deinit */
    0x0100,               /* 1.0 */
    nullptr,              /* status variables */
    threadpool_epoll_system_variables,              /* system variables */
    nullptr,              /* config options */
    0,                    /* flags */
} mysql_declare_plugin_end;

