#include <ctype.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <vector>
#include <chrono>
#include <climits>
#include <sys/epoll.h>
#include <sys/eventfd.h>

#include "mysql/plugin.h"
#include "sql/mysqld.h"              // connection_errors_internal
#include "my_thread.h"
#include "sql/sql_plugin.h"  // st_plugin_int
#include "sql/conn_handler/plugin_connection_handler.h"
#include "sql/sql_thd_internal_api.h"

static void* tp_thread_start(void *p);

static unsigned int my_total_threadpools = 1;
static MYSQL_SYSVAR_UINT(total_threadpools, my_total_threadpools,
  PLUGIN_VAR_READONLY | PLUGIN_VAR_OPCMDARG, "Total EPOLL threadpools",
  NULL, NULL, 1, 0xFFFF, 1, 0);

static unsigned int my_max_threads_per_pool = 1;
static MYSQL_SYSVAR_UINT(max_threads_per_pool, my_max_threads_per_pool,
  PLUGIN_VAR_READONLY | PLUGIN_VAR_OPCMDARG, "Maximum threads per pool",
  NULL, NULL, 1, 0xFFFF, 1, 0);

static unsigned int my_min_waiting_threads_per_pool = 1;
static MYSQL_SYSVAR_UINT(min_waiting_threads_per_pool, my_min_waiting_threads_per_pool,
  PLUGIN_VAR_READONLY | PLUGIN_VAR_OPCMDARG, "Minimum threads waiting for client io per pool",
  NULL, NULL, 1, 0xFFFF, 1, 0);

static int my_max_waiting_thread_time_ns = 1000;
static MYSQL_SYSVAR_INT(max_waiting_thread_time_ns, my_max_waiting_thread_time_ns,
  PLUGIN_VAR_READONLY | PLUGIN_VAR_OPCMDARG, "Maximum time, in ns, an extra thread waits for client io",
  NULL, NULL, -1, INT_MAX, 1000, 0);

static SYS_VAR *threadpool_epoll_system_variables[] = {
    MYSQL_SYSVAR(total_threadpools), MYSQL_SYSVAR(max_threads_per_pool),
    MYSQL_SYSVAR(min_waiting_threads_per_pool), MYSQL_SYSVAR(max_waiting_thread_time_ns), nullptr};

using namespace std;


struct Threadpool {
  int epfd = -1;
  int evfd = -1;                        /* used to shutdown the threadpool */
  atomic<size_t> threads_count;         /* total threads */
  atomic<size_t> threads_epoll_waiting;    /* total threads waiting on epoll */
  atomic<size_t> threads_lock_waiting;  /* total threads waiting on a lock */
  vector<chrono::steady_clock::time_point> ep_waiting_times; /* Start time N threads have been waiting */

  Threadpool(const Threadpool &);
  Threadpool();
  Threadpool& operator=(const Threadpool&);
  ~Threadpool();
  bool thread_timeout() {return false;}
  void thread_loop();
  int initialize();
  void teardown();
};

struct TpEpEvent {
  // This method processes events. If it returns true it means
  // the server is shutting down.
  virtual bool process();
  virtual ~TpEpEvent();
};

struct TpEpClientEvent : public TpEpEvent {
  Threadpool &tp;
  THD *thd;
  bool do_handshake;

  TpEpClientEvent(Threadpool &tpIn, THD *thdIn)
    : tp(tpIn), thd(thdIn), do_handshake(true) {}
  
  ~TpEpClientEvent() {
    destroy_thd(thd);
  }

  void readd_to_epoll() {
    epoll_event evt;
    evt.events = EPOLLIN | EPOLLRDHUP | EPOLLET | EPOLLONESHOT;
    evt.data.ptr = this;
    if (epoll_ctl(tp.epfd, EPOLL_CTL_MOD, thd_get_fd(thd), &evt)) {
        // to do log err
        exit(1);
    }
  }

  bool process() override {
    mysql_socket_set_thread_owner(thd_get_mysql_socket(thd));
    char thread_top;
    if (do_handshake) {
      do_handshake = false;
      thd_init(thd, &thread_top);

      if (!thd_prepare_connection(thd)) {
        readd_to_epoll();
        return false;
      }
    } else {
      if (!do_command(thd)) {
        // successfully processed. re-add to epoll
        readd_to_epoll();
        return false;
      }

      end_connection(thd);
    }
    close_connection(thd, 0, false, false);

    delete this;
    dec_connection_count();
    return false;
  }
};

struct TpEpShutdownEvent : public TpEpEvent {
  Threadpool &tp;
  TpEpShutdownEvent(Threadpool &tpIn) : tp(tpIn) {}

  bool process() override {
    epoll_event evt;
    evt.events = EPOLLIN | EPOLLONESHOT;
    evt.data.ptr = this;
    if (epoll_ctl(tp.epfd, EPOLL_CTL_MOD, tp.evfd, &evt)) {
        // to do log err
        exit(1);
    }
    return true;
  }
};

Threadpool::Threadpool(const Threadpool &) {}

Threadpool::Threadpool() {}

Threadpool& Threadpool::operator=(const Threadpool&) {
  return *this;
}

Threadpool::~Threadpool() {
  teardown();
}

void Threadpool::thread_loop() {
  int err;
  epoll_event evt;
  while (true) {
    ++threads_epoll_waiting;
    err = epoll_wait(epfd, &evt, 1, my_max_waiting_thread_time_ns);
    --threads_epoll_waiting;
    if (err == EINTR) {
      if (thread_timeout()) {
        return;
      } else {
        continue;
      }
    } else if(err) {
      // todo log error
      exit(1);
    }

    TpEpEvent *event = (TpEpEvent*)evt.data.ptr;
    if (event->process()) {
      // server shutdown
      return;
    }
  }
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

  ep_waiting_times.resize(my_max_threads_per_pool, chrono::steady_clock::now());

  for (size_t i = 0; i < my_min_waiting_threads_per_pool; i++) {
    my_thread_handle thread;
    my_thread_attr_t attr;
    my_thread_attr_init(&attr);
    my_thread_attr_setdetachstate(&attr, MY_THREAD_CREATE_DETACHED);
    int res = my_thread_create(&thread, &attr, tp_thread_start, this);
    my_thread_attr_destroy(&attr);
    if (res)
      return res;
    threads_epoll_waiting.wait(i + 1);
  }
  return 0;
}

void Threadpool::teardown() {
  size_t count = threads_count;
  while(count) {
    size_t val = 1;
    static_assert(sizeof(val) == 8);
    assert(write(evfd, &val, sizeof(val)) == 8);
    threads_count.wait(count - 1);
    count = threads_count;
  }
  epoll_ctl(epfd, EPOLL_CTL_DEL, evfd, NULL);
  if (evfd >= 0) close(evfd);
  if (epfd >= 0) close(epfd);
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
  } while(!next_threadpool.compare_exchange_weak(next, nextnext, memory_order_relaxed));

  Threadpool &tp = threadpools[next];
  THD *thd = create_thd(channel_info);
  destroy_channel_info(channel_info);
  if (thd == nullptr) {
    connection_errors_internal++;
    increment_aborted_connects();
    Connection_handler_manager::dec_connection_count();
    return true;
  }
  TpEpClientEvent *tp_ep_client_event = new TpEpClientEvent(tp, thd);

  epoll_event evt;
  evt.events = EPOLLOUT | EPOLLONESHOT;
  evt.data.ptr = tp_ep_client_event;
  if (epoll_ctl(tp.epfd, EPOLL_CTL_ADD, thd_get_fd(thd), &evt)) {
    //todo log err
    return true;
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


void tp_ep_thd_wait_begin(THD */*thd*/, int /*wait_type*/) {

}

void tp_ep_thd_wait_end(THD */*thd*/) {

}

void tp_ep_post_kill_notification(THD */*thd*/) {

}

THD_event_functions tp_ep_thd_event = {
  tp_ep_thd_wait_begin,
  tp_ep_thd_wait_end,
  tp_ep_post_kill_notification
};

static void* tp_thread_start(void *p) {

  Threadpool &tp = *(Threadpool*)p;
  
  tp.threads_count.fetch_add(1, memory_order_relaxed);

  if (my_thread_init()) {
    my_thread_exit(nullptr);
    return nullptr;
  }
  tp.thread_loop();
  tp.threads_count.fetch_sub(1, memory_order_relaxed);
  my_thread_end();
  my_thread_exit(nullptr);
  return nullptr;
}

static int tp_ep_plugin_init(void *p [[maybe_unused]]) {
  DBUG_TRACE;

  if (my_max_threads_per_pool < my_min_waiting_threads_per_pool) {
    my_min_waiting_threads_per_pool = my_max_threads_per_pool;
  }

  threadpools.resize(my_total_threadpools);

  for (Threadpool &tp : threadpools) {
    int err = tp.initialize();
    if (err) {
      //TODO log error
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

static int tp_ep_plugin_deinit(void */*p*/) {
  DBUG_TRACE;

  return 0;
}

struct st_mysql_daemon threadpool_epoll_plugin = {MYSQL_DAEMON_INTERFACE_VERSION};

mysql_declare_plugin(threadpool_epoll) {
    MYSQL_DAEMON_PLUGIN,
    &threadpool_epoll_plugin,
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
   