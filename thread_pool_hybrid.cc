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

namespace Thread_pool_hybrid {

  /**
   * Thread_pool_hybrid implementation.
   * 
   * This is a hybrid thread pool implementation. It uses a pool of N
   * threads that serve M clients. When number of clients is less than the
   * maximum number of threads for a given pool, each client is full time
   * served by its assigned thread, notifed of work via poll. When the number
   * of clients exceeds the maximum number of threads allowed, each thread is 
   * notifed via an eventfd (which is also watched by poll) and the pool
   * converts to waiting on epoll.
   * 
   * This solves the latency of epoll_wait when watching a small number of
   * sockets but preserves the scalability off epoll_wait on a large number
   * of sockets.
   * 
   * The default number of pools is the number of cores and each thread pool
   * has a default maximum of 16 threads. 
   */

using namespace std;

static MYSQL_PLUGIN thread_pool_hybrid_plugin;

/******************************************************************************
 * Plugin global variables
 ******************************************************************************/

static unsigned int my_total_thread_pools = 0;
static MYSQL_SYSVAR_UINT(
  total_thread_pools, my_total_thread_pools,
  PLUGIN_VAR_READONLY | PLUGIN_VAR_OPCMDARG,
  "Total thread pools. Settring to 0 defaults to number cores available.",
  NULL, NULL, 0, 0, 0xFFFF, 0);

static unsigned int my_max_threads_per_pool = 1;
static MYSQL_SYSVAR_UINT(
  max_threads_per_pool, my_max_threads_per_pool,
  PLUGIN_VAR_READONLY | PLUGIN_VAR_OPCMDARG,
  "Maximum threads per pool. Can grow to larger than this.",
  NULL, NULL, 16, 2, 0xFFFF, 0);

static unsigned int my_min_waiting_threads_per_pool = 4;
static MYSQL_SYSVAR_UINT(
  min_waiting_threads_per_pool, my_min_waiting_threads_per_pool,
  PLUGIN_VAR_READONLY | PLUGIN_VAR_OPCMDARG,
  "Minimum threads waiting for client io per pool. "
  "Must be at least 1 less than thread_pool_hybrid_total_thread_pools.",
  NULL, NULL, 4, 1, 0xFFFF, 0);

static SYS_VAR *system_variables[] = {
    MYSQL_SYSVAR(total_thread_pools),
    MYSQL_SYSVAR(max_threads_per_pool),
    MYSQL_SYSVAR(min_waiting_threads_per_pool),
    nullptr};

/******************************************************************************
 * Thread_pool manages a collection of threads. Threads are per connection
 * until my_max_threads_per_pool reached in which case the threads use
 * epoll_wait.
 ******************************************************************************/

struct Thread_pool {
  int epfd = -1;                  /* epoll fd used by pool */
  int evfd_epoll = -1;            /* eventfd to shutdown the threads in epoll */
  int evfd_poll = -1;             /* eventfd to shutdown the threads in poll 
                                     or convert them to epoll */

  struct Threads_state {
    uint32_t count = 0;           /* total threads in Thread_pool */
    uint32_t epoll_waiting = 0;   /* total threads waiting on epoll in Thread_pool */
    uint32_t lock_waiting = 0;    /* total threads waiting on a lock in Thread_pool */
    uint32_t connection_count = 0;/* total clients connected to the Thread_pool */
  };

  atomic<Threads_state> threads_state;

  atomic<bool> shutdown;

  Thread_pool();
  ~Thread_pool();

  int initialize();
  void shutdown_threads();
  int spawn_thread();
  bool use_connection_per_thread();

  static void* thread_start(void*);
  void thread_loop();
};


/******************************************************************************
 * Client_event class + definitions
 ******************************************************************************/

struct Client_event {
  Thread_pool &tp;
  THD *thd;
  bool do_handshake;
  bool in_lock_wait;

  Client_event(Thread_pool &tp_in, THD *thd_in)
      : tp(tp_in), thd(thd_in), do_handshake(true), in_lock_wait(false) {
  }

  ~Client_event() {
    // close the connection, decrement our connection count, destroy the thd
    close_connection(thd, 0, false, false);
    Thread_pool::Threads_state state_old, state_new;
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
      // normal when fd closed. clean up code will free the
      // Client_event
      my_plugin_log_message(&thread_pool_hybrid_plugin, MY_ERROR_LEVEL,
        "unexpected errno %d from epoll_ctl(EPOLL_CTL_MOD,...). raising SIGABRT", errno);
      std::raise(SIGABRT);
    }
  }

  void del_from_epoll() {
    if (epoll_ctl(tp.epfd, EPOLL_CTL_DEL, thd_get_fd(thd), nullptr)) {
      // normal when fd closed. clean up code will free the
      // Client_event
    }
  }

  void process() {
    char thread_top = 0;
    if (do_handshake) {
      do_handshake = false;
      thd_init(thd, &thread_top);
      if (!thd_prepare_connection(thd)) {
        if (tp.use_connection_per_thread()) {
          // Successful handshake, we have enough threads available to
          // go the use_connection_per_thread path. If we should be in epoll
          // we'll get an event through evfd_poll and kick us out into epoll.
          // So it's ok that the call to tp.use_connection_per_thread()
          // can switch to false before we call poll.
          goto use_connection_per_thread;
        } else {
          // Successful handshake. more connections than threads. use epoll.
          // it's ok to be in epoll when we should be in poll, because any
          // client event in epoll will kick us back to the poll path.
          // But it's not ok to be in poll when we should be epoll, because
          // only the client event for the specifc connection can fire and
          // wake us up.
          readd_to_epoll();
          return;
        }
      }
      // failed handshake, this is the error path.
      increment_aborted_connects();
      del_from_epoll();
      // we fall down to end and delete ourself.
    } else {
      // we get here from epoll_wait having done the handshake previously, 
      // and now to process a command,
      thd_set_thread_stack(thd, &thread_top);
      thd_store_globals(thd);
      goto do_command;
use_connection_per_thread:
      {
      // Here we wait for the descriptor to become read ready or transition
      // to epoll mode
      pollfd pfd[] = {{thd_get_fd(thd), POLLIN | POLLRDHUP, 0},
                      {tp.evfd_poll, POLLIN | POLLRDHUP, 0}};
      int res = poll(pfd, 2, -1);
      if (res == -1) {
        if (errno == EINTR) {
          // interrupt error, wait on poll again.
          goto use_connection_per_thread;
        } else {
          // don't know this error. abort
          my_plugin_log_message(&thread_pool_hybrid_plugin, MY_ERROR_LEVEL,
            "unexpected errno %d from poll. raising SIGABRT", errno);
          std::raise(SIGABRT);
        }
      }
      if (pfd[1].revents != 0) {
        // clear the event
        uint64_t val;
        if (read(tp.evfd_poll, &val, sizeof(val))) {
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
        if (tp.use_connection_per_thread()) {
          // We have enough threads available to go the
          // use_connection_per_thread path. If we should be in epoll
          // we'll get an event through evfd_poll and kick us out into epoll.
          // So it's ok that the call to tp.use_connection_per_thread()
          // can switch to false before we call poll.
          goto use_connection_per_thread;
        } else {
          // More connections than threads. use epoll.
          // it's ok to be in epoll when we should be in poll, because any
          // client event in epoll will kick us back to the poll path.
          // But it's not ok to be in poll when we should be epoll, because
          // only the client event for the specifc connection can fire and
          // wake us up.
          readd_to_epoll();
          return;
        }
      }
      // this is the error/stop path
      del_from_epoll();
      end_connection(thd);
    }
    delete this;
  }
};


/******************************************************************************
 * Thread_pool member definitions
 ******************************************************************************/

Thread_pool::Thread_pool() : shutdown(false) {
}

Thread_pool::~Thread_pool() {
  epoll_ctl(epfd, EPOLL_CTL_DEL, evfd_epoll, NULL);
  if (evfd_poll >= 0) close(evfd_poll);
  if (evfd_epoll >= 0) close(evfd_epoll);
  if (epfd >= 0) close(epfd);
}

bool Thread_pool::use_connection_per_thread() {
  return threads_state.load().connection_count <= my_max_threads_per_pool;
}

int Thread_pool::initialize() {
  if ((epfd = epoll_create1(EPOLL_CLOEXEC)) == -1)
    return errno;
  
  if ((evfd_epoll = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK)) == -1)
    return errno;

  if ((evfd_poll = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK | EFD_SEMAPHORE)) == -1)
    return errno;
  
  epoll_event epev;
  epev.events = EPOLLIN;
  epev.data.ptr = nullptr;
  if (epoll_ctl(epfd, EPOLL_CTL_ADD, evfd_epoll, &epev) == -1)
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

void Thread_pool::shutdown_threads() {
  shutdown.store(true);
  // signal to any threads in poll to shutdown.
  // we can't have more than my_max_threads_per_pool in poll
  uint64_t val = my_max_threads_per_pool;
  if (write(evfd_poll, &val, sizeof(val))) {
    // placate compiler -Wunused-result
  }
  // now signal to threads in epoll to shutdown
  // we can have way more than my_max_threads_per_pool in epoll,
  // due to locking (see thd_wait_begin) but because none
  // of the client threads ever clear the event, any thread that
  // calls epoll_wait will get the event.
  if (write(evfd_epoll, &val, sizeof(val))) {
    // placate compiler -Wunused-result
  }
  while (threads_state.load().count) {
    if (threads_state.load().count == 1) {
      // Last thread might be this thread doing the teardown. if so skip it.
      // Thread will shutdown once this request is complete.
      THD *thd = thd_get_current_thd();
      if (thd) {
        Client_event *event = (Client_event*)thd_get_scheduler_data(thd);
        if (event && &event->tp == this)
          return;
      }
    }
  }
}

int Thread_pool::spawn_thread() {
  my_thread_handle thread;
  my_thread_attr_t attr;
  my_thread_attr_init(&attr);
  my_thread_attr_setdetachstate(&attr, MY_THREAD_CREATE_DETACHED);
  int res = my_thread_create(&thread, &attr, thread_start, this);
  my_thread_attr_destroy(&attr);
  return res;
}

void* Thread_pool::thread_start(void *p) {
  Thread_pool *tp = (Thread_pool*)p;

  if (my_thread_init()) {
    my_plugin_log_message(&thread_pool_hybrid_plugin, MY_ERROR_LEVEL,
      "Thread_pool thread failed in my_thread_init()");
    std::raise(SIGABRT);
  }
  my_thread_self_setname("tp_hybrid");

  tp->thread_loop();

  my_thread_end();
  return nullptr;
}

void Thread_pool::thread_loop() {
  while (true) {
    epoll_event evt;
    Threads_state state_old, state_new;
    // keeps epoll_waiting in the range of my_min_waiting_threads_per_pool or 
    // my_min_waiting_threads_per_pool + 1
    bool thread_die;
    do {
      state_old = state_new = threads_state.load();
      if (state_new.epoll_waiting > my_min_waiting_threads_per_pool) {
        // state_new.epoll_waiting would become 2 or more than
        // my_min_waiting_threads_per_pool.
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
        my_plugin_log_message(&thread_pool_hybrid_plugin, MY_ERROR_LEVEL,
          "unexpected errno %d from epoll_wait. raising SIGABRT", errno);
        std::raise(SIGABRT);
      }
    }
    if (evt.data.ptr == nullptr) {
      // this is our server shutdown/plugin-unload event.
      // Don't read from epfd_epoll and any thread that calls
      // epoll_wwait will get this event
      // Decrement count and quit thread
      do {
        state_old = state_new = threads_state.load();
        state_new.count--;
        state_new.epoll_waiting--;
      } while (!threads_state.compare_exchange_weak(state_old, state_new,
                                                    memory_order_relaxed));
      return;
    }

    bool spawnthread;
    do {
      // we got a regular event. We'll be preoccupied with processing it,
      // so see if we should spawn another thread before we do.
      state_old = state_new = threads_state.load();
      state_new.epoll_waiting--;
      if (state_new.epoll_waiting < my_min_waiting_threads_per_pool &&
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
        my_plugin_log_message(&thread_pool_hybrid_plugin, MY_ERROR_LEVEL,
          "errno %d from spawn_thread. raising SIGABRT", errno);
        // couldn't spawn thread likely to to low resources. kill server
        std::raise(SIGABRT);
      }
    }

    ((Client_event*)evt.data.ptr)->process();
  }
}

static Thread_pool* thread_pools;
static atomic<size_t> next_thread_pool;

/******************************************************************************
 * Connection_handler_functions
 ******************************************************************************/

static bool add_connection(Channel_info *channel_info) {
  // first assign this connection to a thread_pool
  size_t next;
  size_t nextnext;
  do {
     next = next_thread_pool.load(memory_order_relaxed);
     nextnext = next + 1;
     if (nextnext == my_total_thread_pools) nextnext = 0;
  } while (!next_thread_pool.compare_exchange_weak(next, nextnext,
                                                   memory_order_relaxed));

  Thread_pool &tp = thread_pools[next];

  THD *thd = create_thd(channel_info);
  if (thd == nullptr) {
    increment_aborted_connects();
    dec_connection_count();
    return true;
  }
  destroy_channel_info(channel_info);
 
  Client_event *client_event = new(nothrow) Client_event(tp, thd);

  Thread_pool::Threads_state state_old, state_new;
  do {
    state_old = state_new = tp.threads_state.load();
    state_new.connection_count++; // decremented in dtor of client_event
  } while (!tp.threads_state.compare_exchange_weak(state_old, state_new,
                                                   memory_order_relaxed));
  thd_set_scheduler_data(thd, client_event);
  epoll_event evt;
  evt.events = EPOLLOUT | EPOLLRDHUP | EPOLLONESHOT;
  evt.data.ptr = client_event;
  if (epoll_ctl(tp.epfd, EPOLL_CTL_ADD, thd_get_fd(thd), &evt)) {
    my_plugin_log_message(&thread_pool_hybrid_plugin, MY_ERROR_LEVEL,
      "Error %d in Client_event constructor", errno);
    delete client_event;
  }
  if (state_new.connection_count == my_max_threads_per_pool + 1) {
    // signal switch to epoll to any threads waiting in poll
    uint64_t val = state_new.connection_count;
    if (write(tp.evfd_poll, &val, sizeof(val))) {
      // placate compiler -Wunused-result
    }
  }

  return false;
}

static void end() {
  // stop all threads
  for (size_t i = 0; i < my_total_thread_pools; i++)
    thread_pools[i].shutdown_threads();
}

Connection_handler_functions conn_handler = {
  (uint)get_max_connections(),
  add_connection,
  end
};

/******************************************************************************
 * THD_event_functions
 *****************************************************************************/

static void Thd_wait_begin(THD *thd, int wait_type) {
  if (!thd) return;

  switch (wait_type) {
    case THD_WAIT_ROW_LOCK:
    case THD_WAIT_GLOBAL_LOCK:
    case THD_WAIT_META_DATA_LOCK:
    case THD_WAIT_TABLE_LOCK:
    case THD_WAIT_USER_LOCK:
    {
      /*
      This code prevents all possible client thread_pool threads being stuck in
      waiting for locks and therefore a client that can clear the lock(s) doesn't
      have an available client thread to continue it's transaction, creating a
      resource deadlock. So when the count of locked threads is the same as
      threads count, create another thread so the holder(s) of the lock(s) has a
      chance to continue its transaction and unstick the server.
      */
      Client_event *event = (Client_event*)thd_get_scheduler_data(thd);
      if (event) {
        Thread_pool &tp = event->tp;
        event->in_lock_wait = true; // mark as waiting for lock
        Thread_pool::Threads_state state_old, state_new;
        bool spawnthread;
        do {
          state_old = state_new = tp.threads_state;
          state_new.lock_waiting++;
          if (state_new.count == state_new.lock_waiting) {
            state_new.count++;
            spawnthread = true;
          } else {
            spawnthread = false;
          }
        } while (!tp.threads_state.compare_exchange_weak(state_old, state_new,
                                                         memory_order_relaxed));

        if (spawnthread) {
          // if all threads are waiting on locks, spawn another thread
          // so we can process the connection(s) holding the lock(s)
          int res = tp.spawn_thread();
          if (res) {
            my_plugin_log_message(&thread_pool_hybrid_plugin, MY_ERROR_LEVEL,
              "Error %d in thd_wait_begin()", res);
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

static void Thd_wait_end(THD *thd) {
  if (!thd) return;

  Client_event *event = (Client_event*)thd_get_scheduler_data(thd);
  if (event && event->in_lock_wait) {
    Thread_pool &tp = event->tp;
    event->in_lock_wait = false;
    Thread_pool::Threads_state state_old, state_new;
    do {
      state_old = state_new = tp.threads_state;
      state_new.lock_waiting--;
    } while (!tp.threads_state.compare_exchange_weak(state_old, state_new,
                                                     memory_order_relaxed));
  }
}

static void Post_kill_notification(THD *thd) {
  Client_event *event = (Client_event*)thd_get_scheduler_data(thd);
  if (event) {
    shutdown(thd_get_fd(thd), SHUT_RDWR);
  }
}

THD_event_functions thd_event = {
  Thd_wait_begin,
  Thd_wait_end,
  Post_kill_notification
};

/******************************************************************************
 * Connection handler module initializer/denitializer
 ******************************************************************************/

static int plugin_init(MYSQL_PLUGIN plugin_ref) {
  DBUG_TRACE;
  thread_pool_hybrid_plugin = plugin_ref;

  if (my_total_thread_pools == 0)
    my_total_thread_pools = sysconf(_SC_NPROCESSORS_ONLN);

  if (my_max_threads_per_pool <= my_min_waiting_threads_per_pool + 1)
    my_min_waiting_threads_per_pool = my_max_threads_per_pool - 1;

  thread_pools = new(nothrow) Thread_pool[my_total_thread_pools];
  if (thread_pools == NULL)
    goto errhandle;

  for (size_t i = 0; i < my_total_thread_pools; i++) {
    int err = thread_pools[i].initialize();
    if (err) {
      my_plugin_log_message(&thread_pool_hybrid_plugin, MY_ERROR_LEVEL,
        "errno %d from Thread_pool::initialize()", errno);
      goto errhandle;
    }
  }

  if (my_connection_handler_set(&conn_handler, &thd_event))
    goto errhandle;
  return 0;
errhandle:
  delete[] thread_pools;
  return 1;
}

static int plugin_deinit(MYSQL_PLUGIN plugin_ref [[maybe_unused]]) {
  // stop all threads
  for (size_t i = 0; i < my_total_thread_pools; i++)
    thread_pools[i].shutdown_threads();
  // free the thread pools
  delete[] thread_pools;
  (void)my_connection_handler_reset();
  return 0;
}


struct st_mysql_daemon plugin_daemom = {MYSQL_DAEMON_INTERFACE_VERSION};

} // end namespace


mysql_declare_plugin(thread_pool_hybrid) {
    MYSQL_DAEMON_PLUGIN,
    &Thread_pool_hybrid::plugin_daemom,
    "thread_pool_hybrid",
    "Damien Katz",
    "thread_pool and epoll connection handler",
    PLUGIN_LICENSE_PROPRIETARY,
    Thread_pool_hybrid::plugin_init,          /* Plugin Init */
    nullptr,              /* Plugin Check uninstall */
    Thread_pool_hybrid::plugin_deinit,        /* Plugin Deinit */
    0x0100,               /* 1.0 */
    nullptr,              /* status variables */
    Thread_pool_hybrid::system_variables,  /* system variables */
    nullptr,              /* config options */
    0,                    /* flags */
} mysql_declare_plugin_end;

