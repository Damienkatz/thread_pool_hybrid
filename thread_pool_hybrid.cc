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
#include <sys/timerfd.h>
#include <sys/ioctl.h>
#include <inttypes.h>

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

static unsigned int total_thread_pools = 0;

static MYSQL_SYSVAR_UINT(
  total_thread_pools, total_thread_pools,
  PLUGIN_VAR_READONLY | PLUGIN_VAR_OPCMDARG,
  "Total thread pools. Settring to 0 defaults to number cores available.",
  NULL, NULL, 0, 0, 0xFFFF, 0);

static unsigned int max_threads_per_pool = 1;

static MYSQL_SYSVAR_UINT(
  max_threads_per_pool, max_threads_per_pool,
  PLUGIN_VAR_READONLY | PLUGIN_VAR_OPCMDARG,
  "Maximum threads per pool. Can grow to larger than this.",
  NULL, NULL, 16, 2, 0xFFFF, 0);

static unsigned int min_waiting_threads_per_pool = 4;

static MYSQL_SYSVAR_UINT(
  min_waiting_threads_per_pool, min_waiting_threads_per_pool,
  PLUGIN_VAR_READONLY | PLUGIN_VAR_OPCMDARG,
  "Minimum threads waiting for client io per pool. "
  "Must be at least 1 less than thread_pool_hybrid_total_thread_pools.",
  NULL, NULL, 4, 1, 0xFFFF, 0);


static uint64_t keep_excess_threads_alive_ms = 4;

static MYSQL_SYSVAR_ULONG(
  keep_excess_threads_alive_ms, keep_excess_threads_alive_ms,
  PLUGIN_VAR_READONLY | PLUGIN_VAR_OPCMDARG,
  "How long, in ms, should an extra thread wait idle before dieing. 0 to"
  " instantly die.",
  NULL, NULL, 1000, 0, ~0ULL, 0);

static bool enable_connection_per_thread_mode;

static MYSQL_SYSVAR_BOOL(
  enable_connection_per_thread_mode, enable_connection_per_thread_mode,
  PLUGIN_VAR_READONLY | PLUGIN_VAR_OPCMDARG,
  "Enable each thread to use poll w/ it's assigned client when the count "
  "of connections is less than or equal to max_threads_per_pool.",
  nullptr, /* check func*/
  nullptr, /* update func*/
  true);      /* default*/


static char *debug_out_file = nullptr;

static FILE *debug_file = nullptr;

static int check_debug_out_file(MYSQL_THD thd[[maybe_unused]], SYS_VAR *self[[maybe_unused]],
                                  void *save, struct st_mysql_value *value) {
  int value_len = 0;
  if (value == nullptr) return true;

  const char* proposed_debug_file = value->val_str(value, nullptr, &value_len);

  if (proposed_debug_file == nullptr) return true;

  if (strlen(proposed_debug_file) == 0 ) {
    if (debug_file)
      fclose(debug_file);
    debug_file = nullptr;
    *static_cast<const char **>(save) = proposed_debug_file;
    return false;
  }

  if (strlen(proposed_debug_file) > PATH_MAX) return true;
  
  FILE *debug_file_in = fopen(proposed_debug_file, "a");
  if (debug_file_in) {
    if (debug_file)
      fclose(debug_file);
    debug_file = debug_file_in;
    setbuf(debug_file, NULL);
    *static_cast<const char **>(save) = proposed_debug_file;
    return false;
  }
  return true;
}

static void update_debug_out_file(MYSQL_THD, SYS_VAR *var [[maybe_unused]],
                                  void *var_ptr, const void *save) {
  *static_cast<const char **>(var_ptr) =
        *static_cast<const char **>(const_cast<void *>(save));
}

static MYSQL_SYSVAR_STR(
  debug_out_file, debug_out_file,
  PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC,
  "Enable debug messages output to filename.",
  check_debug_out_file, /* check func*/
  update_debug_out_file, /* update func*/
  "");

static SYS_VAR *system_variables[] = {
    MYSQL_SYSVAR(total_thread_pools),
    MYSQL_SYSVAR(max_threads_per_pool),
    MYSQL_SYSVAR(min_waiting_threads_per_pool),
    MYSQL_SYSVAR(keep_excess_threads_alive_ms),
    MYSQL_SYSVAR(enable_connection_per_thread_mode),
    MYSQL_SYSVAR(debug_out_file),
    nullptr};

atomic<size_t> line_number = 0;
#define debug_out(tp, ...) \
if (debug_file) { \
  char b[82] = {0}; \
  Thread_pool::Threads_state s = tp->threads_state; \
  size_t line = line_number++; \
  snprintf(b, sizeof(b), "%zu %d [%" PRIu32 ",%" PRIu32 ",%" PRIu32 ",%" PRIu32 ",%" PRIu32 "] ", \
    line, gettid(), tp->epfd, s.count, s.epoll_waiting, s.lock_waiting, s.connection_count); \
  snprintf(b + strlen(b), sizeof(b) - strlen(b), __VA_ARGS__); \
  fprintf(debug_file, "%s\n", b); \
}

/******************************************************************************
 * Thread_pool manages a collection of threads. Threads are per connection
 * until max_threads_per_pool reached in which case the threads use
 * epoll_wait.
 ******************************************************************************/

struct Thread_pool {
  int epfd = -1;                  /* epoll fd used by pool */
  int pipe_read = -1;             /* used to communicate new connection, or pool shutdown */
  int pipe_write = -1;            /* used to communicate new connection, or pool shutdown */
  int timerfd = -1;               /* used to kill off excess threads at interval */
  int evfd_poll = -1;             /* eventfd to shutdown the threads in poll 
                                     or convert them to epoll */
  int evfd_epoll = -1;             /* eventfd to shutdown the threads in epoll */

  struct Threads_state {
    uint32_t count = 0;           /* total threads in Thread_pool */
    uint32_t epoll_waiting = 0;   /* total threads waiting on epoll in Thread_pool */
    uint32_t lock_waiting = 0;    /* total threads waiting on a lock in Thread_pool */
    uint32_t connection_count = 0;/* total clients connected to the Thread_pool */
  };

  atomic<Threads_state> threads_state;

  typedef chrono::steady_clock clock;
  typedef chrono::time_point<chrono::steady_clock> time_point;
  typedef chrono::duration<chrono::steady_clock> duration;
  
  atomic<time_point> *threads_waiting_since;
  atomic<size_t> start_of_threads_waiting_since;

  atomic<bool> shutdown = false;

  atomic<bool> timer_set = false;

  Thread_pool();
  ~Thread_pool();

  int initialize();
  void shutdown_pool();
  int spawn_thread();
  bool use_connection_per_thread();
  void set_time_out_timer();
  bool has_thread_timed_out();

  static void* thread_start(void*);
  void thread_loop();
};

/******************************************************************************
 * Client_event class + definitions
 ******************************************************************************/

struct Client_event {
  Thread_pool *tp;
  THD *thd;

  Client_event(Thread_pool *tp_in, THD *thd_in)
      : tp(tp_in), thd(thd_in) {
  }

  void clean_up_thd() {
    // close the connection, decrement our connection count, destroy the thd
    close_connection(thd, 0, false, false);
    Thread_pool::Threads_state state_old, state;
    do {
      state_old = state = tp->threads_state;
      state.connection_count--;
    } while (!tp->threads_state.compare_exchange_weak(state_old, state));

    destroy_thd(thd, false);
    dec_connection_count();
  }

  void add_to_epoll(bool is_new_thd) {
    epoll_event evt;
    evt.events = EPOLLIN | EPOLLONESHOT;
    evt.data.ptr = thd;
    int op = is_new_thd ? EPOLL_CTL_ADD : EPOLL_CTL_MOD;
    if (epoll_ctl(tp->epfd, op, thd_get_fd(thd), &evt)) {
      my_plugin_log_message(&thread_pool_hybrid_plugin, MY_ERROR_LEVEL,
        "unexpected errno %d from epoll_ctl(EPOLL_CTL_ADD,...). raising SIGABRT", errno);
      std::raise(SIGABRT);
    }
  }

  void del_from_epoll() {
    epoll_ctl(tp->epfd, EPOLL_CTL_DEL, thd_get_fd(thd), nullptr);
  }

  void process(bool is_new_thd, int epoll_events) {
    char thread_top = 0;
    if (epoll_events & (EPOLLHUP | EPOLLERR)) {
        debug_out(tp, "epoll got an error or hang up %02X", epoll_events);
      goto error;
    }
    if (is_new_thd) {
      thd_init(thd, &thread_top);
      if (!thd_prepare_connection(thd)) {
        if (tp->use_connection_per_thread()) {
          // Successful handshake, we have enough threads available to
          // go the use_connection_per_thread path. If we should be in epoll
          // we'll get an event through evfd_poll and kick us out into epoll.
          // So it's ok that the call to tp->use_connection_per_thread()
          // can switch to false before we call poll.
          goto use_connection_per_thread;
        } else {
          // Successful handshake. more connections than threads. use epoll.
          // it's ok to be in epoll when we should be in poll, because any
          // client event in epoll will kick us back to the poll path.
          // But it's not ok to be in poll when we should be epoll, because
          // only the client event for the specifc connection can fire and
          // wake us up.
          add_to_epoll(is_new_thd);
          return;
        }
      }
      // failed handshake, this is the error path.
      increment_aborted_connects();
      clean_up_thd();
      return;
    } else {
      // we get here having done the handshake, then epoll_wait.
      // and now to process a command,
      thd_set_thread_stack(thd, &thread_top);
      thd_store_globals(thd);

      debug_out(tp, "epoll got an event %02X", epoll_events);
      goto do_command;
use_connection_per_thread:
      {
      // Here we wait for the descriptor to become read ready or transition
      // to epoll mode
      pollfd pfd[] = {{thd_get_fd(thd), POLLIN, 0},
                      {tp->evfd_poll, POLLIN, 0}};
      debug_out(tp, "Waiting in poll");
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
        // clear the event, we get this when being notified to switch to epoll
        uint64_t val;
        if (read(tp->evfd_poll, &val, sizeof(val))) {
          // placate compiler -Wunused-result
        }
      }
      if (pfd[0].revents == 0) {
        if (tp->use_connection_per_thread()) {
          debug_out(tp, "Got old `switch to epoll` notification");
          // We only got the switch to epoll event. So do the switch
          // but only if it's still vaild.
          goto use_connection_per_thread;
        } else {
          add_to_epoll(is_new_thd);
          debug_out(tp, "Got `switch to epoll` notification");
        }
        return;
      } else if (pfd[0].revents & (POLLHUP | POLLERR)) {
        // we got a client error or a hang up.
        debug_out(tp, "poll got an error or hang up %02X", (int)pfd[0].revents);
        goto error;
      } // else fall through to processing the connection
      debug_out(tp, "poll got an event %02X", (int)pfd[0].revents);
      }
do_command:
      if (!do_command(thd)) {
        // successfully processed. 
        if (tp->use_connection_per_thread()) {
          // We have enough threads available to go the
          // use_connection_per_thread path. If we should be in epoll
          // we'll get an event through evfd_poll and kick us out into epoll.
          // So it's ok that the call to tp->use_connection_per_thread()
          // can switch to false before we call poll.
          goto use_connection_per_thread;
        } else {
          // More connections than threads. use epoll.
          // it's ok to be in epoll when we should be in poll, because any
          // client event in epoll will kick us back to the poll path.
          // But it's not ok to be in poll when we should be epoll, because
          // only the client event for the specifc connection can fire and
          // wake us up.
          add_to_epoll(is_new_thd);
          return;
        }
      }
error:
      // this is the error/stop path
      if (!is_new_thd)
        del_from_epoll();
      end_connection(thd);
    }
    clean_up_thd();
  }
};


/******************************************************************************
 * Thread_pool member definitions
 ******************************************************************************/

Thread_pool::Thread_pool() {
}

Thread_pool::~Thread_pool() {
  if (epfd != -1 && pipe_read != -1)
    epoll_ctl(epfd, EPOLL_CTL_DEL, pipe_read, NULL);
  if (epfd != -1 && evfd_epoll != -1)
    epoll_ctl(epfd, EPOLL_CTL_DEL, evfd_epoll, NULL);
  if (epfd != -1 && timerfd != -1)
    epoll_ctl(epfd, EPOLL_CTL_DEL, timerfd, NULL);
  
  if (evfd_poll >= 0) close(evfd_poll);
  if (pipe_write >= 0) close(pipe_write);
  if (pipe_read >= 0) close(pipe_read);
  if (timerfd >= 0) close(timerfd);
  if (epfd >= 0) close(epfd);
}

bool Thread_pool::use_connection_per_thread() {
  return !shutdown.load() &&
          enable_connection_per_thread_mode &&
          threads_state.load().connection_count <= max_threads_per_pool;
}

int Thread_pool::initialize() {
  // the 0th place in this vector isn't used.
  size_t n = max_threads_per_pool - min_waiting_threads_per_pool;
  threads_waiting_since = new atomic<time_point>[n];
  start_of_threads_waiting_since = n;

  if ((epfd = epoll_create1(EPOLL_CLOEXEC)) == -1)
    return errno;

  int pipes[2] = {-1, -1};
  if (pipe2(pipes, O_CLOEXEC | O_DIRECT) == -1)
    return errno;
  pipe_read = pipes[0];
  pipe_write = pipes[1];
  
  int flags = fcntl(pipe_write, F_GETFL, 0);
  if (fcntl(pipe_read, F_SETFL, flags | O_NONBLOCK) == -1)
    return errno;
  
  if ((evfd_epoll = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK | EFD_SEMAPHORE)) == -1)
    return errno;
    
  if ((evfd_poll = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK | EFD_SEMAPHORE)) == -1)
    return errno;
  
  if ((timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC)) == -1)
    return errno;
  
  epoll_event evt;
  evt.events = EPOLLIN;
  evt.data.u64 = 0;
  if (epoll_ctl(epfd, EPOLL_CTL_ADD, evfd_epoll, &evt) == -1)
    return errno;
  
  evt.events = EPOLLIN;
  evt.data.u64 = 1;
  if (epoll_ctl(epfd, EPOLL_CTL_ADD, pipe_read, &evt) == -1)
    return errno;
  
  evt.events = EPOLLIN;
  evt.data.u64 = 2;
  if (epoll_ctl(epfd, EPOLL_CTL_ADD, timerfd, &evt) == -1)
    return errno;
  
  for (size_t i = 0; i < min_waiting_threads_per_pool; i++) {
    Threads_state state_old, state;
    do {
      state_old = state = threads_state;
      state.count++;
    } while (!threads_state.compare_exchange_weak(state_old, state));
    int res = spawn_thread();
    if (res) {
      // thread not created, decrement count and return error
      do {
        state_old = state = threads_state;
        state.count--;
      } while (!threads_state.compare_exchange_weak(state_old, state));
      return res;
    }
  }
  return 0;
}

void Thread_pool::shutdown_pool() {
  shutdown.store(true);
  // signal to any threads in poll to shutdown.
  // we can't have more than max_threads_per_pool in poll
  uint64_t val = max_threads_per_pool;
  if (write(evfd_poll, &val, sizeof(val))) {
    // placate compiler -Wunused-result
  }

  // clear out any unprocessed Channel_infos
  Channel_info *ci;
  while (read(pipe_read, &ci, sizeof(ci)) == sizeof(ci)) {
    destroy_channel_info(ci);
    increment_aborted_connects();
    dec_connection_count();

    Threads_state state_old, state;
    do {
      state_old = state = threads_state;
      state.connection_count--;
    } while (!threads_state.compare_exchange_weak(state_old, state));
  }

  // now signal to threads in epoll to shutdown
  // we can have way more than max_threads_per_pool waiting in epoll,
  // due to locking (see thd_wait_begin) but because none
  // of the client threads ever clear the 1 event, any thread that
  // calls epoll_wait will get the event.
  val = 1;
  if (write(evfd_epoll, &val, sizeof(val))) {
    // placate compiler -Wunused-result
  }
  while (threads_state.load().count) {
    val = 1;
    if (threads_state.load().count == 1) {
      // Last thread might be this thread doing the teardown. if so skip it.
      // Thread will shutdown once this request is complete.
      THD *thd = thd_get_current_thd();
      if (thd) {
        // our requesting thd can't be waiting on a lock so no need to decode
        // the ptr.
        if (this == (Thread_pool *)thd_get_scheduler_data(thd))
          // its our thread still alive. return;
          break;
      }
    }
  }
}

void Thread_pool::set_time_out_timer() {
  if (timer_set.exchange(true)) {
    // timer already set
    return;
  }
  // we just transitioned to excess waiting threads. set a timer
  // to see if a thread should die in keep_excess_threads_alive_ms
  struct timespec now;
  if (clock_gettime(CLOCK_MONOTONIC, &now) == -1)
    std::raise(SIGABRT);
  
  now.tv_sec += (keep_excess_threads_alive_ms / 1000);
  now.tv_nsec += (keep_excess_threads_alive_ms % 1000) * 1000000;
  now.tv_sec += now.tv_nsec / 1000000;
  now.tv_nsec = now.tv_nsec % 1000000;

  struct itimerspec new_value;
  new_value.it_interval.tv_sec = 0;
  new_value.it_interval.tv_nsec = 0;
  new_value.it_value.tv_sec = now.tv_sec;
  new_value.it_value.tv_nsec = now.tv_nsec;

  if (timerfd_settime(timerfd, TFD_TIMER_ABSTIME, &new_value, nullptr) == -1) {
    my_plugin_log_message(&thread_pool_hybrid_plugin, MY_ERROR_LEVEL,
      "timerfd_settimet() returned %d", errno);
    std::raise(SIGABRT);
  }
}

bool Thread_pool::has_thread_timed_out() {
  // Clear the notification event.
  uint64_t buf;
  if (read(timerfd, &buf, sizeof(buf)) == 0) {
    // another thread responded to the timer already
  }
  bool return_val;
  // see if we've been waiting (as a group) for too long.
  Threads_state state_old, state;
  do {
    state_old = state = threads_state;
    return_val = false;
    if (state.count > max_threads_per_pool &&
        state.lock_waiting + 1 < state.count) {
      // we are outside the maximum number of threads,
      // we should die.
      state.count--;
      state.epoll_waiting--;
      return_val = true;
      continue;
    }

    if (state.epoll_waiting > min_waiting_threads_per_pool) {
      atomic<time_point> &since = threads_waiting_since[
                                      start_of_threads_waiting_since];
      time_point now = clock::now();
      auto msecs = chrono::milliseconds{keep_excess_threads_alive_ms};
      if (now - since.load() > msecs) {
        // we've had threads waiting without work for n msecs. die.
        state.count--;
        state.epoll_waiting--;
        return_val = true;
      }
    }
  } while (!threads_state.compare_exchange_weak(state_old, state));

  if (return_val) {
    debug_out(this, "thread has decided to die");
  }

  size_t start_old, start = start_of_threads_waiting_since;
  if (return_val) {
    // we should die. compute the new start slot
    do {
      start_old = start = start_of_threads_waiting_since;
      start++;
      if (start > max_threads_per_pool - min_waiting_threads_per_pool)
        start = 0;
    } while (!start_of_threads_waiting_since
                .compare_exchange_weak(start_old, start)); 
  }
  // using start, add the current time to it.
  if (state.epoll_waiting > min_waiting_threads_per_pool) {
    if (timer_set.exchange(true)) 
      return false;
    auto next_time_out = threads_waiting_since[start].load().time_since_epoch();
    long nsecs = chrono::duration_cast<chrono::nanoseconds>(next_time_out).count();
    time_t secs = (nsecs / 1000000) + (keep_excess_threads_alive_ms / 1000);
    nsecs = (nsecs % 1000000) + (keep_excess_threads_alive_ms % 1000) * 1000000;
    struct itimerspec  new_value;
    new_value.it_interval.tv_sec = 0;
    new_value.it_interval.tv_nsec = 0;
    new_value.it_value.tv_sec = secs;
    new_value.it_value.tv_nsec = nsecs;
    
    if (timerfd_settime(timerfd, TFD_TIMER_ABSTIME, &new_value, nullptr) == -1) {
      my_plugin_log_message(&thread_pool_hybrid_plugin, MY_ERROR_LEVEL,
        "timerfd_settimet() returned %d", errno);
      std::raise(SIGABRT);
    }
  } else {
    timer_set.store(false);
  }

  return return_val;
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
  Thread_pool *tp = (Thread_pool *)p;

  if (my_thread_init()) {
    my_plugin_log_message(&thread_pool_hybrid_plugin, MY_ERROR_LEVEL,
      "Thread_pool thread failed in my_thread_init()");
    std::raise(SIGABRT);
  }

  debug_out(tp, "thread birthed");
  my_thread_self_setname("tp_hybrid");

  tp->thread_loop();

  my_thread_end();

  debug_out(tp, "thread died");
  return nullptr;
}

void Thread_pool::thread_loop() {
  while (true) {
    epoll_event evt;
    Threads_state state_old, state;
    // add us too state.epoll_waiting
    do {
      state_old = state = threads_state;
      state.epoll_waiting++;
    } while (!threads_state.compare_exchange_weak(state_old, state));

    if (state.epoll_waiting <= max_threads_per_pool &&
        state.epoll_waiting > min_waiting_threads_per_pool) {
      // mark our slot current time
      size_t n = state.epoll_waiting - min_waiting_threads_per_pool - 1;
      n = (n + start_of_threads_waiting_since) %
            (max_threads_per_pool - min_waiting_threads_per_pool);

      threads_waiting_since[n].store(clock::now());
      
      if (state.epoll_waiting - min_waiting_threads_per_pool == 1) {
        // we are the first excess thread now waiting, starting the
        // timer so a thread if unecessary.
        set_time_out_timer();
      }
    }
wait_again:
    debug_out(this, "Waiting in epoll");
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
    
    if (evt.data.u64 == 2) {
      if (has_thread_timed_out()) {
        return;
      } else {
        goto wait_again;
      }
    }
  
    if (evt.data.u64 == 0) {
      // this means we should die as it's the shutdown or unload event.
      // Don't clear the event, other threads will want this event too.
      do {
        state_old = state = threads_state;
        state.count--;
        state.epoll_waiting--;
      } while (!threads_state.compare_exchange_weak(state_old, state));
      return;
    }
    
    bool spawnthread;
    do {
      // we got a regular event. We'll be preoccupied with processing it,
      // so see if we should spawn another thread before we do.
      state_old = state = threads_state;
      state.epoll_waiting--;
      if (state.epoll_waiting < min_waiting_threads_per_pool &&
          state.count < max_threads_per_pool) {
        spawnthread = true;
        state.count++;
      } else {
        spawnthread = false;
      }
    } while (!threads_state.compare_exchange_weak(state_old, state));
  
    if (spawnthread) {
      int res = spawn_thread();
      if (res) {
        my_plugin_log_message(&thread_pool_hybrid_plugin, MY_ERROR_LEVEL,
          "errno %d from spawn_thread. raising SIGABRT", errno);
        // couldn't spawn thread likely due to low resources. kill server
        std::raise(SIGABRT);
      }
    }

    THD *thd;
    bool is_new_thd;
    if (evt.data.u64 == 1) {
      // this means it's a new connection. Extract the Channel_info ptr
      // and turn it into a THD ptr
      Channel_info *ci;
      if (read(pipe_read, &ci, sizeof(ci)) == -1) {
        debug_out(this, "Got empty notification from pipe_read");
        continue;
      }
      thd = create_thd(ci);
      is_new_thd = true;
      destroy_channel_info(ci);
      if (thd == nullptr) {
        increment_aborted_connects();
        dec_connection_count();
        do {
          state_old = state = threads_state;
          state.connection_count--;
        } while (!threads_state.compare_exchange_weak(state_old, state));
        continue;
      }
      thd_set_scheduler_data(thd, this);
    } else {
      thd = (THD *)evt.data.ptr;
      is_new_thd = false;
    }

    Client_event(this, thd).process(is_new_thd, evt.events);
  }
}

static Thread_pool* thread_pools;
static atomic<size_t> next_thread_pool;

/******************************************************************************
 * Connection_handler_functions
 ******************************************************************************/

static bool add_connection(Channel_info *ci) {
  // first assign this connection to a thread_pool
  size_t next;
  size_t nextnext;
  do {
     next = next_thread_pool;
     nextnext = next + 1;
     if (nextnext == total_thread_pools) nextnext = 0;
  } while (!next_thread_pool.compare_exchange_weak(next, nextnext));

  Thread_pool *tp = &thread_pools[next];
  Thread_pool::Threads_state state_old, state;
  do {
    state_old = state = tp->threads_state;
    state.connection_count++; // decremented in Client_event::clean_up_thd()
  } while (!tp->threads_state.compare_exchange_weak(state_old, state));
  
  if (enable_connection_per_thread_mode &&
      state.connection_count == max_threads_per_pool + 1) {
    // signal switch to epoll to any threads waiting in poll
    uint64_t val = max_threads_per_pool;
    if (write(tp->evfd_poll, &val, sizeof(val))) {
      // placate compiler -Wunused-result
    }
  }
  
  if (write(tp->pipe_write, &ci, sizeof(ci)) != sizeof(ci)) {
    // this should not happen
    std::raise(SIGABRT);
  }

  return false;
}

static void end() {
  // stop all threads
  for (size_t i = 0; i < total_thread_pools; i++)
    thread_pools[i].shutdown_pool();
  // free the mem
  if (thread_pools)
    delete[] thread_pools;
  
  // null out
  total_thread_pools = 0;
  thread_pools = nullptr;

  if (debug_file) {
    fclose(debug_file);
    debug_file = nullptr;
  }
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
  if (!thd)
    thd = thd_get_current_thd();
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
      Thread_pool *tp = (Thread_pool*)thd_get_scheduler_data(thd);
      if (tp) {
        Thread_pool::Threads_state state_old, state;
        bool spawnthread;
        do {
          state_old = state = tp->threads_state;
          state.lock_waiting++;
          if (state.count == state.lock_waiting) {
            state.count++;
            spawnthread = true;
          } else {
            spawnthread = false;
          }
        } while (!tp->threads_state.compare_exchange_weak(state_old, state));
        
        // encode our that this thd is waiting on locks in the unused bits in the
        // pointer to the thd's scheduler_data. Least Significant Bit is fine, as
        // the tp will be 64bit aligned and never odd.
        thd_set_scheduler_data(thd, (void *)((uintptr_t)tp | (uintptr_t)1));

        if (!spawnthread)
          return;
        
        int res = tp->spawn_thread();
        if (res) {
          my_plugin_log_message(&thread_pool_hybrid_plugin, MY_ERROR_LEVEL,
            "Error %d in thd_wait_begin()", res);
          std::raise(SIGABRT);
        }
      }
      break;
    }
    default:
      break;
  }
}

static void Thd_wait_end(THD *thd) {
  if (!thd)
    thd = thd_get_current_thd();
  if (!thd) return;
  // see if we've encoded that this thread is in a lock (see Thd_wait_begin) in
  // the scheduler data 
  uintptr_t encoded_ptr = (uintptr_t)thd_get_scheduler_data(thd);
  bool is_in_lock = (encoded_ptr & (uintptr_t)1) != 0;

  if (is_in_lock) {
    // strip out the is_in_lock bit
    Thread_pool *tp = (Thread_pool*)(encoded_ptr & ~(uintptr_t)1);
    thd_set_scheduler_data(thd, tp);
    Thread_pool::Threads_state state_old, state;
    do {
      state_old = state = tp->threads_state;
      state.lock_waiting--;
    } while (!tp->threads_state.compare_exchange_weak(state_old, state));
    
  }
}

static void Post_kill_notification(THD *thd) {
  if (thd_get_scheduler_data(thd)) {
    // There is scheduler_data. its one of ours, shutdown the fd but don't close,
    // that way we get an epoll or poll event and clean it up
    thd_close_connection(thd);
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

  if (total_thread_pools == 0)
    total_thread_pools = sysconf(_SC_NPROCESSORS_ONLN);

  if (max_threads_per_pool < min_waiting_threads_per_pool)
    min_waiting_threads_per_pool = max_threads_per_pool;

  thread_pools = new(nothrow) Thread_pool[total_thread_pools];
  if (thread_pools == NULL)
    goto errhandle;

  for (size_t i = 0; i < total_thread_pools; i++) {
    int err = thread_pools[i].initialize();
    if (err) {
      my_plugin_log_message(&thread_pool_hybrid_plugin, MY_ERROR_LEVEL,
        "errno %d from Thread_pool::initialize()", errno);
      goto errhandle;
    }
  }

#ifdef epoll_params
  if (debug_file) {
    struct epoll_params params;
    /* Code to show how to retrieve the current settings */

    memset(&params, 0, sizeof(struct epoll_params));

    if (ioctl(thread_pools[0].epfd, EPIOCGPARAMS, &params) == -1)
      std::raise(SIGABRT);

    /* params struct now contains the current parameters */

    fprintf(debug_file, "epoll usecs: %lu\n", params.busy_poll_usecs);
    fprintf(debug_file, "epoll packet budget: %u\n", params.busy_poll_budget);
    fprintf(epoll_params), "epoll prefer busy poll: %u\n", params.prefer_busy_poll);
  }
#endif

  if (my_connection_handler_set(&conn_handler, &thd_event))
    goto errhandle;
  return 0;
errhandle:
  delete[] thread_pools;
  return 1;
}

static int plugin_deinit(MYSQL_PLUGIN plugin_ref [[maybe_unused]]) {
  Thread_pool_hybrid::end();
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

