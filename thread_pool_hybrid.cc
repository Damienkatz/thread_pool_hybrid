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
  "Total thread pools. Setting to 0 defaults to number cores available.",
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


static uint64_t keep_excess_threads_alive_ms = 0;

static MYSQL_SYSVAR_ULONG(
  keep_excess_threads_alive_ms, keep_excess_threads_alive_ms,
  PLUGIN_VAR_OPCMDARG,
  "How long, in milliseconds, should an extra thread wait idle before dying. "
  "0 to instantly die.",
  NULL, NULL, 50, 0, ~0ULL, 0);

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

static int check_debug_out_file(MYSQL_THD thd[[maybe_unused]],
                                SYS_VAR *self[[maybe_unused]],
                                void *save,
                                struct st_mysql_value *value) {
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
  char b[100] = {0}; \
  Thread_pool::Threads_state s = tp->threads_state; \
  size_t line = line_number++; \
  snprintf(b, sizeof(b), "%zu %d [%" PRIu32 ",%" PRIu32 ",%" PRIu32 ",%" PRIu32 ",%" PRIu32 "] ", \
    line, gettid(), tp->epfd, s.threads, s.epoll_waiting, s.lock_waiting, s.connections); \
  snprintf(b + strlen(b), sizeof(b) - strlen(b), __VA_ARGS__); \
  fprintf(debug_file, "%s\n", b); \
}

/******************************************************************************
 * Thread_pool manages a collection of threads. Threads are per connection
 * until max_threads_per_pool reached in which case the threads use
 * epoll_wait.
 ******************************************************************************/

struct Thread_pool {
  int epfd = -1;                  // epoll fd used by pool

  int new_ci_pipe_read = -1;      // Used by new_connection(...) and
  int new_ci_pipe_write = -1;     // epoll_wait(...) to communicate a new
                                  // connection to the thread pool

  int timerfd = -1;               // timeout timer to kill off excess threads

  int evfd_poll = -1;             // eventfd to shutdown the threads in poll 
                                  //  or convert them to epoll

  int evfd_epoll = -1;            // eventfd to shutdown the threads in epoll

  /**
   * threads_state is atomic var represents the state of the threads &
   * connections in the thread pool.
   */
  struct Threads_state {
    uint16_t threads = 0;         // threads in Thread_pool
    uint16_t epoll_waiting = 0;   // threads waiting on epoll in Thread_pool
    uint16_t lock_waiting = 0;    // threads waiting on a lock in Thread_pool
    uint16_t connections = 0;     // clients connected to the Thread_pool
  };
  static_assert(sizeof(Threads_state) == sizeof(uint64_t)); // make sure no padding

  atomic<Threads_state> threads_state;

  typedef chrono::steady_clock clock;
  typedef chrono::time_point<chrono::steady_clock> time_point;
  typedef chrono::duration<chrono::steady_clock> duration;

  /**
   * Variable `threads_waiting_since` is a circular array of beginning wait
   * times of length `max_threads_per_pool - min_waiting_threads_per_pool` and
   * the variable `first_waiting_since` is when the first thread N that is
   * greater than `min_waiting_threads_per_pool` started waiting.
   * `first_waiting_since + 1 % length` is the second.
   * `first_waiting_since + 2 % length` is the third.
   *  ...etc...
   *
   * The threads are waiting on epoll_wait, and the Nth above
   * `min_waiting_threads_per_pool` about to be waiting thread marks
   * `first_waiting_since + N % length` with the aproximate time it starts
   * waiting.
   *
   * Before the first thread (`min_waiting_threads_per_pool + 1`) starts
   * waiting, it sets `timer_set` to true and set `timerfd` to fire at
   * `keep_excess_threads_alive_ms` in the future.
   * 
   * A thread waiting at that time or later will get the event, and if the
   * total threads waiting is greater than `min_waiting_threads_per_pool` it
   * will check the time at `first_waiting_since`. If the time at
   * `first_waiting_since` is more than `keep_excess_threads_alive_ms` in the
   * past, the thread will perform `++first_waiting_since %= length` and then
   * die. If there are other threads waiting it will set timerfd` to fire at
   * the time in the `first_waiting_since` slot at 
   * `keep_excess_threads_alive_ms` beyond that time.
   * 
   * This means when there is no acivity in the thread pool, it uses zero CPU
   * after the excess threads time out.
   */
  atomic<time_point> *threads_waiting_since;
  atomic<size_t> first_waiting_since;

  /**
   * timer_set is true when timerfd (the thread timeout timer) is set.
   */
  atomic<bool> timer_set = false;

  /**
   * These are the values returned in epoll_event.data.u64 by epoll_wait.
   */
  enum {
    EVENT_SHUTDOWN = 0,
    EVENT_TIMER = 1,
    EVENT_CHANNEL_INFO = 2
  //EVENT_THD = any value besides those above is a client THD event
  //            and contains the THD* in epoll_event.data.ptr
  };

  static atomic<bool> shutdown;
  static Thread_pool* thread_pools;
  static size_t next_thread_pool;

  /**
   * Represents the scheduler_data stored in a thd, which is stored as a
   * void pointer. The void pointer is never dereferenced it's bits are
   * copied into and out this struct. This allows us to attach state to thd
   * without allocation.
   */
  struct thd_scheduler_data {
    uint16_t offset = 0; // Offset into thread_pools array that contains thd.
    uint16_t flags = 1;  // Flags about the connection. 1 always set so it's
                         // distiguishable from a null ptr when offset is 0
                         // and no flags
    enum {
      FLAG_LOCKED = 2
    };
#if UINTPTR_MAX == 0xFFFFFFFFFFFFFFFFu
    uint32_t padding = 0;
#endif
    thd_scheduler_data(uint16_t offset_in) : offset(offset_in) {}

    thd_scheduler_data(void *ptr) {
      if (ptr == nullptr) std::raise(SIGABRT); //SHOULD NOT HAPPEN
      memcpy(this, &ptr, sizeof(ptr));
    }
  
    Thread_pool *tp() {
      return &thread_pools[offset];
    }
  
    void *to_ptr() {
      void *v;
      memcpy(&v, this, sizeof(thd_scheduler_data));
      return v;
    }
  };
  static_assert(sizeof(thd_scheduler_data) == sizeof(void *));

  Thread_pool();
  ~Thread_pool();

  int initialize();
  void shutdown_pool();

  void incr_epoll_waiting_and_record_wait_start_and_maybe_set_timer();
  int spawn_thread();
  bool has_thread_timed_out();
  void decr_epoll_waiting_and_maybe_spawn_thread();
  THD* get_channel_info_and_turn_into_thd();

  static void* thread_start(void*);
  void thread_loop();

  // Client event processing
  bool use_connection_per_thread();
  void clean_up_thd(THD *thd);
  void add_to_epoll(THD *thd, bool is_new_thd);
  void process(THD *thd, bool is_new_thd, int epoll_events);

  // Connection_handler_functions
  static bool add_connection(Channel_info *ci);
  static void end();

  // THD_event_functions
  static void Thd_wait_begin(THD *thd, int wait_type);
  static void Thd_wait_end(THD *thd);
  static void Post_kill_notification(THD *thd);

  // module initializer/denitializer
  static int plugin_init(MYSQL_PLUGIN plugin_ref);
  static int plugin_deinit(MYSQL_PLUGIN plugin_ref [[maybe_unused]]);
};

atomic<bool> Thread_pool::shutdown = false;
Thread_pool *Thread_pool::thread_pools = nullptr;
size_t Thread_pool::next_thread_pool = 0;


/******************************************************************************
 * Thread_pool member definitions
 ******************************************************************************/

Thread_pool::Thread_pool() {
}

Thread_pool::~Thread_pool() {
  if (epfd != -1 && new_ci_pipe_read != -1)
    epoll_ctl(epfd, EPOLL_CTL_DEL, new_ci_pipe_read, NULL);
  if (epfd != -1 && evfd_epoll != -1)
    epoll_ctl(epfd, EPOLL_CTL_DEL, evfd_epoll, NULL);
  if (epfd != -1 && timerfd != -1)
    epoll_ctl(epfd, EPOLL_CTL_DEL, timerfd, NULL);
  
  if (evfd_poll >= 0) close(evfd_poll);
  if (new_ci_pipe_write >= 0) close(new_ci_pipe_write);
  if (new_ci_pipe_read >= 0) close(new_ci_pipe_read);
  if (timerfd >= 0) close(timerfd);
  if (epfd >= 0) close(epfd);
}

int Thread_pool::initialize() {
  size_t n = max_threads_per_pool - min_waiting_threads_per_pool;
  threads_waiting_since = new atomic<time_point>[n];
  first_waiting_since = 0;

  if ((epfd = epoll_create1(EPOLL_CLOEXEC)) == -1)
    return errno;

  /* pipes are created blocking */
  int pipes[2] = {-1, -1};
  if (pipe2(pipes, O_CLOEXEC | O_DIRECT) == -1)
    return errno;

  new_ci_pipe_read = pipes[0];
  new_ci_pipe_write = pipes[1];
  
  /* Now set new_ci_pipe_read to non-blocking. Write pipe stays blocking */
  int flags = fcntl(new_ci_pipe_read, F_GETFL, 0);
  if (fcntl(new_ci_pipe_read, F_SETFL, flags | O_NONBLOCK) == -1)
    return errno;
  
  if ((evfd_epoll = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK | EFD_SEMAPHORE)) == -1)
    return errno;
    
  if ((evfd_poll = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK | EFD_SEMAPHORE)) == -1)
    return errno;
  
  if ((timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC)) == -1)
    return errno;
  
  epoll_event evt;
  evt.events = EPOLLIN;
  evt.data.u64 = EVENT_SHUTDOWN;
  if (epoll_ctl(epfd, EPOLL_CTL_ADD, evfd_epoll, &evt) == -1)
    return errno;
  
  evt.events = EPOLLIN;
  evt.data.u64 = EVENT_CHANNEL_INFO;
  if (epoll_ctl(epfd, EPOLL_CTL_ADD, new_ci_pipe_read, &evt) == -1)
    return errno;
  
  evt.events = EPOLLIN;
  evt.data.u64 = EVENT_TIMER;
  if (epoll_ctl(epfd, EPOLL_CTL_ADD, timerfd, &evt) == -1)
    return errno;
  
  for (size_t i = 0; i < min_waiting_threads_per_pool; i++) {
    Threads_state state_old, state;
    do {
      state_old = state = threads_state;
      state.threads++;
    } while (!threads_state.compare_exchange_weak(state_old, state));
    int res = spawn_thread();
    if (res) {
      // thread not created, decrement and return error
      do {
        state_old = state = threads_state;
        state.threads--;
      } while (!threads_state.compare_exchange_weak(state_old, state));
      return res;
    }
  }
  return 0;
}

void Thread_pool::shutdown_pool() {
  // signal to any threads in poll to shutdown.
  // we can't have more than max_threads_per_pool in poll
  uint64_t val = max_threads_per_pool;
  if (write(evfd_poll, &val, sizeof(val))) {
    // placate compiler -Wunused-result
  }
  // clear out any unprocessed Channel_infos
  Channel_info *ci;
  while (read(new_ci_pipe_read, &ci, sizeof(ci)) == sizeof(ci)) {
    destroy_channel_info(ci);
    increment_aborted_connects();
    dec_connection_count();

    Threads_state state_old, state;
    do {
      state_old = state = threads_state;
      state.connections--;
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
  while (threads_state.load().threads) {
    val = 1;
    if (threads_state.load().threads == 1) {
      // Last thread might be this thread doing the teardown. if so skip it.
      // Thread will shutdown once this request is complete.
      THD *thd = thd_get_current_thd();
      if (thd && thd_get_scheduler_data(thd)) {
        if (this == thd_scheduler_data(thd_get_scheduler_data(thd)).tp())
          // its our thread still alive. return
          return;
      }
    }
  }
}

void Thread_pool::incr_epoll_waiting_and_record_wait_start_and_maybe_set_timer() {
  Threads_state state_old, state;
  // add us to state.epoll_waiting
  do {
    state_old = state = threads_state;
    state.epoll_waiting++;
  } while (!threads_state.compare_exchange_weak(state_old, state));

  if (state.epoll_waiting <= max_threads_per_pool &&
      state.epoll_waiting > min_waiting_threads_per_pool) {
    // mark our slot current time. If we are `epoll_waiting + 1 =
    // min_waiting_threads_per_pool` (the first thread) N is zero.
    size_t n = state.epoll_waiting - min_waiting_threads_per_pool - 1;
    size_t first;
    do {
      first = first_waiting_since;
      n = (n + first) % (max_threads_per_pool - min_waiting_threads_per_pool);
    } while(first != first_waiting_since);

    threads_waiting_since[n].store(clock::now());
    
    if (state.epoll_waiting - min_waiting_threads_per_pool == 1 &&
        !timer_set.exchange(true)) {
      // we are the first excess thread now waiting and no timer set, so start
      // the timer so a thread dies in future if necessary.
      
      struct timespec now;
      if (clock_gettime(CLOCK_MONOTONIC, &now) == -1)
        std::raise(SIGABRT);
      
      now.tv_sec += (keep_excess_threads_alive_ms / 1000);
      now.tv_nsec += (keep_excess_threads_alive_ms % 1000) * 1000000;
      now.tv_sec += now.tv_nsec / 1000000000;
      now.tv_nsec = now.tv_nsec % 1000000000;

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
      debug_out(this, "Set timer");
    }
  }
}

bool Thread_pool::has_thread_timed_out() {
  // Clear the notification event.
  uint64_t buf;
  if (read(timerfd, &buf, sizeof(buf)) == 0) {
    debug_out(this, "another thread responded to the timer already");
    return false;
  }
  debug_out(this, "Checking has_thread_timed_out()");

  // now mark the timer as off so another thread can turn it on.
  timer_set.store(false);

  bool return_val;
  // see if we've been waiting (as a group) for too long.
  Threads_state state_old, state;
  do {
    state_old = state = threads_state;
    return_val = false;
    if (state.threads > max_threads_per_pool &&
        state.lock_waiting + 1 < state.threads) {
      // we are outside the maximum number of threads,
      // we should die.
      state.threads--;
      state.epoll_waiting--;
      return_val = true;

      debug_out(this, "thread to die because above max_threads_per_pool");
      continue;
    }

    if (state.epoll_waiting > min_waiting_threads_per_pool) {
      time_point since = threads_waiting_since[first_waiting_since];
      time_point now = clock::now();
      auto msecs = chrono::milliseconds{keep_excess_threads_alive_ms};
      if (now - since > msecs) {
        // we've had > min_waiting_threads_per_pool waiting without work for
        // keep_excess_threads_alive_ms. die.
        state.threads--;
        state.epoll_waiting--;
        return_val = true;

        debug_out(this, "thread to die because without work for"
                        " keep_excess_threads_alive_ms");
      }
    }
  } while (!threads_state.compare_exchange_weak(state_old, state));

  if (return_val)
    debug_out(this, "thread has confirmed going to die");

  size_t first_old, first = first_waiting_since;
  if (return_val) {
    // we should die. compute the new first slot
    do {
      first_old = first = first_waiting_since;
      ++first %= max_threads_per_pool - min_waiting_threads_per_pool;
    } while (!first_waiting_since.compare_exchange_weak(first_old, first));
  }
  
  if (state.epoll_waiting > min_waiting_threads_per_pool &&
      !timer_set.exchange(true)) {
    // another thread didn't turn the timer on, so we do.
    // using start slot time, add keep_excess_threads_alive_ms to the 
    // time and set it to the timer
    auto time = threads_waiting_since[first].load().time_since_epoch();
    long nsecs = chrono::duration_cast<chrono::nanoseconds>(time).count() +
                 keep_excess_threads_alive_ms * 1000000;

    time_t secs = (nsecs / 1000000000);
    nsecs = (nsecs % 1000000000);
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
  }
  return return_val;
}

void Thread_pool::decr_epoll_waiting_and_maybe_spawn_thread() {
  Threads_state state_old, state;
  bool spawnthread;
  do {
    // we got a regular event. We'll be preoccupied with processing it,
    // so see if we should spawn another thread before we do.
    state_old = state = threads_state;
    state.epoll_waiting--;
    if (state.epoll_waiting < min_waiting_threads_per_pool &&
        state.threads < max_threads_per_pool) {
      spawnthread = true;
      state.threads++;
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

THD* Thread_pool::get_channel_info_and_turn_into_thd() {
  // this means it's a new connection. Extract the Channel_info ptr
  // and turn it into a THD ptr
  Channel_info *ci;
  if (read(new_ci_pipe_read, &ci, sizeof(ci)) == 0) {
    // We hit this path when another thread was awoken and processed this event
    // before we could get to it.
    debug_out(this, "Got empty notification from new_ci_pipe_read");
    return nullptr;
  }
  
  THD* thd = create_thd(ci);

  destroy_channel_info(ci);
  if (thd == nullptr) {
    increment_aborted_connects();
    dec_connection_count();
    Threads_state state_old, state;
    do {
      state_old = state = threads_state;
      state.connections--;
    } while (!threads_state.compare_exchange_weak(state_old, state));
    return nullptr;
  }
  
  thd_set_scheduler_data(thd, thd_scheduler_data(this - thread_pools).to_ptr());

  return thd;
}

void* Thread_pool::thread_start(void *p) {
  Thread_pool *tp = (Thread_pool *)p;
  
  debug_out(tp, "thread spawned");

  if (my_thread_init()) {
    my_plugin_log_message(&thread_pool_hybrid_plugin, MY_ERROR_LEVEL,
      "Thread_pool thread failed in my_thread_init()");
    std::raise(SIGABRT);
  }

  my_thread_self_setname("tp_hybrid");

  tp->thread_loop();

  my_thread_end();

  debug_out(tp, "thread died");

  return nullptr;
}

void Thread_pool::thread_loop() {
  while (true) {
    incr_epoll_waiting_and_record_wait_start_and_maybe_set_timer();

    epoll_event evt;
wait_again:
    debug_out(this, "Waiting in epoll");
    int cnt = epoll_wait(epfd, &evt, 1, -1);
    if (cnt == -1) {
      if (errno == EINTR) {
        debug_out(this, "Waiting in epoll interrupted");
        // interrupted, wait again
        goto wait_again;
      } else {
        my_plugin_log_message(&thread_pool_hybrid_plugin, MY_ERROR_LEVEL,
          "unexpected errno %d from epoll_wait. raising SIGABRT", errno);
        std::raise(SIGABRT);
      }
    }
    
    if (evt.data.u64 == EVENT_TIMER) {
      if (has_thread_timed_out()) {
        return;
      } else {
        goto wait_again;
      }
    }
  
    if (evt.data.u64 == EVENT_SHUTDOWN) {
      // Don't clear the event, other threads will want this event too.
      Threads_state state_old, state;
      do {
        state_old = state = threads_state;
        state.threads--;
        state.epoll_waiting--;
      } while (!threads_state.compare_exchange_weak(state_old, state));
      return;
    }

    decr_epoll_waiting_and_maybe_spawn_thread();

    THD *thd;
    bool is_new_thd;
    if (evt.data.u64 == EVENT_CHANNEL_INFO) {
      if ((thd = get_channel_info_and_turn_into_thd()) == nullptr)
        continue; // another thread processed this event
      is_new_thd = true;
    } else {
      // EVENT_THD It's a THD woken by a client sending data
      thd = (THD *)evt.data.ptr;
      is_new_thd = false;
    }

    process(thd, is_new_thd, evt.events);
  }
}
/******************************************************************************
 * Client event processing 
 *******************************************************************************/

bool Thread_pool::use_connection_per_thread() {
  return threads_state.load().connections <= max_threads_per_pool &&
         !shutdown.load() &&
         enable_connection_per_thread_mode;
}

void Thread_pool::clean_up_thd(THD *thd) {
  debug_out(this, "Cleaning up thd with fd %d", thd_get_fd(thd));

  // close the connection, decrement our connections, destroy the thd
  close_connection(thd, 0, false, false);
  Threads_state state_old, state;
  do {
    state_old = state = threads_state;
    state.connections--;
  } while (!threads_state.compare_exchange_weak(state_old, state));

  destroy_thd(thd, false);
  dec_connection_count();
}

void Thread_pool::add_to_epoll(THD *thd, bool is_new_thd) {
  epoll_event evt;
  evt.events = EPOLLIN | EPOLLONESHOT;
  evt.data.ptr = thd;
  int op = is_new_thd ? EPOLL_CTL_ADD : EPOLL_CTL_MOD;
  if (epoll_ctl(epfd, op, thd_get_fd(thd), &evt)) {
    my_plugin_log_message(&thread_pool_hybrid_plugin, MY_ERROR_LEVEL,
      "unexpected errno %d from epoll_ctl(...). raising SIGABRT", errno);
    std::raise(SIGABRT);
  }
}

void Thread_pool::process(THD *thd, bool is_new_thd, int epoll_events) {
  char thread_top = 0;
  if (is_new_thd) {
    thd_init(thd, &thread_top);
    if (!thd_prepare_connection(thd)) {
      if (use_connection_per_thread()) {
        // Successful handshake, we have enough threads available to
        // go the use_connection_per_thread path. If we should be in poll
        // and use_connection_per_thread() would return false we'll get
        // an event through evfd_poll and kick us out into epoll.
        // So it's ok that the call to use_connection_per_thread()
        // can switch to false before we call poll.
        goto use_connection_per_thread;
      } else {
        // Successful handshake, more connections than threads. use epoll.
        // it's ok to be in epoll when we should be in poll, because any
        // client event in epoll will kick us back to the poll path.
        add_to_epoll(thd, is_new_thd);
        return;
      }
    }
    // failed handshake, this is the error path.
    increment_aborted_connects();
    clean_up_thd(thd);
    return;
  } else {
    // we get here having done the handshake, then epoll_wait.
    // and now to process a command,
    thd_set_thread_stack(thd, &thread_top);
    thd_store_globals(thd);

    debug_out(this, "epoll got an event %02X", epoll_events);

    if (epoll_events & (EPOLLHUP | EPOLLERR))
      goto error;
    goto do_command;
use_connection_per_thread:
    {
    // Here we wait for the descriptor to become read ready or transition
    // to epoll mode
    pollfd pfd[] = {{thd_get_fd(thd), POLLIN, 0},
                    {evfd_poll, POLLIN, 0}};

    debug_out(this, "Waiting in poll");

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
      if (read(evfd_poll, &val, sizeof(val))) {
        // placate compiler -Wunused-result
      }
    }
    if (pfd[0].revents == 0) {
      if (use_connection_per_thread()) {
        // We only got the switch to epoll event. So do the switch
        // but only if it's still vaild.
        debug_out(this, "Got old `switch to epoll` notification");

        goto use_connection_per_thread;
      } else {
        debug_out(this, "Got `switch to epoll` notification");

        add_to_epoll(thd, is_new_thd);
        return;
      }
    } else if (pfd[0].revents & (POLLHUP | POLLERR)) {
      // we got a client error or a hang up.
      debug_out(this, "poll got an error or hang up %02X", (int)pfd[0].revents);

      goto error;
    } // else fall through to processing the connection
    debug_out(this, "poll got an event %02X", (int)pfd[0].revents);
    }
do_command:
    if (!do_command(thd)) {
      // successfully processed. 
      if (use_connection_per_thread()) {
        // Successful processing, we have enough threads available to
        // go the use_connection_per_thread path. If we should be in poll
        // and use_connection_per_thread() would return false we'll get
        // an event through evfd_poll and kick us out into epoll.
        // So it's ok that the call to use_connection_per_thread()
        // can switch to false before we call poll.
        goto use_connection_per_thread;
      } else {
        // Successful processing, more connections than threads. use epoll.
        // it's ok to be in epoll when we should be in poll, because any
        // client event in epoll will kick us back to the poll path.
        add_to_epoll(thd, is_new_thd);
        return;
      }
    }
error:
    // this is the error/stop path
    if (!is_new_thd)
      epoll_ctl(epfd, EPOLL_CTL_DEL, thd_get_fd(thd), nullptr);
    
    end_connection(thd);
  }
  clean_up_thd(thd);
}

/******************************************************************************
 * Connection_handler_functions
 ******************************************************************************/

bool Thread_pool::add_connection(Channel_info *ci) {
try_next_pool:
  // first assign this connection to a thread_pool
  size_t next = next_thread_pool++;
  if (next_thread_pool == total_thread_pools)
    next_thread_pool = 0;

  Thread_pool *tp = &thread_pools[next];

  Threads_state state_old, state;
  do {
    state_old = state = tp->threads_state;
    if (state.connections == 0xFFFF) {
      // Thread_pool cannot take any more connectons. try next pool.
      // We will loop around until we find thread a pool that can take the connection.
      // it's possible we never find a pool and loop indefintely until a connection drops.
      goto try_next_pool;
    }
    state.connections++; // decremented in Client_event::clean_up_thd()
  } while (!tp->threads_state.compare_exchange_weak(state_old, state));
  
  if (enable_connection_per_thread_mode &&
      state.connections == max_threads_per_pool + 1) {
    // signal 'switch to epoll' to any threads waiting in poll
    uint64_t val = max_threads_per_pool;
    if (write(tp->evfd_poll, &val, sizeof(val))) {
      // placate compiler -Wunused-result
    }
  }
  
  if (write(tp->new_ci_pipe_write, &ci, sizeof(ci)) != sizeof(ci)) {
    // this is a blocking pipe and should not happen
    std::raise(SIGABRT);
  }

  return false;
}


void Thread_pool::end() {
  // stop all threads
  for (size_t i = 0; i < total_thread_pools; i++)
    thread_pools[i].shutdown_pool();
  
  // free the mem
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
  Thread_pool::add_connection,
  Thread_pool::end
};

/******************************************************************************
 * THD_event_functions
 *****************************************************************************/

void Thread_pool::Thd_wait_begin(THD *thd, int wait_type) {
  if (!thd) thd = thd_get_current_thd();
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
      void *vdata = thd_get_scheduler_data(thd);
      if (vdata) {
        thd_scheduler_data data(vdata);
        Thread_pool *tp = data.tp();
        Threads_state state_old, state;
        bool spawnthread;
        do {
          state_old = state = tp->threads_state;
          state.lock_waiting++;
          if (state.threads == state.lock_waiting && state.threads != 0xFFFF) {
            state.threads++;
            spawnthread = true;
          } else {
            spawnthread = false;
          }
        } while (!tp->threads_state.compare_exchange_weak(state_old, state));

        data.flags |= thd_scheduler_data::FLAG_LOCKED;
        thd_set_scheduler_data(thd, data.to_ptr());

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

void Thread_pool::Thd_wait_end(THD *thd) {
  if (!thd)
    thd = thd_get_current_thd();
  if (!thd) return;
  // see if we've encoded that this thread is in a lock (see Thd_wait_begin) in
  // the scheduler data 
  void *vdata = thd_get_scheduler_data(thd);
  if (!vdata) return;
  thd_scheduler_data data(vdata);

  if ((data.flags & thd_scheduler_data::FLAG_LOCKED)) {
    data.flags &= ~thd_scheduler_data::FLAG_LOCKED;
    // set the locked bit to off
    thd_set_scheduler_data(thd, data.to_ptr());
    Threads_state state_old, state;
    do {
      state_old = state = data.tp()->threads_state;
      state.lock_waiting--;
    } while (!data.tp()->threads_state.compare_exchange_weak(state_old, state));
    
  }
}

void Thread_pool::Post_kill_notification(THD *thd) {
  // if it has scheduler_data, it's ours
  if (thd_get_scheduler_data(thd)) {
    // this will alert both epoll and poll
    ::shutdown(thd_get_fd(thd), SHUT_RD);
  }
}

THD_event_functions thd_event = {
  Thread_pool::Thd_wait_begin,
  Thread_pool::Thd_wait_end,
  Thread_pool::Post_kill_notification
};


/******************************************************************************
 * User Defined Function: tph(pool INT, info INT) returns INT
 * 
 * To use, on mysql client, do:
 *  CREATE FUNCTION tph RETURNS INT SONAME "thread_pool_hybrid.so";
 * 
 * Pool is the pool # starting from zero.
 * 
 * Info is 0-3 where
 *  0 == threads
 *  1 == epoll_wating
 *  2 == lock_wating
 *  3 == connections
 ******************************************************************************/

extern "C"
bool tph_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  if (args->arg_count != 2 ||
      args->arg_type[0] != INT_RESULT || 
      args->arg_type[0] != INT_RESULT) {
    strcpy(message, "TPH() takes 2 integer arguments");
    return true;
  }
  
  initid->max_length = 9;
  initid->const_item = false;
  initid->maybe_null = true;
  return false;
}

extern "C"
void tph_deinit(UDF_INIT *) {
}

extern "C"
long long tph(UDF_INIT *, UDF_ARGS *args, char *,
                        unsigned char *null_value,
                        unsigned char *) {
  if (!args->args[0]) {
    *null_value = 1;
    return 0;
  }
  int pool_n = (int)*((long long *)args->args[0]);

  if (pool_n < 0 || pool_n >= (int)total_thread_pools) {
    *null_value = 1;
    return 0;
  }

  Thread_pool::Threads_state s = Thread_pool::thread_pools[pool_n].threads_state;
  switch (*((long long *)args->args[1])) {
    case 0:
      return (long long)s.threads;
    case 1:
      return (long long)s.epoll_waiting;
    case 2:
      return (long long)s.lock_waiting;
    case 3:
      return (long long)s.connections;
    default:
      *null_value = 1;
      return 0;
  }
}


/******************************************************************************
 * Connection handler module initializer/denitializer
 ******************************************************************************/

int Thread_pool::plugin_init(MYSQL_PLUGIN plugin_ref) {
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

int Thread_pool::plugin_deinit(MYSQL_PLUGIN plugin_ref [[maybe_unused]]) {
  shutdown.store(true);
  end();
  (void)my_connection_handler_reset();
  return 0;
}

struct st_mysql_daemon plugin_daemom = {MYSQL_DAEMON_INTERFACE_VERSION};

} // end namespace

using namespace Thread_pool_hybrid;

mysql_declare_plugin(thread_pool_hybrid) {
    MYSQL_DAEMON_PLUGIN,
    &plugin_daemom,
    "thread_pool_hybrid",
    "Damien Katz",
    "thread_pool and epoll connection handler",
    PLUGIN_LICENSE_PROPRIETARY,
    Thread_pool::plugin_init,          /* Plugin Init */
    nullptr,              /* Plugin Check uninstall */
    Thread_pool::plugin_deinit,        /* Plugin Deinit */
    0x0100,               /* 1.0 */
    nullptr,              /* status variables */
    system_variables,  /* system variables */
    nullptr,              /* config options */
    0,                    /* flags */
} mysql_declare_plugin_end;


