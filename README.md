# thread_pool_hybrid
MySQL thread pool hybrid (poll/epoll) client connection handler.

Faster and more scalable than both the default MySQL Community Edition connection per thread handler ([see benchmarks](https://github.com/Damienkatz/thread_pool_hybrid_benchmarks/blob/main/thread_pool_hybrid-vs-connection_per_thread-mysql_8.0.4-ubuntu_24.04-on-r5.8xlarge.md)) and the MySQL Enterprise Edition thread pool handler ([see benchmarks](https://github.com/Damienkatz/thread_pool_hybrid_benchmarks/blob/main/thread_pool_hybrid_vs_enterprise_thread_pool-mysql-9.0.3-redhat_9.0.4-r7i.4xlarge.md)).

Allows much higher client counts without a drop in throughput vs the closed source "thread pool" handler while keeping the low latency performance of open source "connection per thread" handler, when the client count is low. When the count of clients gets very high and therefore threads, becomes too high the cost of too frequent context switching starts to dominate the CPU and memory bus. And adding more clients makes the time spent context switching greater and greater, and means there is less time available to do actual work.

So before that can happen, thread_pool_hybrid has each thread waiting on a single client socket and a shared eventfd in `poll(...)`. When reaching the maximum number of threads it gets a notification through the eventfd and switches to calling `epoll_wait(...)`. And now instead of getting messages from a single client socket, a thread can get them from any open client socket added to the thread pool epoll instance. When this happens each thread in the pool was either processing a request or waiting on `poll(...)`. And threads waiting on `poll(...)` will get the message, those processing a request will, when they are done processing the current request, will check and see they are supposed to call `epoll_wait(...)`.

There are, by default, the same number of independent thread pools as there are processors available to the machine. When a new connection comes in, `add_connection(...)` in the handler is called by MySQL, and it looks to see which is the next thread pool via the global `atomic<size_t> next_thread_pool` variable. It assigns the client socket to it, and then increments the `next_thread_pool` unless it is on the last thread pool, in which case it changes `next_thread_pool` to zero. This helps ensure all thread pools are saturating the CPUs relatively evenly.

# Usage

## Building and Installing

First, compile the plugin and install in to plugin dir

    cp -r . /path/to/mysql-src/plugin/thread_pool_hybrid
    cd /path/to/mysql-src
    cmake . -DBUILD_CONFIG=mysql_release -DFORCE_INSOURCE_BUILD=1
    cd plugin/thread_pool_hybrid
    make
    make install

Then, you can load the plugin into mysqld via the mysql client:

    mysql> INSTALL PLUGIN THREAD_POOL_HYBRID SONAME 'thread_pool_hybrid.so';

You uninstall like this:

    mysql> UNINSTALL PLUGIN THREAD_POOL_HYBRID;

However it will uninstall the library and then ***crash the server!*** This is because uninstalling this way removes the thread_pool_hybrid code from the running server, but that same code is currently serving the client request. So it crashes, having the code it is in the middle of running, *disappear*.

Or you can install by adding this to a config file (often in `/etc/my.cnf`):

    [mysqld]
    ...
    plugin-load-add=thread_pool_hybrid.so
    ...

Or you can add a launch parameter like this:

    ./mysqld --plugin-load-add=thread_pool_hybrid.so ...

And uninstall by removing these settings.

## Configuring the Thread Pools

If your Linux server is dedicated to just running MySQL, you should probably not worry about changing the defaults, you won't see much, if any, performance gain by changing things. But if the server is shared with a application server and or you just want to play around, here are all the configurable parameters you can add the configuration file or command line parameters.

### thread_pool_hybrid_total_thread_pools

| Description | Value |
| --- | --- |
| **Command-Line Format** | --thread_pool_hybrid_total_thread_pools=#|
| **System Variable** | thread_pool_hybrid_total_thread_pools |
| **Dynamic** | No |
| **Type** | Integer |
| **Default Value** | Number of CPUs or VCPUs available |
| **Minimum Value** | 0 (Defaults to Number of CPUs or VCPUs available) |
| **Maximum Value** | 65,535 |

Total thread pools. Setting to 0 defaults to number cores available.

### thread_pool_hybrid_max_threads_per_pool

| Description | Value |
| --- | --- |
| **Command-Line Format** | --thread_pool_hybrid_max_threads_per_pool=#|
| **System Variable** | thread_pool_hybrid_max_threads_per_pool=# |
| **Dynamic** | No |
| **Type** | Integer |
| **Default Value** | 16 |
| **Minimum Value** | 2 |
| **Maximum Value** | 65,535 |

Maximum number of the threads per pool. Though the total count can grow larger than this if all the threads are waiting on lock(s), as the connection holding the lock(s) might not have a thread and the server would hang due to resource deadlock. So it will add a thread and serve another connection until all the coonections either have a thread or the lock(s) are released.

### thread_pool_hybrid_min_waiting_threads_per_pool

| Description | Value |
| --- | --- |
| **Command-Line Format** | --thread_pool_hybrid_min_waiting_threads_per_pool=#|
| **System Variable** | thread_pool_hybrid_min_waiting_threads_per_pool=# |
| **Dynamic** | No |
| **Type** | Integer |
| **Default Value** | 4 |
| **Minimum Value** | 1 |
| **Maximum Value** | 65,535 |

Until the thread counts reaches maximum, the minimum threads waiting in epoll_wait. This is so if there is a sudden surge in connections the server can keep up by using threads in reserve.

### thread_pool_hybrid_min_waiting_threads_per_pool

| Description | Value |
| --- | --- |
| **Command-Line Format** | --thread_pool_hybrid_keep_excess_threads_alive_ms=milliseconds |
| **System Variable** | thread_pool_hybrid_keep_excess_threads_alive_ms=milliseconds |
| **Dynamic** | Yes |
| **Type** | Integer |
| **Default Value** | 50 |
| **Minimum Value** | 0 |
| **Maximum Value** | 18,446,744,073,709,551,615 |

How long extra threads -- above thread_pool_hybrid_min_waiting_threads_per_pool -- should wait in milliseconds inside epoll_waiting before dyings. 0 to instantly die.

### thread_pool_hybrid_enable_connection_per_thread_mode

| Description | Value |
| --- | --- |
| **Command-Line Format** | --thread_pool_hybrid_enable_connection_per_thread_mode=ON/OFF |
| **System Variable** | thread_pool_hybrid_keep_excess_threads_alive_ms==ON/OFF |
| **Dynamic** | Yes |
| **Type** | Boolean |
| **Default Value** | ON |

Enables or disables connection-per-thread mode until the connection count is greater than thread_pool_hybrid_max_threads_per_pool. Why switch it off? I dunno, to test how slow epoll is with a lower connection count?

### thread_pool_hybrid_debug_out_file

| Description | Value |
| --- | --- |
| **Command-Line Format** | --thread_pool_hybrid_debug_out_file=/path/to/thread/pool/debug/out/file|
| **System Variable** | thread_pool_hybrid_debug_out_file=/path/to/thread/pool/debug/out/file |
| **Dynamic** | Yes |
| **Type** | String |
| **Default Value** | "" |

When set to file that the MySQL process has write access to, will append a bunch of debugging messages to the file as it works. This is dynamic and can be turned on like this:

    MySQL> SET GLOBAL thread_pool_hybrid_debug_out_file = "/home/me/foo.txt";

Then you can actviely observe the debug messages in near real time with the command line:

    sudo tail -f /home/me/foo.txt

If the server can't open the file for writing, it will not set the value.

You can turn off the debug messages with:

    MySQL> SET GLOBAL thread_pool_hybrid_debug_out_file = "";    


The debug messages look like this:

    ...
    302382 256981 [29,3,1,0,0] Waiting in epoll
    302383 256867 [29,3,2,0,0] Set timer
    302384 256867 [29,3,2,0,0] Waiting in epoll
    302385 256980 [29,3,3,0,0] Waiting in epoll
    302386 256980 [29,3,3,0,0] Checking has_thread_timed_out()

An explanation of the values:

302385=*sequence number* 256980=*thread_id* [29=*epollfd*,3=*threads*,3=*epoll_waiting*,0=*lock_waiting*,0=*connections*] Waiting in epoll=*message*

| Field | Description |
| --- | --- |
| **sequence number**| A monotonically increasing number. |
|**threadid** | The Linux thread id for the thread the message is about. |
|**epollfd** | The epoll fd for the thread pool the thread is in. |
|**threads** | The number of the threads in the thread pool. |
|**epoll_waiting** | The number of the threads waiting in `epoll_wait(...)`. |
|**lock_waiting** | The number of threads waiting on locks. |
|**connection** | The total number of connections assigned the the thread pool. |
|**message** | A message describing what the thread is doing. |

## User defined function: TPH(thread_pool INT, info INT) returns INT

This function gives information about the Nth (starting from zero) thread pool.

Valid range for thread_pool is 0 to thread_pool_hybrid_total_thread_pools - 1.

Valid numbers for info are:

| Value | Field | Description |
| --- | --- | --- |
| 0 | threads | The number of the threads in the thread pool. |
| 1 | epoll_waiting | The number of threads waiting in the thread pool in epoll_wait. |
| 2 | lock_waiting | The number of the threads waiting in the thread pool on a lock. |
| 3 | connections | The number of connections in the thread pool.|

You must first activate the user defined function (UDF) like this:

    mysql> CREATE FUNCTION TPH RETURNS INT SONAME "thread_pool_hybrid.so";

To display all the information about the first thread pool at once, use this line:

    mysql > SELECT TPH(0,0) as threads, TPH(0,1) as epoll_waiting, TPH(0,2) as lock_waiting, TPH(0,3) as connections;
    +---------+---------------+--------------+-------------+
    | threads | epoll_waiting | lock_waiting | connections |
    +---------+---------------+--------------+-------------+
    |       5 |             4 |            0 |           1 |
    +---------+---------------+--------------+-------------+
