# thread_pool_hybrid
MySQL thread pool hybrid (poll/epoll) connection handler.

Allows 10x simultaneous clients vs the default "per thread" connection handler while keeping the low latency performance of "connection per thread" client handling when the count of clients, and therefore threads, becomes so high that the cost of too frequent context switching all the running threads starts to dominate the CPU and memory bus. And adding more clients makes everything slower. And as each thread's overhead makes everyhting else slower there is less time available to do actual work.

So before that happanes, each thread converts from waiting inside poll() and instead call epoll_wait(), and now instead of getting messages from a single socket it can get them from any open socket added to the epoll instance. When this happens the threads were either processing a request or waiting for a request. And the poll() waiters will get the message.

# Usage

First, compile the plugin and install in to plugin dir

    cp -r . /path/to/mysql-src/plugin/thread_pool_hybrid
    cd /path/to/mysql-src
    cmake . -DBUILD_CONFIG=mysql_release -DFORCE_INSOURCE_BUILD=1
    cd plugin/thread_pool_hybrid
    make
    make install

Then, load the plugin into mysql

    mysql> INSTALL PLUGIN THREAD_POOL_HYBRID SONAME 'thread_pool_hybrid.so';
