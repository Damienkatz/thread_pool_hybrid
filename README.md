# threadpool_epoll
MySQL thread pool hybrid connection handler. Allows 10x simultaneous clients vs the default "per thread" connection handler while keeping the performance of "per thread" when the client count is low.

# Usage

First, compile the plugin and install in to plugin dir

    cp -r . /path/to/mysql-src/plugin/thread_pool_hybrid
    cd /path/to/mysql-src
    cmake . -DBUILD_CONFIG=mysql_release -DFORCE_INSOURCE_BUILD=1
    cd plugin/thread_pool_hybrid
    make
    make install

Then, load the plugin into mysql

    mysql> INSTALL PLUGIN THREAD_POOL_HYBRID SONAME 'libthread_pool_hybrid.so';
