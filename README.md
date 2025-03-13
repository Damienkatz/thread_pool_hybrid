# threadpool_epoll
MySQL threadpool and epoll connection handler. Allows 100x simultaneous clients vs thread-per-connection.

# Usage

mysql plugins MUST be built using the same version of the source code and the same build arguments. If mysqld is built as a debug version without cmake parameter -DBUILD_CONFIG, the parameter must not be added when compiling plugins.

First, compile the plugin and install in to plugin dir

    cp -r . /path/to/mysql-src/plugin/threadpool_epoll
    cd /path/to/mysql-src
    cmake . -DBUILD_CONFIG=mysql_release
    cd plugin/threadpool_epoll
    make
    make install

Then, load the plugin into mysql

mysql> INSTALL PLUGIN THREADPOOL SONAME 'threadpool_epoll.so';
