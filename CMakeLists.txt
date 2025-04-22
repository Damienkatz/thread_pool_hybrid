MYSQL_ADD_PLUGIN(threadpool_epoll
  threadpool_epoll.cc
  MODULE_ONLY
  LINK_LIBRARIES atomic
  MODULE_OUTPUT_NAME "libthreadpool_epoll"
  )
  