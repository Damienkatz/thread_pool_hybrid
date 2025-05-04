MYSQL_ADD_PLUGIN(thread_pool_hybrid
  threadpool_epoll.cc
  MODULE_ONLY
  LINK_LIBRARIES atomic
  MODULE_OUTPUT_NAME "libthread_pool_hybrid"
  )
  