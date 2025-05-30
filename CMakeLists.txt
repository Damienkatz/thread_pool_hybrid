MYSQL_ADD_PLUGIN(thread_pool_hybrid
  thread_pool_hybrid.cc
  MODULE_ONLY
  LINK_LIBRARIES atomic
  MODULE_OUTPUT_NAME "thread_pool_hybrid"
  )
  