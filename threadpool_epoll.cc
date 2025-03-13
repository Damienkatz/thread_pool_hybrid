   #include <ctype.h>
   #include <fcntl.h>
   #include <mysql/plugin.h>
   #include <mysql_version.h>
   #include <stdio.h>
   #include <stdlib.h>
   #include <time.h>
   
   #include "m_string.h"  // strlen
   #include "my_dbug.h"
   #include "my_dir.h"
   #include "my_inttypes.h"
   #include "my_io.h"
   #include "my_psi_config.h"
   #include "my_sys.h"  // my_write, my_malloc
   #include "my_thread.h"
   #include "mysql/psi/mysql_memory.h"
   #include "sql/sql_plugin.h"  // st_plugin_int
   #include "template_utils.h"
   
   PSI_memory_key key_memory_threadpool_epoll_context;
   
   #ifdef HAVE_PSI_INTERFACE
   
   static PSI_memory_info all_threadpool_epoll_memory[] = {
       {&key_memory_threadpool_epoll_context, "threadpool_epoll_context", 0, 0,
        PSI_DOCUMENT_ME}};
   
   static void init_deamon_example_psi_keys() {
     const char *category = "threadpool_epoll";
     int count;
   
     count = static_cast<int>(array_elements(all_threadpool_epoll_memory));
     mysql_memory_register(category, all_threadpool_epoll_memory, count);
   }
   #endif /* HAVE_PSI_INTERFACE */
   
   /*
     Initialize threadpool_epoll at server start or plugin installation.
   
     SYNOPSIS
       threadpool_epoll_plugin_init()
   
     DESCRIPTION
       Starts the connection handler
   
     RETURN VALUE
       0                    success
       1                    failure (cannot happen)
   */
   
   static int threadpool_epoll_plugin_init(void *p) {
     DBUG_TRACE;
   
   #ifdef HAVE_PSI_INTERFACE
     init_threadpoll_epoll_psi_keys();
   #endif
   
     return 0;
   }
   
   /*
     Terminate the daemon example at server shutdown or plugin deinstallation.
   
     SYNOPSIS
       threadpool_epoll_plugin_deinit()
   
     RETURN VALUE
       0                    success
       1                    failure (cannot happen)
   
   */
   
   static int threadpool_epoll_plugin_deinit(void *p) {
     DBUG_TRACE;
   
     return 0;
   }
   
   struct st_mysql_daemon threadpool_epoll_plugin = {MYSQL_DAEMON_INTERFACE_VERSION};
   
   mysql_declare_plugin(threadpool_epoll){
       MYSQL_DAEMON_PLUGIN,
       &threadpool_epoll_plugin,
       "threadpool_epoll",
       PLUGIN_AUTHOR_ORACLE,
       "threadpool and epoll connection handler",
       PLUGIN_LICENSE_GPL,
       threadpool_epoll_plugin_init,   /* Plugin Init */
       nullptr,                        /* Plugin Check uninstall */
       threadpool_epoll_plugin_deinit, /* Plugin Deinit */
       0x0100   /* 1.0 */,
       nullptr, /* status variables                */
       nullptr, /* system variables                */
       nullptr, /* config options                  */
       0,       /* flags                           */
   } mysql_declare_plugin_end;
   