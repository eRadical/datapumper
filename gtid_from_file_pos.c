#include<stdlib.h>
#include <glib.h>
#include <glib/gstdio.h>
#include <my_global.h>
#include <mysqld_error.h>
#include <sql_common.h>
#include <string.h>
#include "gtid_from_file_pos.h"

unsigned int get_event(const char *buf, unsigned int len) {
    if (len < EVENT_TYPE_POSITION)
        return EVENT_TOO_SHORT;
    return buf[EVENT_TYPE_POSITION];
}

int  gtid_state_from_binlog_pos(MYSQL *conn,const char *binlog_file,unsigned int pos,unsigned int stop_pos){

     (void) conn;
     (void) binlog_file;

     gboolean found_format_desc_event=FLASE;

     if(pos < 4) pos=4;
     uchar buf[128];
     NET *net;
     net= &conn->net;
     unsigned long len = 0;

     guint32 event_type;

     guint32 server_id= G_MAXUINT32 - mysql_thread_id(conn);
    
     guint64 pos_counter= 0;
 
     int4store(buf, (guint32)pos);
     int2store(buf + 4, 0);
     int4store(buf + 6, server_id);
     memcpy(buf + 10, binlog_file, strlen(binlog_file));

     if (mysql_query(conn, "SET @master_binlog_checksum='NONE'"))
     {
        g_error("Could not notify master about checksum awareness ,Master returned '%s'", mysql_error(conn));
      }
     
     if (simple_command(conn, COM_BINLOG_DUMP, buf,strlen(binlog_file) + 10, 1)) {
         g_critical("Error: binlog: Critical error whilst requesting binary log");
     }


     while(1){
         if(pos_counter > stop_pos) g_error("end...\n");
         if (net->vio != 0) len=cli_safe_read(conn);
         if ((len == 0) || (len == ~(unsigned long) 0)) {
            if (mysql_errno(conn) == ER_NET_READ_INTERRUPTED) {
                 continue;
            }else{
                g_error("Error: binlog: Network packet read error getting binlog file: %s,error %s", binlog_file,mysql_error(conn));
            }
         }
         if (len < 8 && net->read_pos[0]) {
              break;
         }

       
        g_printf("%ld\n",len);
        pos_counter += len;
        event_type= get_event((const char*)net->read_pos + 1, len -1);
        switch(event_type){
             case EVENT_TOO_SHORT:
                  g_error("Error: binlog: Event too short in binlog file: %s", binlog_file);
             case FORMAT_DESCRIPTION_EVENT:
                  if(found_format_desc_event){
                    g_error("Error: binlog :found duplicate format_description_event .");
                  }else{
                    found_format_desc_event=TRUE;
                  }
                  break;
             case 
             
        }
     }
     
     return 0;
}

