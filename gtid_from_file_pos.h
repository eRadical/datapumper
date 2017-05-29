#include<mysql.h>
#define LOG_EVENT_MINIMAL_HEADER_LEN 19U
enum Log_event_type{
     STOP_EVENT= 3,
     ROTATE_EVENT= 4,
     FORMAT_DESCRIPTION_EVENT= 15,
     GTID_LOG_EVENT= 33, 
     EVENT_TOO_SHORT= 254
};

enum event_postions {
    EVENT_TIMESTAMP_POSITION= 0,
    EVENT_TYPE_POSITION= 4,
    EVENT_SERVERID_POSITION= 5,
    EVENT_LENGTH_POSITION= 9,
    EVENT_NEXT_POSITION= 13,
    EVENT_FLAGS_POSITION= 17,
    EVENT_EXTRA_FLAGS_POSITION= 19 // currently unused in v4 binlogs, but a good marker for end of header
};

void make_log_name(char* buf, const char* log_ident);
int  gtid_state_from_binlog_pos(MYSQL *conn,const char *in_name,unsigned int pos,unsigned int stop_pos);


