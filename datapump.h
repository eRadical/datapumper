#include<glib.h>

#define INFORMATION_SCHEMA_DB_NAME "information_schema"
#define PERFORMANCE_SCHEMA_DB_NAME "performance_schema"
#define MySQLMONITOR_DB_NAME "MySQLMonitor"
#define NHA_DB_NAME "infra"
#define MYSQL_DB_NAME "mysql"
#define NAGIOS_DB_NAME "nagios"
#define TEST_DB_NAME "test"

void write_log_file(const gchar *log_domain, GLogLevelFlags log_level, const gchar *message, gpointer user_data);
void set_verbose(guint verbosity);
void get_current_timestamp(char *date,int length);

enum job_type { JOB_SHUTDOWN, JOB_DUMP , SCHEMA_DUMP,TABLE_DUMP };
enum consumer_type { CONSUMER_SHUTDOWN,CONSUMER_IMPORT,CONSUMER_SCHEMA_IMPORT,CONSUMER_TABLE_IMPORT};

typedef struct{
        gchar *db_name;
        gchar *table_name;
        gchar *index;
        guint64 datalength;
        guint64 rows;
}tb;

typedef struct{
        gchar *file;
        gchar *offset;
        gchar *gtid; 
}masterInfo;

typedef struct{
    GAsyncQueue* queue;
    GAsyncQueue* ready;
    GAsyncQueue *consumer_queue;
    GAsyncQueue *consumer_ready;
    GAsyncQueue *schedule_queue;
    unsigned long main_conn_id;
}configuration ;

typedef struct {
        enum job_type type;
        void *job_data;
        configuration *conf;
}job;


typedef struct{
    tb *table;
    gchar *sql;  
}table_job;

typedef struct{
    gchar *statement;
}schema_job;

typedef struct{
    enum consumer_type type;
    void *c_data;
    configuration *config;
}consumer_data;

typedef struct{
    tb *table;
    gchar *sql;
}consumer_job;

typedef struct{
    gchar *statement;
    tb *table;
}table_st_job;

typedef struct{
       guint thread_id;
       configuration *config;
}thread_data;

int dump_all_tables_in_databases(MYSQL *conn,configuration *config,GList *tblist,GList *dblist);
gboolean sig_triggered(gpointer user_data);
guint precheck_no_innodb_tables(MYSQL *conn,GList *tables);
