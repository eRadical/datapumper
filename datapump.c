#include <glib.h>
#include <glib/gstdio.h>
#include <mysql.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include "datapump.h"
#include "mysqlconn.h"
#include "datapumpconfig.h"
#include "util.h"


gchar *db;
guint version=0;
guint is_dump=0;
guint is_dump_all=0;
guint auxilizary=0;
gchar *logname;
gchar *tables_list;
guint verbose=3;
FILE *logfile;

gchar *output_directory=NULL;

gchar *hostname=NULL;
gchar *username=NULL;
gchar *password=NULL;
guint port=3306;

gchar *target_host=NULL;
gchar *target_user=NULL;
gchar *target_pass=NULL;
guint target_port=3306;

gchar DIRECTORY[]="backup";

GMutex *init_mutex=NULL;
GMutex *init_consumer_mutex=NULL;

MYSQL *g_conn = NULL;

FILE *metadata;
gchar *metafile;


guint rows_per_chunk=1000000;
guint statement_size=10000; 

guint source_thread_num=4,target_thread_num=10;


GMainLoop *m1;

gboolean daemon_mode= FALSE;
gboolean no_schema=FALSE;
gboolean no_table=FALSE;
gboolean no_data=FALSE;

guint errors=0;

gint start_consumer=0;

volatile gint schema_consumer_count=0;
volatile gint schema_consumer_done=0;

volatile gint table_consumer_count=0;
volatile gint table_consumer_done=0;


static GOptionEntry entries[]={ 
        { "version", 'V', 0, G_OPTION_ARG_NONE, &version, "DataPumper Version", NULL },
        { "verbose", 'v', 0, G_OPTION_ARG_INT, &verbose, "Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info, default 2", NULL },
        { "host", 'h', 0, G_OPTION_ARG_STRING, &hostname, "The source mysql host to connect to", NULL },
        { "user", 'u', 0, G_OPTION_ARG_STRING, &username, "The source mysql username with privileges to run the dump", NULL },
        { "password", 'p', 0, G_OPTION_ARG_STRING, &password, "The source mysql user password", NULL },
        { "port", 'P', 0, G_OPTION_ARG_INT, &port, "The source tcp/ip port to connect to", NULL },
        { "thost",'g',0,G_OPTION_ARG_STRING,&target_host,"The target mysql host to connect to ",NULL },
        { "tuser",'i',0,G_OPTION_ARG_STRING,&target_user,"The target mysql usrname with privileges to accept the dump",NULL },
        { "tpassword",'m',0,G_OPTION_ARG_STRING,&target_pass,"The target mysql user password",NULL},
        { "tport",'K',0,G_OPTION_ARG_INT,&target_port,"The target tcp/ip port to connect to ",NULL},
        { "dump", 'd', 0, G_OPTION_ARG_NONE, &is_dump, "DataPumper dump database service", NULL },
        { "all",  'A', 0, G_OPTION_ARG_NONE, &is_dump_all, "Dump all database ",NULL},
        { "auxilizary",'a',0,G_OPTION_ARG_NONE,&auxilizary,"Dump the auxilizary databases ,like test,MySQLMonitor etc ",NULL},
        { "database", 'B', 0, G_OPTION_ARG_STRING, &db, "Database to dump,split by \',\' ", NULL },
        { "tables" ,'T',0,G_OPTION_ARG_STRING,&tables_list,"Table list to dump,split by \',\' ",NULL},
        { "rows",'r',0,G_OPTION_ARG_INT,&rows_per_chunk,"The rows every chunk to dump tables ",NULL},
        { "statement-size", 's', 0, G_OPTION_ARG_INT, &statement_size, "Attempted size of INSERT statement in bytes, default 1000000", NULL},
        { "sthread",'S',0,G_OPTION_ARG_INT,&source_thread_num,"The source thread num to dump table data",NULL},
        { "dthread",'D',0,G_OPTION_ARG_INT,&target_thread_num,"The target thread num to consume table data",NULL},
        { "output", 'o',0,G_OPTION_ARG_STRING,&output_directory,"Dump output directory",NULL},
        { "logfile",'L',0,G_OPTION_ARG_STRING,&logname,"Logfile to log information",NULL},
        { "daemon", 'e', 0, G_OPTION_ARG_NONE, &daemon_mode, "Enable daemon mode", NULL },
        { "no-schema",'N',0,G_OPTION_ARG_NONE,&no_schema,"Not dump database structure", NULL},
        { "no-table",'n',0,G_OPTION_ARG_NONE,&no_table,"Not dump table structure",NULL}
       
};

struct tm tval;

void no_log(const gchar *log_domain, GLogLevelFlags log_level, const gchar *message, gpointer user_data) {
    (void) log_domain;
    (void) log_level;
    (void) message;
    (void) user_data;
}

void set_verbose(guint verbosity) {
    if (logname) {
        logfile = g_fopen(logname, "w");
        if (!logfile) {
            g_critical("Could not open log file '%s' for writing: %d", logname, errno);
            exit(EXIT_FAILURE);
        }
    }

    switch (verbosity) {
        case 0:
            g_log_set_handler(NULL, (GLogLevelFlags)(G_LOG_LEVEL_MASK), no_log, NULL);
            break;
        case 1:
            g_log_set_handler(NULL, (GLogLevelFlags)(G_LOG_LEVEL_WARNING | G_LOG_LEVEL_MESSAGE), no_log, NULL);
            if (logfile)
                g_log_set_handler(NULL, (GLogLevelFlags)(G_LOG_LEVEL_ERROR | G_LOG_LEVEL_CRITICAL), write_log_file, NULL);
            break;
        case 2:
            g_log_set_handler(NULL, (GLogLevelFlags)(G_LOG_LEVEL_MESSAGE), no_log, NULL);
            if (logfile)
                g_log_set_handler(NULL, (GLogLevelFlags)(G_LOG_LEVEL_WARNING | G_LOG_LEVEL_ERROR | G_LOG_LEVEL_WARNING | G_LOG_LEVEL_ERROR | G_LOG_LEVEL_CRITICAL), write_log_file, NULL);
            break;
        default:
            if (logfile)
                g_log_set_handler(NULL, (GLogLevelFlags)(G_LOG_LEVEL_MASK), write_log_file, NULL);
            break;
    }
}


void get_start_timeinfo(){
     time_t t;
     time(&t);
     localtime_r(&t,&tval);
}


void set_default_output_directory(){
     get_start_timeinfo();
     output_directory = g_strdup_printf("%s-%04d%02d%02d-%02d%02d%02d",DIRECTORY,
                                                                       tval.tm_year+1900, tval.tm_mon+1, tval.tm_mday,
                                                                       tval.tm_hour, tval.tm_min, tval.tm_sec);
     
}


void create_backup_dir(char *new_directory) {
    if (g_mkdir(new_directory, 0700) == -1)
    {
        if (errno != EEXIST)
        {
            g_critical("Unable to create `%s': %s",new_directory,g_strerror(errno));
            exit(EXIT_FAILURE);
        }
    }
}


void do_master_status(MYSQL *conn,masterInfo *masterinfo){
      gchar *query=g_strdup("show global status like '%Binlog_snapshot_%'");
      MYSQL_RES *res=mysql_query_rs(conn,query);
      MYSQL_ROW row;
      MYSQL_FIELD *fields;
      gint va_col=-1,vl_col=-1;
      fields=mysql_fetch_fields(res);
      
      g_free(query);
      guint i;
      for(i=0;i<mysql_num_fields(res);i++){
         if(!g_strcasecmp(fields[i].name,"Variable_name")) va_col=i;
         else if(!g_strcasecmp(fields[i].name,"Value")) vl_col=i;
      }
      while((row=mysql_fetch_row(res))){
          if(!g_strcasecmp(row[va_col],"Binlog_snapshot_file")) masterinfo->file=g_strdup(row[vl_col]);
          else if(!g_strcasecmp(row[va_col],"Binlog_snapshot_position")) masterinfo->offset=g_strdup(row[vl_col]);
      }
      mysql_close_result(res);
  
     return;
}


void quote_name(const char *source,char *dest){
     char *to=dest;
     char qtype='`';
     *to++=qtype;
     while(*source){
        if(*source == qtype) *to++=qtype;
        *to++=*source++;
     }
     to[0]=qtype;
     to[1]=0;
}

MYSQL *get_main_conn(){
       if(g_conn) return g_conn;
       g_conn=mysql_connect(hostname,port,username,password,NULL);
       if(mysql_query(g_conn, "SET SESSION wait_timeout = 2147483")){
          g_error("Failed to increase source wait_timeout: %s", mysql_error(g_conn));
       }
       if(mysql_query(g_conn,"SET SESSION net_read_timeout = 2147483")){
          g_error("Failed to increase source net_read_timeout: %s", mysql_error(g_conn));
       }
       if (mysql_query(g_conn, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")) {
          g_critical("Failed to set isolation level: %s", mysql_error(g_conn));
          exit(EXIT_FAILURE);
       }
       if(mysql_query(g_conn,"START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */")){
          g_critical("Failed to start transaction in consist snapshot ,error : %s",mysql_error(g_conn));
       }
      
       return g_conn;
}

gchar *get_max_cardinality_index_in_table(MYSQL *conn,tb *table){
     gchar *query=g_strdup_printf("show index from %s.%s",table->db_name,table->table_name);
     gchar *key = NULL;
     MYSQL_RES *res;
     MYSQL_FIELD *fields;
     MYSQL_ROW row;
     res=mysql_query_rs(conn,query);
     g_free(query);

     gint uni_col=-1,key_name_col=-1,seq_col=-1,col_name_col=-1,car_col=-1;
     guint i=0;
     fields=mysql_fetch_fields(res);
     for(i=0;i<mysql_num_fields(res);i++){
          if(!g_strcasecmp(fields[i].name,"Non_unique")) uni_col=i;
          else if(!g_strcasecmp(fields[i].name,"Key_name")) key_name_col=i;
          else if(!g_strcasecmp(fields[i].name,"Seq_in_index")) seq_col=i;
          else if(!g_strcasecmp(fields[i].name,"Column_name")) col_name_col=i;
          else if(!g_strcasecmp(fields[i].name,"Cardinality")) car_col=i;
     }
     while((row=mysql_fetch_row(res))){
           if(!strcmp(row[key_name_col],"PRIMARY") && !strcmp(row[seq_col],"1")) {
              key=g_strdup(row[col_name_col]);
              break;
     }
     }
     if(!key){
       mysql_data_seek(res,0); 
       while((row=mysql_fetch_row(res))){
           if(!strcmp(row[uni_col],"0") && !strcmp(row[seq_col],"1")){
              key=g_strdup(row[col_name_col]);
              break;
           }
       }
     }
     if(!key){
        guint64 max_cardinality=0;
        guint64 cardinality=0;
        mysql_data_seek(res,0);
        while((row=mysql_fetch_row(res))){
           if(!strcmp(row[seq_col],"1")){
                cardinality=strtoll(row[car_col],NULL,10);
                if(cardinality > max_cardinality){
                    max_cardinality=cardinality;
                    key=row[col_name_col];
             }
        }
     }
     if(key) key=g_strdup(key);
   }
   table->index=key;
   
   if(res) mysql_close_result(res);

   return key; 
}


void get_tables_in_database(MYSQL *conn, gchar *database,GList **tables){
     gchar *query=g_strdup_printf("SHOW TABLE STATUS FROM %s ",database);
     MYSQL_RES *res;
     MYSQL_ROW row;
     MYSQL_FIELD  *fields;

    res=mysql_query_rs(conn,query); 
    fields=mysql_fetch_fields(res);
    g_free(query);

    gint name_col=-1,data_col=-1,row_col=-1;
    guint i=0;
    for(i=0;i<mysql_num_fields(res);i++){
       if(!g_strcasecmp(fields[i].name,"Name")) name_col=i;
       else if(!g_strcasecmp(fields[i].name,"Data_length")) data_col=i;
       else if(!g_strcasecmp(fields[i].name,"Rows")) row_col=i;
    }
    
    while((row=mysql_fetch_row(res))){
         tb *table=g_new0(tb,1);
         table->db_name=g_strdup(database);
         table->table_name=g_strdup_printf("`%s`",row[name_col]);
         if(row[data_col]) table->datalength=strtoll(row[data_col],NULL,10);
         else table->datalength=0;
         if(row[row_col]) table->rows=strtoll(row[row_col],NULL,10);
         else table->rows=0;
         *tables=g_list_append(*tables,table);
          table_consumer_count++;
    }
   mysql_close_result(res);
}

void generate_dump_table_task(MYSQL *conn,tb *table,configuration *conf){
     gchar *query = NULL;
     if(table->index) query=g_strdup_printf("select /*!40001 SQL_NO_CACHE */ min(%s),max(%s) from %s.%s",table->index,table->index,table->db_name,table->table_name);
     guint min=0,max=0;
     MYSQL_RES *res =NULL;
     MYSQL_ROW row;
     MYSQL_FIELD *fields = NULL;
     gchar *task_query = NULL;
     guint fullscan=0;

     if(!query) {
       fullscan=1;
       goto end_task;
     }

     res=mysql_query_rs(conn,query);


     if(!res) goto end_task;

      row=mysql_fetch_row(res);
      if(!row || !row[0] || !row[1]) goto end_task;
      fields=mysql_fetch_fields(res);
      switch(fields[0].type){
         case MYSQL_TYPE_LONG:
         case MYSQL_TYPE_LONGLONG:
         case MYSQL_TYPE_INT24:
              min=strtoll(row[0],NULL,10);
              max=strtoll(row[1],NULL,10);
              break;
         default:
            fullscan=1;
            goto end_task;
         
      }
     if(res) {
         mysql_close_result(res);
         res=NULL;
     }
     if(table->rows < max - min) table->rows=max-min;
     if(table->rows <= rows_per_chunk) {
         fullscan=1;
         goto end_task;
     }
     else{
       guint64 start=min,step=rows_per_chunk,next=start+step+1;
       while(start <= max){
           if(start == min)
           task_query=g_strdup_printf("select /*!40001 SQL_NO_CACHE */ * from %s.%s where %s " \
                                      "is null  or %s >= %llu and %s < %llu ",table->db_name,table->table_name,table->index,table->index,(unsigned long long)start,table->index,(unsigned long long)next);
           else
           task_query=g_strdup_printf("select /*!40001 SQL_NO_CACHE */ * from %s.%s where " \
                                      "%s>=%llu and %s<%llu ",table->db_name,table->table_name,table->index,(unsigned long long)start,table->index,(unsigned long long)next);

           start=next;
           next=start+step+1;

           table_job *tjob=g_new0(table_job,1);
           job *j=g_new0(job,1);
           tjob->table=table;
           tjob->sql=task_query;

           j->type=JOB_DUMP;
           j->job_data=(void *)tjob;
           j->conf=conf;
           while(1){
           if(g_async_queue_length(conf->queue) > 50){
              nanosleep((const struct timespec[]){{0, 100000000L}}, NULL);
           }else{
              break;
           }     
           }
           g_async_queue_push(conf->queue,j);
                       
       }
     }
     
    end_task:
       if(fullscan){
          task_query=g_strdup_printf("select /*!40001 SQL_NO_CACHE */ * from %s.%s ",table->db_name,table->table_name);
          table_job *tjob=g_new0(table_job,1);
          job *j=g_new0(job,1);
          tjob->table=table;
          tjob->sql=task_query;

          j->type=JOB_DUMP; 
          j->job_data=(void *)tjob; 
          j->conf=conf; 
          g_async_queue_push(conf->queue,j);
       }
       if(res) mysql_close_result(res);
       if(query) g_free(query);
}


void generate_database_structure_task(configuration *config,GList *dblist){
     GList *iter=dblist;
     gchar *dbname;
     for(iter=g_list_first(iter);iter;iter=g_list_next(iter)){
          dbname=(gchar *)iter->data;
          schema_job *tj=g_new0(schema_job,1);
          job *j=g_new0(job,1);
          tj->statement=g_strdup_printf("SHOW CREATE DATABASE %s",dbname);
          j->job_data=(void *) tj;
          j->type=SCHEMA_DUMP;
          j->conf=config;
          g_async_queue_push(config->queue,j);
     }
}


// It's very dangerous to overlay the same table data in the target database. so if the table in the target database , we need refuse dump.
void generate_table_structure_task(configuration *config,GList *tblist){
     GList *iter=tblist;
     tb *table;
     
     for(iter=g_list_first(iter);iter;iter=g_list_next(iter)){
         table=(tb *)iter->data;
         table_st_job *tsj=g_new0(table_st_job,1);
         job *j=g_new0(job,1);
         tsj->statement=g_strdup_printf("SHOW CREATE TABLE %s.%s",table->db_name,table->table_name);
         tsj->table=table;
         j->job_data=(void *)tsj;
         j->type=TABLE_DUMP;
         j->conf=config;
         g_async_queue_push(config->queue,j);
     }
}

void dump_all_databases(MYSQL *conn,configuration *config,GList *tblist){
     gchar *query=g_strdup("SHOW DATABASES");
     GList *dblist = NULL;
     MYSQL_RES *res;
     MYSQL_ROW row;
     gchar dbname[100];
     res=mysql_query_rs(conn,query);
     g_free(query);
     while((row=mysql_fetch_row(res))){
       if(!g_strcasecmp(row[0],INFORMATION_SCHEMA_DB_NAME) || !g_strcasecmp(row[0],PERFORMANCE_SCHEMA_DB_NAME) || !g_strcasecmp(row[0],MYSQL_DB_NAME)) continue;
       if(!auxilizary){
           if(!g_strcasecmp(row[0],MySQLMONITOR_DB_NAME)       || \
              !g_strcasecmp(row[0],NHA_DB_NAME)                || \
              !g_strcasecmp(row[0],NAGIOS_DB_NAME)             || \
              !g_strcasecmp(row[0],TEST_DB_NAME))                      
              continue;
        }
      
       quote_name(row[0],dbname); 
       dblist=g_list_append(dblist,g_strdup(dbname));
       schema_consumer_count++;
     }

     mysql_close_result(res);

     if(!no_schema){
       generate_database_structure_task(config,dblist);
     }

     GList *iter=dblist;
     gchar *tmp_db;
     for(iter=g_list_first(iter);iter;iter=g_list_next(iter)){ 
         tmp_db=(gchar *)iter->data;
         get_tables_in_database(conn,tmp_db,&tblist);
         g_free(tmp_db);
     }
    
     if(precheck_no_innodb_tables(conn,tblist)){
         exit(EXIT_FAILURE);
     }

     // we need first generate dump schema task and then generate the data task
     if(!no_table){
       generate_table_structure_task(config,tblist);
     }

     if(!no_data){
       iter=tblist;
       tb *table;
       //for loop the dump tables and try to get the max  cardinality column name,and generate the dump task 
       for(iter=g_list_first(iter);iter;iter=g_list_next(iter)){
        table=(tb *)iter->data;
        get_max_cardinality_index_in_table(conn,table);
        generate_dump_table_task(conn,table,config);
      } 
     }   
    
     g_list_free(dblist);
     return ;
}

/*
 * 1.dump database structure
 * 2.dump table structure
 * 3.dump data
 *
 */
int dump_all_tables_in_databases(MYSQL *conn,configuration *config,GList *tblist,GList *dblist){
    GList *iter=tblist;
    tb *table = NULL;
    
    if(!no_schema){
       generate_database_structure_task(config,dblist);
    }
     GList *diter=dblist;
     gchar *tmp_db;
     for(diter=g_list_first(diter);diter;diter=g_list_next(diter)){
         tmp_db=(gchar *)diter->data;
         get_tables_in_database(conn,tmp_db,&tblist);
         g_free(tmp_db);
     }

    if(!no_table){
       generate_table_structure_task(config,tblist);
    }
    if(!no_data){
    for(iter=g_list_first(iter);iter;iter=g_list_next(iter)){
        table=(tb *) iter->data;
        get_max_cardinality_index_in_table(conn,table);
        generate_dump_table_task(conn,table,config);
    }
   }
   if(dblist) g_list_free(dblist);
   return 0;
}

void free_master_info(masterInfo *masterinfo){
     if(masterinfo->file) g_free(masterinfo->file);
     if(masterinfo->offset) g_free(masterinfo->offset);
     if(masterinfo->gtid) g_free(masterinfo->gtid);
     g_free(masterinfo);
}

void write_metadata_info(masterInfo *masterinfo){  
     if(masterinfo && masterinfo->file && masterinfo->offset){
           char data_buffer[20];
           metafile=g_strdup_printf("%s/metadata.partial",output_directory);
           metadata=g_fopen(metafile,"w");
          if(!metadata){
              g_critical("Failed to open metadata file  %s,error :%s\n",metafile,g_strerror(errno));
              exit(EXIT_FAILURE);
          }
              get_current_timestamp(data_buffer,20);
              fprintf(metadata,"start dump : %s\nmaster log file : %s\nmaster log position : %s\n",data_buffer,masterinfo->file,masterinfo->offset);
              fflush(metadata);
              fclose(metadata);
              g_warning("start dump :%s",data_buffer);

     }
}

void free_table(tb *table){
     if(table){
        if(table->db_name) g_free(table->db_name);
        if(table->table_name) g_free(table->table_name);
        if(table->index) g_free(table->index);
        g_free(table);
     }
}

void events_consumer(thread_data *tdata){
     configuration *conf=tdata->config;
     MYSQL *consumer_conn=mysql_multi_connect(target_host,target_port,target_user,target_pass,NULL,init_consumer_mutex);
     if(mysql_query(consumer_conn,"SET SESSION wait_timeout = 2147483")){
         g_error("Failed to set wait_timeout on producer conn,error : %s",mysql_error(consumer_conn));
     }
     if(mysql_query(consumer_conn,"SET SESSION net_write_timeout = 2147483")){
          g_error("Failed to increase target net_read_timeout: %s", mysql_error(consumer_conn));
     }
     if(mysql_query(consumer_conn,"SET NAMES binary")){
          g_error("Thread %d failed execute set names binary on target mysql server",tdata->thread_id);
     }
     if(mysql_query(consumer_conn,"SET FOREIGN_KEY_CHECKS=0")){
          g_error("Thread %d failed execute set FOREIGN_KEY_CHECKS=0 on target mysql server",tdata->thread_id);
     }

     gchar *use_db=NULL;
     g_async_queue_push(conf->consumer_ready,GINT_TO_POINTER(1));
     g_warning("Consumer thread %d started",tdata->thread_id);
     for(;;){
         consumer_job  *cj=NULL;
         consumer_data *cd=(consumer_data *)g_async_queue_pop(conf->consumer_queue);
         switch(cd->type){
             case CONSUMER_IMPORT:
                  if(!schema_consumer_done || !table_consumer_done){
                      while(1){
                        nanosleep((const struct timespec[]){{0, 100000000L}}, NULL);    
                        if(schema_consumer_done && table_consumer_done) break;
                      }
                  }
                  cj=(consumer_job *)cd->c_data;
                  if(mysql_query(consumer_conn,cj->sql)){
                     g_critical("Failed import data on target mysql server (%s) , error : %s",cj->sql,mysql_error(consumer_conn));
                     if(cj->sql) g_free(cj->sql);
                     if(cj) g_free(cj);
                     if(cd) g_free(cd);
                     errors++;
                     exit(EXIT_FAILURE);
                  }
                  if(cj->sql) g_free(cj->sql);
                  if(cj) g_free(cj);
                  if(cd) g_free(cd);
                  break;
             case CONSUMER_SCHEMA_IMPORT:
                  cj=(consumer_job *)cd->c_data;
                  g_message("Create database sql %s on target server \n",cj->sql);
                  if(mysql_query(consumer_conn,cj->sql)){
                      g_critical("Failed exeucte create database  on target mysql server (%s) , error : %s",cj->sql,mysql_error(consumer_conn));
                      if(cj->sql) g_free(cj->sql);
                      if(cj) g_free(cj);
                      if(cd) g_free(cd);
                      exit(EXIT_FAILURE);
                  }
                  if(cj->sql) g_free(cj->sql);
                  if(cj) g_free(cj);
                  if(cd) g_free(cd);
                  if(g_atomic_int_dec_and_test(&schema_consumer_count)) schema_consumer_done=1;
                  break;
             case CONSUMER_TABLE_IMPORT:
                  if(!schema_consumer_done){
                    while(1){
                      nanosleep((const struct timespec[]){{0, 100000000L}}, NULL);
                      if(schema_consumer_done){
                         break;
                      }
                  }
                  }
                  cj=(consumer_job *)cd->c_data;
                  use_db=g_strdup_printf("USE %s",((tb *)cj->table)->db_name);
                  if(mysql_query(consumer_conn,use_db)){
                    g_critical("Failed select db on target mysql server (%s), error : %s",((tb *)cj->table)->db_name,mysql_error(consumer_conn));
                    if(use_db) g_free(use_db);
                    exit(EXIT_FAILURE);
                  }
                  if(mysql_query(consumer_conn,cj->sql)){
                    g_critical("Failed exeucte create table  on target mysql server (%s), error : %s",((tb *)cj->table)->db_name,mysql_error(consumer_conn));
                    if(cj->sql) g_free(cj->sql);
                    if(cj) g_free(cj);
                    if(cd) g_free(cd);
                    errors++;
                    exit(EXIT_FAILURE);
                    }
                    if(cj->sql) g_free(cj->sql);
                    if(cj) g_free(cj);
                    if(cd) g_free(cd);
                    if(use_db) g_free(use_db);
                    if(g_atomic_int_dec_and_test(&table_consumer_count)) table_consumer_done=1;
                    break;
             case CONSUMER_SHUTDOWN:
                  g_free(cj);
                  if(consumer_conn) mysql_close(consumer_conn);
                  mysql_thread_end();
                  g_warning("Consumer thread %d ended",tdata->thread_id);
                  return;
             default:
                  g_critical("Events consumer read error event");
                  exit(EXIT_FAILURE);
         }
     }
     
}


void generate_consumer_schema_task(MYSQL *conn,job *j,thread_data *tdata){
      MYSQL_RES *res=NULL;
      MYSQL_ROW row;
      schema_job *sj=(schema_job *)j->job_data;
      
      res=mysql_query_rs(conn,sj->statement);
       
      row=mysql_fetch_row(res);
      consumer_job *cjob=g_new0(consumer_job,1);
      consumer_data *cdata=g_new0(consumer_data,1);
      cjob->sql=g_strdup(row[1]);
      cdata->type=CONSUMER_SCHEMA_IMPORT;
      cdata->config=tdata->config;
      cdata->c_data=(void *)cjob;
      g_async_queue_push(tdata->config->consumer_queue,cdata);


      mysql_close_result(res);
}


void generate_consumer_table_task(MYSQL *conn,job *j,thread_data *tdata){
      MYSQL_RES *res=NULL;
      MYSQL_ROW row;
      table_st_job *tsj=(table_st_job *)j->job_data;

      res=mysql_query_rs(conn,tsj->statement);

      row=mysql_fetch_row(res);

      //mysql_real_escape_string(conn, escaped->str, row[1], lengths[1]);
      consumer_job *cjob=g_new0(consumer_job,1);
      consumer_data *cdata=g_new0(consumer_data,1);
      cjob->sql=g_strdup(row[1]);
      cjob->table=tsj->table;
      cdata->type=CONSUMER_TABLE_IMPORT;
      cdata->config=tdata->config;
      cdata->c_data=(void *)cjob;
      g_async_queue_push(tdata->config->consumer_queue,cdata);

      mysql_close_result(res);

}

void generate_consumer_data_task(MYSQL *conn,job *j,thread_data *tdata){
     MYSQL_RES *res=NULL;
     MYSQL_FIELD *fields=NULL;
     MYSQL_ROW row;
     table_job *tj=(table_job *)j->job_data;
     guint num_fields = 0;
     guint64 num_rows_st = 0;

     res=mysql_query_rs(conn,tj->sql);

     fields=mysql_fetch_fields(res);   

     num_fields=mysql_num_fields(res);

     GString *escaped = g_string_sized_new(3000);

     GString* statement = g_string_sized_new(statement_size);   
     g_string_set_size(statement,0);

     GString *statement_row=g_string_sized_new(0);

     guint i=0;
     guint rows_in_st=0;

     while((row=mysql_fetch_row(res))){
       gulong *lengths=mysql_fetch_lengths(res);

       if(!statement->len){
           g_string_printf(statement,"insert into %s.%s values ",tj->table->db_name,tj->table->table_name);
           num_rows_st=0;
       }
       if(statement_row->len){
           g_string_append(statement,statement_row->str);
           g_string_append_c(statement,',');
           g_string_set_size(statement_row,0);
       }
       g_string_append_c(statement_row,'(');
       for(i=0;i<num_fields;i++){
           if(!row[i]){
                g_string_append(statement_row,"NULL");
           }else if(fields[i].flags & NUM_FLAG){
                g_string_append(statement_row,row[i]);
           }else{
                g_string_set_size(escaped, lengths[i]*2+1);
                mysql_real_escape_string(conn, escaped->str, row[i], lengths[i]);
                g_string_append_c(statement_row,'\"');
                g_string_append(statement_row,escaped->str);
                g_string_append_c(statement_row,'\"');
           }
          if(i < num_fields - 1){      
                g_string_append_c(statement_row,',');
          }else{
                g_string_append_c(statement_row,')');
                if((statement->len + statement_row->len +1) > statement_size){
                    if(!rows_in_st){
                       g_string_append(statement,statement_row->str);
                       g_string_set_size(statement_row,0);
                }
                       consumer_job *cjob=g_new0(consumer_job,1);
                       consumer_data *cdata=g_new0(consumer_data,1);
                       cjob->table=tj->table;
                       cjob->sql=g_strdup(statement->str);
                       cdata->c_data=(void *)cjob;
                       cdata->type=CONSUMER_IMPORT;
                       cdata->config=tdata->config;
                       while(1){
                          if(g_async_queue_length(tdata->config->consumer_queue) > 100){ 
                             nanosleep((const struct timespec[]){{0, 100000000L}}, NULL);
                          }else{
                             break;
                          }
                       }
                       g_async_queue_push(tdata->config->consumer_queue,cdata);
                       g_string_set_size(statement,0);
                       rows_in_st=0;
                       
                }else{
                     if(rows_in_st) g_string_append_c(statement,',');
                      g_string_append(statement,statement_row->str);
                      rows_in_st++;
                      g_string_set_size(statement_row,0);
                      
                } 
          }
        }
       
     }
       
       if(statement_row->len){
           if(!statement->len){
                g_string_printf(statement,"insert into %s.%s values ",tj->table->db_name,tj->table->table_name);
            }
                g_string_append(statement,statement_row->str);           
           
       }
    
       if(statement->len){
          consumer_job *cjob=g_new0(consumer_job,1);
          consumer_data *cdata=g_new0(consumer_data,1);
          cjob->table=tj->table;
          cjob->sql=g_strdup(statement->str);
          cdata->c_data=(void *)cjob;
          cdata->type=CONSUMER_IMPORT;
          cdata->config=tdata->config;
          g_async_queue_push(tdata->config->consumer_queue,cdata);
       }
       
      
       g_string_free(statement,TRUE);
       g_string_free(statement_row,TRUE);
       g_string_free(escaped,TRUE);
       mysql_close_result(res);
}

void events_producer(thread_data *tdata){
     configuration *conf=tdata->config;
     gchar *session_sql=g_strdup_printf("START TRANSACTION WITH CONSISTENT SNAPSHOT FROM SESSION %lu",conf->main_conn_id);
     MYSQL *producer_conn=mysql_multi_connect(hostname,port,username,password,NULL,init_mutex);
     
     if(mysql_query(producer_conn,"SET SESSION wait_timeout = 2147483")){
         g_error("Failed to set wait_timeout on producer conn,error : %s",mysql_error(producer_conn));
     }
     if(mysql_query(producer_conn,"SET SESSION net_read_timeout = 2147483")){
          g_error("Failed to increase source net_read_timeout: %s", mysql_error(producer_conn));
     }

     if(mysql_query(producer_conn,"SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")){
         g_error("Failed to set producer session ioslation level repeatable read,%s",mysql_error(producer_conn));
     }
    

     if(mysql_query(producer_conn,session_sql)){
         g_error("Failed to start transaction with consist snapshot from session %lu,error:%s",conf->main_conn_id,mysql_error(producer_conn));
     }

     if(mysql_query(producer_conn,"SET NAMES binary")){
         g_error("Failed to set session character set binary ,error : %s",mysql_error(producer_conn));
     }
      
     g_free(session_sql);
     g_async_queue_push(conf->ready,GINT_TO_POINTER(1));
     
     g_warning("Producer thread %d started",tdata->thread_id);

     table_job *tj=NULL;
     schema_job *sj=NULL;
     table_st_job *tsj=NULL;
     for(;;){
        job *j=(job *)g_async_queue_pop(conf->queue);
        switch(j->type){
            case JOB_DUMP:
                 tj=(table_job *)j->job_data;
                 g_message("Thread %d dumping data for %s",tdata->thread_id,tj->sql);
                 generate_consumer_data_task(producer_conn,j,tdata);
                 g_free(j);
                 g_free(tj->sql);
                 g_free(tj);
                 break;
            case SCHEMA_DUMP:
                 generate_consumer_schema_task(producer_conn,j,tdata);
                 sj=(schema_job *)j->job_data;
                 g_free(j);
                 g_free(sj->statement);
                 g_free(sj);
                 break;
            case TABLE_DUMP:
                 generate_consumer_table_task(producer_conn,j,tdata);
                 tsj=(table_st_job *)j->job_data;
                 g_free(j);
                 g_free(tsj->statement);
                 g_free(tsj);
                 break;
            case JOB_SHUTDOWN:
                 mysql_close(producer_conn);
                 g_free(j);
                 mysql_thread_end();
                 g_warning("Producer thread %d ended",tdata->thread_id);
                 return;
            default:
                 g_critical("Something very bad happened!");
                 exit(EXIT_FAILURE);
        }
          
     }
}

GThread **start_consumer_task(configuration *config){
     guint n=0;
     GThread **threads = g_new(GThread*,target_thread_num);
     thread_data *tdata=g_new0(thread_data,target_thread_num);
     for(n=0;n<target_thread_num;n++){
          tdata[n].config=config;
          tdata[n].thread_id=(guint)n;
          threads[n]=g_thread_create((GThreadFunc)events_consumer,&tdata[n],TRUE,NULL);
          g_async_queue_pop(config->consumer_ready);
     }
     g_async_queue_unref(config->consumer_ready);

     return threads;
}


void get_special_tables_in_one_database(MYSQL *conn,GList **tblist,gchar *dbname,gchar **tables){
     guint i=0;
     GString *tables_in_condition=g_string_sized_new(0);
     for(i=0;tables[i] != NULL;i++){
        if(i>0) g_string_append(tables_in_condition,",");
        g_string_append(tables_in_condition,"'");
        g_string_append(tables_in_condition,tables[i]);
        g_string_append(tables_in_condition,"'");
     }
     gchar *query=g_strdup_printf("SHOW TABLE STATUS FROM %s WHERE Name in (%s)",dbname,tables_in_condition->str);

     MYSQL_RES *res;
     MYSQL_ROW row;
     MYSQL_FIELD  *fields;
 
     res=mysql_query_rs(conn,query);
     fields=mysql_fetch_fields(res);
     g_free(query);

     gint name_col=-1,data_col=-1,row_col=-1;
     for(i=0;i<mysql_num_fields(res);i++){
       if(!g_strcasecmp(fields[i].name,"Name")) name_col=i;
       else if(!g_strcasecmp(fields[i].name,"Data_length")) data_col=i;
       else if(!g_strcasecmp(fields[i].name,"Rows")) row_col=i;
    }
   
    while((row=mysql_fetch_row(res))){
         tb *table=g_new0(tb,1);
         table->db_name=g_strdup(dbname);
         table->table_name=g_strdup_printf("`%s`",row[name_col]);
         if(row[data_col] ) table->datalength=strtoll(row[data_col],NULL,10);
         else table->datalength=0;
         if(row[row_col]) table->rows=strtoll(row[row_col],NULL,10);
         else table->rows=0;
         *tblist=g_list_append(*tblist,table);
          table_consumer_count++;
    }
   mysql_close_result(res);
}


void dump_special_tables_in_one_database(MYSQL *conn,configuration *config,GList *tblist,gchar *dbname,gchar **tables){
     GList *dblist=NULL;
     gchar *quote_dbname=g_strdup_printf("`%s`",dbname);
     dblist=g_list_append(dblist,quote_dbname);

     if(!no_schema) generate_database_structure_task(config,dblist);

     get_special_tables_in_one_database(conn,&tblist,quote_dbname,tables);
     if(precheck_no_innodb_tables(conn,tblist)){
         exit(EXIT_FAILURE);
     }
     if(!no_table){
        generate_table_structure_task(config,tblist);      
      }

     if(!no_data){
         tb *table=NULL;
         GList *iter=tblist;
         for(iter=g_list_first(iter);iter;iter=g_list_next(iter)){
             table=(tb *) iter->data;
             get_max_cardinality_index_in_table(conn,table);
             generate_dump_table_task(conn,table,config);
         }    
      }
    if(quote_dbname) g_free(quote_dbname);
    if(dblist) g_list_free(dblist);
   return ;
}

void dump_all_tables_in_one_database(MYSQL *conn,configuration *config,GList *tblist,gchar *dbname){
     GList *dblist=NULL;

     gchar *quote_dbname=g_strdup_printf("`%s`",dbname);
     dblist=g_list_append(dblist,dbname);
     
     if(!no_schema) generate_database_structure_task(config,dblist);
     
     get_tables_in_database(conn,quote_dbname,&tblist);
 
     if(precheck_no_innodb_tables(conn,tblist)){
        exit(EXIT_FAILURE);
     }

     if(!no_table) generate_table_structure_task(config,tblist);

     if(!no_data){
        tb *table=NULL;
        GList *iter=tblist;
        for(iter=g_list_first(iter);iter;iter=g_list_next(iter)){
           table=(tb *) iter->data;
           get_max_cardinality_index_in_table(conn,table);
           generate_dump_table_task(conn,table,config);
        }
     }    

     if(quote_dbname) g_free(quote_dbname);
     if(dblist) g_list_free(dblist);
     return;
}

unsigned long get_connection_id(MYSQL *conn){
     gchar *query=g_strdup_printf("SELECT CONNECTION_ID()");
     MYSQL_ROW row;
     MYSQL_RES *res;
     unsigned long connect_id;

     res=mysql_query_rs(conn,query);
     row=mysql_fetch_row(res);
     connect_id=strtoll(row[0],NULL,10);

     if(res) mysql_close_result(res);
     if(query) g_free(query);
     return connect_id;
     
}

void get_table_status(MYSQL *conn,tb *table,gchar *tbname){
     gchar *query=g_strdup_printf("SHOW TABLE STATUS FROM %s where Name='%s'",table->db_name,tbname);
     MYSQL_ROW row;
     MYSQL_RES *res;
     MYSQL_FIELD *fields;
   
     res=mysql_query_rs(conn,query);
     row=mysql_fetch_row(res);
     fields=mysql_fetch_fields(res);

     guint i=0,d_col=-1;
     for(i=0;i<mysql_num_fields(res);i++){
         if(!g_strcasecmp(fields[i].name,"Data_length")){
              d_col=i;
              break;
         }
     }

    if(row[d_col]) table->datalength=strtoll(row[d_col],NULL,10);
    else table->datalength=0;

    if(res) mysql_close_result(res);
    if(query) g_free(query);
    return ;
}

int dump_tables_only(MYSQL *conn,configuration *config,gchar **tables,GList **tblist){
     guint i=0;
     tb *tab;
     gchar **dt;
     for(i=0;tables[i] != NULL;i++){
         dt=g_strsplit(tables[i],".",2);
         tab=g_new0(tb,1);
         tab->db_name=g_strdup(g_strdup_printf("`%s`",dt[0]));
         tab->table_name=g_strdup_printf("`%s`",dt[1]);
         get_table_status(conn,tab,dt[1]);
         *tblist=g_list_append(*tblist,tab);
         g_strfreev(dt);
     }
    
     if(precheck_no_innodb_tables(conn,*tblist)){
          exit(EXIT_FAILURE);
     }

     if(!no_table) generate_table_structure_task(config,*tblist);

     if(!no_data){
        GList *iter=*tblist;
        for(iter=g_list_first(iter);iter;iter=g_list_next(iter)){
           tab=(tb *) iter->data;
           get_max_cardinality_index_in_table(conn,tab);
           generate_dump_table_task(conn,tab,config);
     }
     }
    return 0;
}

void write_end_dump_time(){
      char data_buffer[20];
      get_current_timestamp(data_buffer,20);
      metadata=g_fopen(metafile,"a");
      fprintf(metadata,"end dump : %s\n",data_buffer);
      fclose(metadata);
      char *success_file=g_strndup(metafile, (unsigned)strlen(metafile)-8);
      g_rename(metafile,success_file);
      g_free(metafile);
      g_free(success_file);
}

guint precheck_no_innodb_tables(MYSQL *conn,GList *tables){
      gchar *query=NULL;
      GList *iter=tables;
      tb *tmp_tb;
      gchar *tmp_tbname = NULL;
      int tbname_len=0;

      MYSQL_RES *res =NULL;
      MYSQL_FIELD *fields;
      MYSQL_ROW row;
      gint e_col=-1;
      guint i=0,no_innodb_count=0;
      GList *no_innodb_tblist = NULL;
      for(iter=g_list_first(iter);iter;iter=g_list_next(iter)){
           tmp_tb=(tb *) iter->data;
           tbname_len=strlen(tmp_tb->table_name);
           if(tmp_tb->table_name[0] == '`' && tmp_tb->table_name[tbname_len - 1 ] == '`')  { 
               tmp_tbname=g_strndup(tmp_tb->table_name+1,tbname_len -2 );
               query=g_strdup_printf("SHOW TABLE STATUS FROM %s WHERE NAME='%s'",tmp_tb->db_name,tmp_tbname);
            }else{
               query=g_strdup_printf("SHOW TABLE STATUS FROM %s WHERE NAME='%s'",tmp_tb->db_name,tmp_tb->table_name);
            }
           res=mysql_query_rs(conn,query);

           if(tmp_tbname) g_free(tmp_tbname);
           if(query) g_free(query);
           if(-1 == e_col){
              fields=mysql_fetch_fields(res);
              for(i=0;i<mysql_num_fields(res);i++){
                  if(!strcasecmp(fields[i].name,"Engine")) {
                      e_col=i;
                      break;
                  }
              }
           }
           row=mysql_fetch_row(res);
           if(!row[e_col] || g_strcasecmp(row[e_col],"InnoDB")){
               no_innodb_tblist=g_list_append(no_innodb_tblist,tmp_tb);
               no_innodb_count++;
           }
           if(res) mysql_close_result(res);
      }
      if(no_innodb_count){
         GString *message=g_string_sized_new(3000);;
         g_string_append_printf(message,"You have %d no-innodb tables",no_innodb_count);
         iter=no_innodb_tblist;
         for(iter=g_list_first(iter);iter;iter=g_list_next(iter)){
             tmp_tb=(tb *) iter->data;
             if(g_list_next(iter)) g_string_append_printf(message," (%s.%s),",tmp_tb->db_name,tmp_tb->table_name);
             else g_string_append_printf(message,"(%s.%s).",tmp_tb->db_name,tmp_tb->table_name);
 
         }
         g_critical(message->str);
         g_string_free(message,TRUE);
         g_list_free(no_innodb_tblist);
      }
     return no_innodb_count;
}

void start_dump_task(configuration *config,GThread **consumer_threads){
     MYSQL *conn;
     gchar **databases;
     GList *tables = NULL;
     masterInfo *masterinfo=g_new0(masterInfo,1);
     
     int dblen=-1,i=0;
      
     if(is_dump_all && (db || tables_list)){
            g_critical("Don't dump all databases and dump special databases or special tables ");
            exit(EXIT_FAILURE);
     }

     get_start_timeinfo();
     if(!output_directory) set_default_output_directory();
     create_backup_dir(output_directory);

     conn=get_main_conn();
     config->main_conn_id=get_connection_id(conn);
     do_master_status(conn,masterinfo);
     write_metadata_info(masterinfo);
     free_master_info(masterinfo);

     GThread **threads = g_new(GThread*,source_thread_num);
     thread_data *tdata=g_new0(thread_data,source_thread_num);
     /* new thread execute the dump events */
     guint n=0;
     for(n=0;n<source_thread_num;n++){
          tdata[n].config=config;
          tdata[n].thread_id=(guint)n;
          threads[n]=g_thread_create((GThreadFunc)events_producer,&tdata[n],TRUE,NULL);
          g_async_queue_pop(config->ready);
     }
         g_async_queue_unref(config->ready);

     if(is_dump_all){
           dump_all_databases(conn,config,tables);
     }
     else if(db){
        GList *dblist = NULL;
        gchar *prevdb = NULL;;
        databases=g_strsplit(db,",",0);
        while(databases[++dblen] != NULL);

        qsort(databases,dblen,sizeof(gchar *),cmp);

        gchar dbname[100];
        for(i=0;i<dblen;i++){
            if(!i) prevdb=databases[i];
            else {
               if(!g_strcmp0(databases[i],prevdb)) continue;
               prevdb=databases[i];
            }
             quote_name(databases[i],dbname);
             dblist=g_list_append(dblist,g_strdup(dbname));
           }
        dblen=schema_consumer_count=g_list_length(dblist);       
        if(dblen > 1){
            if(tables_list){
                g_warning("If you special multi databases and tables to dump ,we just ignore tables and dump databases only");
            }
            dump_all_tables_in_databases(conn,config,tables,dblist);
        }else if(dblen == 1){
            if(tables_list){
               gchar  **tbArr=g_strsplit(tables_list,",",0);
               //dump special tables in one database
               dump_special_tables_in_one_database(conn,config,tables,databases[0],tbArr);
               if(tbArr) g_strfreev(tbArr);
            }else{
               //dump all tables in database
               dump_all_tables_in_one_database(conn,config,tables,databases[0]);
            }
        }
        g_strfreev(databases);
    }else{
       if(tables_list){
              gchar **tbArr=g_strsplit(tables_list,",",0);
              //dump speciall  tables like test.t1 ,just dump the table t1 in database test
              dump_tables_only(conn,config,tbArr,&tables);
              if(tbArr) g_strfreev(tbArr);
        }else{
            g_error("Please input the legal databases or tables or all to dump and migration ");
        }
    }
    
    for(n=0;n<source_thread_num;n++){ 
        job *j = g_new0(job,1);
        j->type=JOB_SHUTDOWN;
        g_async_queue_push(config->queue,j);
    }

   for (n=0; n<source_thread_num; n++) {
        g_thread_join(threads[n]);
   }
   g_async_queue_unref(config->queue);
  

   g_free(threads);
   for (n=0;n<target_thread_num;n++){
        consumer_data *cd=g_new0(consumer_data,1);
        cd->type=CONSUMER_SHUTDOWN;
        cd->config=config;
        g_async_queue_push(config->consumer_queue,cd);
   }
   for(n=0;n<target_thread_num;n++){
       g_thread_join(consumer_threads[n]);
   }
   g_async_queue_unref(config->consumer_queue);
   
   GList *iter=tables;
   for(iter=g_list_first(iter);iter;iter=g_list_next(iter)){
      free_table((tb *)iter->data);
   }
   mysql_close(conn);
   write_end_dump_time();
   mysql_thread_end();
   mysql_library_end();
   if(output_directory) g_free(output_directory);
   if(init_mutex) g_mutex_free(init_mutex);
   if(init_consumer_mutex) g_mutex_free(init_consumer_mutex);
}

void *exec_thread(void *data) {
    configuration *config=(configuration *)data;
    g_async_queue_pop(config->schedule_queue);
    GThread **consumer_threads=start_consumer_task(config);
    start_dump_task(config,consumer_threads);
    g_free(consumer_threads);
    g_async_queue_unref(config->schedule_queue);

    sig_triggered(NULL);
    return NULL;
}

int main(int argc,char **argv){
   GOptionContext  *context;
   GError *error = NULL;

   g_thread_init(NULL);
   init_mutex=g_mutex_new();
   init_consumer_mutex=g_mutex_new();

   context=g_option_context_new("-- MySQL DataPumper");
   GOptionGroup *main_group= g_option_group_new("main", "Main Options", "Main Options", NULL, NULL);
   g_option_group_add_entries(main_group, entries);
   g_option_context_set_main_group(context, main_group);
   if (!g_option_context_parse(context, &argc, &argv, &error)) {
        g_critical ("option parsing failed: %s, try --help\n", error->message);
        exit (EXIT_FAILURE);
    }
   
   g_option_context_free(context);
 
   set_verbose(verbose);

   if(version){
      g_message("datapump version is %d.%d\n",datapump_version_major,datapump_version_minor);
      exit (EXIT_SUCCESS);
   }

   if(no_schema)  schema_consumer_done=1;
   if(no_table)   table_consumer_done=1;
    
   if (daemon_mode) {
        pid_t pid, sid;

        pid= fork();
        if (pid < 0)
            exit(EXIT_FAILURE);
        else if (pid > 0)
            exit(EXIT_SUCCESS);

        umask(0);
        sid= setsid();

        if (sid < 0)
            exit(EXIT_FAILURE); 
     }

    if(is_dump){
       configuration config={NULL,NULL,NULL,NULL,NULL,0};
       config.queue=g_async_queue_new();
       config.ready=g_async_queue_new();
       config.consumer_queue=g_async_queue_new();
       config.consumer_ready=g_async_queue_new();
       config.schedule_queue=g_async_queue_new();
       g_thread_create(exec_thread,(void *) &config, FALSE,NULL);

       g_async_queue_push(config.schedule_queue,GINT_TO_POINTER(1));
       
    }
     m1= g_main_loop_new(NULL, TRUE);
     g_main_loop_run(m1); 
    
     return 0;
}

void get_current_timestamp(char *date,int length){
     time_t rawtime;
     struct tm timeinfo;

     time(&rawtime);
     localtime_r(&rawtime,&timeinfo);
     strftime(date,length,"%Y-%m-%d %H:%M:%S",&timeinfo);

     return ;
}

void write_log_file(const gchar *log_domain, GLogLevelFlags log_level, const gchar *message, gpointer user_data) {
    (void) log_domain;
    (void) user_data;

    gchar date[20];
    time_t rawtime;
    struct tm timeinfo;

    time(&rawtime);
    localtime_r(&rawtime, &timeinfo);
    strftime(date, 20, "%Y-%m-%d %H:%M:%S", &timeinfo);

    GString* message_out = g_string_new(date);
    if (log_level & G_LOG_LEVEL_DEBUG) {
        g_string_append(message_out, " [DEBUG] - ");
    } else if ((log_level & G_LOG_LEVEL_INFO)
        || (log_level & G_LOG_LEVEL_MESSAGE)) {
        g_string_append(message_out, " [INFO] - ");
    } else if (log_level & G_LOG_LEVEL_WARNING) {
        g_string_append(message_out, " [WARNING] - ");
    } else if ((log_level & G_LOG_LEVEL_ERROR)
        || (log_level & G_LOG_LEVEL_CRITICAL)) {
        g_string_append(message_out, " [ERROR] - ");
    }

    g_string_append_printf(message_out, "%s\n", message);
    if (write(fileno(logfile), message_out->str, message_out->len) <= 0) {
        fprintf(stderr, "Cannot write to log file with error %d.  Exiting...", errno);
    }
    g_string_free(message_out, TRUE);
}


 gboolean sig_triggered(gpointer user_data) {
     (void) user_data;
 
     g_message("Shutting down gracefully");
     g_main_loop_quit(m1);
     return FALSE;
 }

