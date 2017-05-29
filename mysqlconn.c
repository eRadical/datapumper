#include<mysqlconn.h>
#include<stdlib.h>
#include<stdio.h>


MYSQL *mysql_connect(gchar *host,gint port,gchar *user,gchar *password,gchar *database){
      MYSQL *conn;
      conn=mysql_init(NULL);
      if(!mysql_real_connect(conn,host,user,password,database,port,NULL,0)){
          g_error("Failed get mysql connection,error : %s",mysql_error(conn));
      }
     return conn;
}

MYSQL *mysql_multi_connect(gchar *host,gint port,gchar *user,gchar *password,gchar *database,GMutex *init_mutex){
       MYSQL *conn;
       g_mutex_lock(init_mutex);
       conn=mysql_init(NULL);
       g_mutex_unlock(init_mutex);
       if(!mysql_real_connect(conn,host,user,password,database,port,NULL,0)){
             g_error("Failed get mysql connection,error : %s",mysql_error(conn));
       }

      return conn;
}

void mysql_close_connect(MYSQL *conn){
     if(conn){
       mysql_close(conn);  
       conn=NULL;
     }
}

void mysql_close_result(MYSQL_RES *res){
     if(res){
       mysql_free_result(res);
       res=NULL;
     }
}


MYSQL_RES *mysql_query_rs(MYSQL *conn,gchar *sql){
   if(!conn || !sql){
     g_error("Conn is invalid or sql is null \n");
     exit (EXIT_FAILURE);
    }
   if(mysql_query(conn,sql)){
      g_error("Query ( %s ) error : %s\n",sql,mysql_error(conn));
      exit(EXIT_FAILURE);
   }  
   
   MYSQL_RES *res=mysql_use_result(conn);
   return res;
}

void mysql_execute(MYSQL *conn,gchar *sql){
   if(!conn || !sql){
      fprintf(stderr,"conn is invalid or sql is null \n");
      exit (EXIT_FAILURE);
   }
   if(mysql_query(conn,sql)){
      fprintf(stderr,"%s\n",mysql_error(conn));
      exit(EXIT_FAILURE);
   }
}

int mysql_query_with_error_report(MYSQL *mysql_con, MYSQL_RES **res,const char *query)
{
  if (mysql_query(mysql_con, query) || (res && !((*res)= mysql_store_result(mysql_con))))
  {
    fprintf(stderr,"Couldn't execute '%s': %s (%d)",query, mysql_error(mysql_con), mysql_errno(mysql_con));
    return 1;
  }
  return 0;
}

