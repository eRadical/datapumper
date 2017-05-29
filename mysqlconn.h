#include<mysql.h>
#include<glib.h>


MYSQL *mysql_connect(gchar *host,gint port,gchar *user,gchar *password,gchar *database);
MYSQL *mysql_multi_connect(gchar *host,gint port,gchar *user,gchar *password,gchar *database,GMutex *init_mutex);
void mysql_close_connect(MYSQL *conn);
void mysql_close_result(MYSQL_RES *res);
MYSQL_RES *mysql_query_rs(MYSQL *conn,gchar *sql);
int mysql_query_with_error_report(MYSQL *conn, MYSQL_RES **res,const char *query);
void mysql_execute(MYSQL *conn,gchar *sql);




