#include <libpq-fe.h>
#include <libconfig.h>
char *get_key_pq(PGconn *conn, const char *sql);
void run_blind_pq(PGconn *conn, config_setting_t *cfgset);
void insert_prepare(char *sqlto, int boost, int fields, const char *sql_destination);