#include <string.h>
#include <libpq-fe.h>
int notify;
int commit;
void extract_pq_load_pq(
    PGconn *conn_source,      const char *sql_source,     const char *filter,
    PGconn *conn_destination, const char *sql_destination
);
void extract_pq_load_pq_async(
    PGconn *conn_source,      const char *sql_source,     const char *filter,
    PGconn *conn_destination, const char *sql_destination
);