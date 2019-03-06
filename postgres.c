#include "postgres.h"

void insert_prepare(char *sqlto, int boost, int fields, const char *sql_destination) {
    sprintf(sqlto, "%s (", sql_destination);
    int i;
    for (i = 1; i < fields * boost; i++) {
        if(i != 0 && (i % fields) == 0) {
            sprintf(sqlto, "%s$%i),(", sqlto, i);
        } else {
            sprintf(sqlto, "%s$%i,", sqlto, i);
        }
    }
    sprintf(sqlto, "%s$%i)", sqlto, i);
}

char *get_key_pq(PGconn *conn, const char *sql) {
    PGresult *res;
    char *key;
    res = PQexec(conn, sql);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "SQL failed: %s\n", PQerrorMessage(conn));
    } else {
        if(PQntuples(res) == 1) {
            key = PQgetvalue(res, 0, 0);
        }
    }
    PQclear(res);
    return key;
}

void run_blind_pq(PGconn *conn, config_setting_t *cfgset) {
    PGresult   *res;
    if(cfgset != NULL) {
        int count = config_setting_length(cfgset);
        for(int s = 0; s < count; ++s) {
            const char *presql = config_setting_get_string_elem(cfgset, s);
            fprintf(stdout, "Running SQL blind: %s\n", presql);
            res = PQexec(conn, presql);
            if(PQresultStatus(res) == PGRES_COMMAND_OK) {
                fprintf(stdout, "Running SQL blind succeded\n");
            } else if (PQresultStatus(res) == PGRES_TUPLES_OK) {
                // Output the results
                int fields = PQnfields(res);
                fprintf(stdout, "Running SQL blind rows : %s\n", PQcmdTuples(res));
                for (int j = 0; j < fields; j++) {
                    fprintf(stdout, "%15s,", PQfname(res, j));
                }
                fprintf(stdout, "\n");
                for (int i = 0; i < PQntuples(res); i++) {
                    for (int j = 0; j < fields; j++) {
                        fprintf(stdout, "%15s,", PQgetvalue(res, i, j));
                    }
                    fprintf(stdout, "\n");
                }
            } else {
                fprintf(stderr, "Running SQL blind failed: %s", PQerrorMessage(conn));
            }
            PQclear(res);
        }
    }
}