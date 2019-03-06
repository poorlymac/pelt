#include <string.h>
#include <libpq-fe.h>
#include "extract.h"
#include "postgres.h"

int notify;
int commit;

void extract_pq_load_pq(
    PGconn *conn_source,      const char *sql_source,     const char *filter,
    PGconn *conn_destination, const char *sql_destination
) {
    PGresult  *res;
    PGresult  *ins;
    PGresult *stmt;
    int inserted = 0;
    int failed   = 0;
    int boost    = 0;
    char* stmtName = "PELT_INSERT";

    // Run SQL
    if (strstr(sql_source, "$1") != NULL) {
        fprintf(stdout, "Extract SQL: %s using %s\n", sql_source, filter);
        const char *paramValues[1];
        paramValues[0] = filter;
        // If 1 we get binary and cannot see integers etc.
        res = PQexecParams(conn_source, sql_source, 1, NULL, paramValues, NULL, NULL,0);
    } else {
        fprintf(stdout, "Extract SQL: %s\n", sql_source);
        res = PQexec(conn_source, sql_source);
    }

    // Handle Data
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "SQL failed: %s\n", PQerrorMessage(conn_source));
        PQclear(res);
    } else {
        // Get result information
        int fields = PQnfields(res);
        int rows   = PQntuples(res);
        fprintf(stdout, "Rows : %i\n", rows);

        // Calculate a suitable boost value
        boost = (65535 - strlen(sql_destination))/fields;
        fprintf(stdout, "Boost set to : %i\n", boost);

        // Create SQL statement
        char sqlto[boost * fields * 10];
        insert_prepare(sqlto, boost, fields, sql_destination);

        // Begin transaction
        const char *paramValues[fields];
        ins = PQexec(conn_destination, "BEGIN");
        if (PQresultStatus(ins) != PGRES_COMMAND_OK) {
            fprintf(stdout, "BEGIN command failed\n");        
        }
        PQclear(ins);

        // Prepare
        stmt = PQprepare(conn_destination, stmtName, sqlto, fields, NULL);
        if ( PQresultStatus(stmt) != PGRES_COMMAND_OK ) {
            fprintf(stderr, "Prepare failed: (%i) %s\n", PQresultStatus(stmt), PQerrorMessage(conn_destination));
        }

        // int rows = PQntuples(res);
        int set  = boost;
        for (int i = 0; i < rows; i = i + boost) {
            // See if we are on our last set
            if((i + boost) > rows) {
                // Customise and reprepare down to the remaining rows
                set = rows - i;
                insert_prepare(sqlto, set, fields, sql_destination);

                // Prepare, need a different name to work
                stmtName = "PELT_INSERT_LAST";
                stmt = PQprepare(conn_destination, stmtName, sqlto, fields, NULL);
                if ( PQresultStatus(stmt) != PGRES_COMMAND_OK ) {
                    fprintf(stderr, "Prepare failed: (%i) %s\n", PQresultStatus(stmt), PQerrorMessage(conn_destination));
                }
            }
            int x = 0;
            for (int r = 0; r < set; r++) {
                for (int j = 0; j < fields; j++) {
                    if(PQgetisnull(res, r + i, j) == 1) {
                        paramValues[x] = NULL;
                    } else {
                        paramValues[x] = PQgetvalue(res, r + i, j);
                    }
                    x++;
                }
            }

            ins = PQexecPrepared(conn_destination, stmtName, fields * set, paramValues, NULL, NULL, 0);
            if ( PQresultStatus(ins) != PGRES_COMMAND_OK ) {
                failed++;
                if(notify != 0 && (failed % notify) == 0 && failed != 0) fprintf(stdout, "Failed   %i ...\n", failed);
            } else {
                inserted+=set;
                if(inserted % (boost * 10) == 0 ) fprintf(stdout, "Inserted %i ...\n", inserted);
            }
        }
        if(failed   > 0) fprintf(stdout, "Failed   %i.\n", failed);
        if(inserted > 0) fprintf(stdout, "Inserted %i.\n", inserted);
        ins = PQexec(conn_destination, "COMMIT");
        if (PQresultStatus(ins) != PGRES_COMMAND_OK) fprintf(stdout, "COMMIT final failed\n");
        PQclear(ins);
        PQclear(res);
    }
}

void extract_pq_load_pq_async(
    PGconn *conn_source,      const char *sql_source,     const char *filter,
    PGconn *conn_destination, const char *sql_destination
) {
    PGresult  *res;
    PGresult  *ins;
    PGresult *stmt;
    int inserted = 0;
    int failed   = 0;
    int boost    = 0;
    char* stmtName = "PELT_INSERT";

    // Run SQL
    if (strstr(sql_source, "$1") != NULL) {
        fprintf(stdout, "Extract SQL: %s using %s\n", sql_source, filter);
        const char *paramValues[1];
        paramValues[0] = filter;
        // If 1 we get binary and cannot see integers etc.
        res = PQexecParams(conn_source, sql_source, 1, NULL, paramValues, NULL, NULL,0);
    } else {
        fprintf(stdout, "Extract SQL: %s\n", sql_source);
        if (!PQsendQuery(conn_source, sql_source)) {
            fprintf(stderr, "Sending extract to server failed: %s\n", PQerrorMessage(conn_source));
        }
    }

    // Set to single row mode
    if (!PQsetSingleRowMode(conn_source)) {
        fprintf(stderr, "Failed to set single row mode: %s\n", PQerrorMessage(conn_source));
    }

    // Begin transaction
    ins = PQexec(conn_destination, "BEGIN");
    if (PQresultStatus(ins) != PGRES_COMMAND_OK) {
        fprintf(stdout, "BEGIN command failed\n");
    }
    PQclear(ins);

    // Loop on rows
    int firstrun = 0;
    int fields   = 0;
    int rows     = 0;
    while (1) {
        // Set up and prepare SQL
        if(firstrun == 0) {
            // Get result information
            res = PQgetResult(conn_source);
            fields = PQnfields(res);

            // TODO: Calculate a suitable boost value
            boost = 1300;
            fprintf(stdout, "Boost set to : %i\n", boost);

            // Create SQL statement
            char sqlto[boost * fields * 10];
            insert_prepare(sqlto, boost, fields, sql_destination);

            // Prepare
            stmt = PQprepare(conn_destination, stmtName, sqlto, fields, NULL);
            if ( PQresultStatus(stmt) != PGRES_COMMAND_OK ) {
                fprintf(stderr, "Prepare failed: (%i) %s\n", PQresultStatus(stmt), PQerrorMessage(conn_destination));
            }
        }
        firstrun++;

        // Process each row
        const char *paramValues[boost * fields];
        int l = 0;
        for (int k = 0; k < boost; k++) {
            for (int j = 0; j < fields; j++) {
                if(PQgetisnull(res, 0, j) == 1) {
                    paramValues[l] = NULL;
                } else {
                    paramValues[l] = PQgetvalue(res, 0, j);
                }
                l++;
            }
            res = PQgetResult(conn_source);
            if(res == NULL) {
                k = boost;
            }
        }
        // Do insert, checking for l value change
        if(l == boost * fields) {
            ins = PQexecPrepared(conn_destination, stmtName, l, paramValues, NULL, NULL, 0);
        } else {
            // Reconstruct and do last
            // Create SQL statement
            char sqlto[boost * fields * 10];
            insert_prepare(sqlto, l/fields, fields, sql_destination);

            // Prepare
            stmtName = "PELT_INSERT_LAST";
            stmt = PQprepare(conn_destination, stmtName, sqlto, fields, NULL);
            if ( PQresultStatus(stmt) != PGRES_COMMAND_OK ) {
                fprintf(stderr, "Prepare failed: (%i) %s\n", PQresultStatus(stmt), PQerrorMessage(conn_destination));
            }
        }
        if ( PQresultStatus(ins) != PGRES_COMMAND_OK ) {
            failed++;
            if(notify != 0 && (failed % notify) == 0 && failed != 0) fprintf(stdout, "Failed   %i ...\n", failed);
        } else {
            inserted+=l/fields;
            if(inserted % (boost * 10) == 0 ) fprintf(stdout, "Inserted %i ...\n", inserted);
        }

        // Exit if null
        if(res == NULL) {
            break;
        }
   }

    // Commit transaction
    ins = PQexec(conn_destination, "COMMIT");
    if (PQresultStatus(ins) != PGRES_COMMAND_OK) {
        fprintf(stdout, "COMMIT command failed\n");        
    }    
    PQclear(ins);

    PQclear(res);
}