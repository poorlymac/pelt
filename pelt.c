#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <libconfig.h>
#include <libpq-fe.h>
#include <mysql.h>
#include <csv.h>
#include "postgres.h"
#include "extract.h"

static void exit_nicely(PGconn *conn1, PGconn *conn2, config_t *cfg, int exitstatus) {
    PQfinish(conn1);
    PQfinish(conn2);
    config_destroy(cfg);
    fprintf(stderr, "Exiting with %i\n", exitstatus);
    exit(exitstatus);
}

char *get_key(const char *type, PGconn *conn, const char *sql) {
    char *key;
    fprintf(stdout, "Running SQL: %s\n", sql);
    if(strncmp(type, "postgresql", 10) == 0) {
        key = get_key_pq(conn, sql);
    } else if (strncmp(type, "mysql", 5) == 0) {
        // TODO
        // key = get_key_my(conn, sql);
    } else if (strncmp(type, "csv", 3) == 0) {
        // TODO
        // key = get_key_csv(conn, sql);
    }
    return key;
}

void run_blind(const char *type, PGconn *conn, config_setting_t *cfgset) {
    if(strncmp(type, "postgresql", 10) == 0) {
        run_blind_pq(conn, cfgset);
    } else if (strncmp(type, "mysql", 5) == 0) {
        // TODO
        // run_blind_my(conn, cfgset);
    } else if (strncmp(type, "csv", 3) == 0) {
        // TODO
        // run_blind_csv(conn, cfgset);
    }
}

void extract_load(
    const char *type_source, PGconn *conn_source, const char *data_source, const char *filter,
    const char *type_destination,   PGconn *conn_destination,   const char *data_destination
) {
    if(strncmp(type_source, "postgresql", 10) == 0 && strncmp(type_destination, "postgresql", 10) == 0) {
        extract_pq_load_pq(conn_source, data_source, filter, conn_destination, data_destination);
    } else if (strncmp(type_source, "postgresql", 10) == 0 && strncmp(type_destination, "mysql",       5) == 0) {
        // extract_pq_load_my(conn_source, data_source, filter, conn_destination, data_destination);
    } else if (strncmp(type_source, "mysql",       5) == 0 && strncmp(type_destination, "mysql",       5) == 0) {
        // extract_my_load_my(conn_source, data_source, filter, conn_destination, data_destination);
    } else if (strncmp(type_source, "mysql",       5) == 0 && strncmp(type_destination, "postgresql", 10) == 0) {
        // extract_my_load_pq(conn_source, data_source, filter, conn_destination, data_destination);
    }    
}

int main(int argc, char **argv) {
    config_t cfg;
    config_setting_t *cfgset;
    // Source
    const char *source_type;
    const char *source_host;
    const char *source_data;
    const char *source_user;
    const char *source_pass;
    char source_conn[256] = "connect_timeout=10";
    int  source_port;
    // Destination
    const char *destination_type;
    const char *destination_host;
    const char *destination_data;
    const char *destination_user;
    const char *destination_pass;
    const char *data_destination;
    const char *data_source;
    const char *key_source;
    const char *date_source;
    const char *key;
    const char *notify_value;
    const char *commit_value;
    char destination_conn[256];
    int  destination_port;
    PGconn     *conn_source;
    PGconn     *conn_destination;

    if(argc != 2) {
        fprintf(stdout, "USAGE:%s file.cfg\n", argv[0]);
        return(EXIT_SUCCESS);
    } else {
        // Check if the file exists
        if(access(argv[1], R_OK ) == -1) {
            fprintf(stderr, "Configuration file '%s' doesn't exist or is not readable\n", argv[1]);
            return(EXIT_FAILURE);
        }
    }
    /* Read the file. If there is an error, report it and exit. */
    config_init(&cfg);
    if(! config_read_file(&cfg, argv[1])) {
        fprintf(stderr, "%s:%d - %s\n", config_error_file(&cfg), config_error_line(&cfg), config_error_text(&cfg));
        config_destroy(&cfg);
        return(EXIT_FAILURE);
    }

    // Set the global notify and commit
    config_lookup_int(&cfg, "notify", &notify);
    config_lookup_int(&cfg, "commit", &commit);
    // fprintf(stdout, "Notify %s, Commit %s\n", notify_value, commit_value);
    // notify = atoi(notify_value);
    // commit = atoi(commit_value);
    fprintf(stdout, "Notify Integer %i, Commit Integer %i\n", notify, commit);

    // connect to source
    config_lookup_string(&cfg, "source.conn.type", &source_type);
    config_lookup_string(&cfg, "source.conn.host", &source_host);
    config_lookup_string(&cfg, "source.conn.dbnm", &source_data);
    config_lookup_string(&cfg, "source.conn.user", &source_user);
    config_lookup_string(&cfg, "source.conn.pass", &source_pass);
    config_lookup_int(   &cfg, "source.conn.port", &source_port);

    // TODO : make this connection inspecific
    if(source_host != NULL && strncmp(source_host, "", 1) != 0) sprintf(source_conn, "%s host=%s",     source_conn, source_host);
    if(source_data != NULL && strncmp(source_data, "", 1) != 0) sprintf(source_conn, "%s dbname=%s",   source_conn, source_data);
    if(source_user != NULL && strncmp(source_user, "", 1) != 0) sprintf(source_conn, "%s user=%s",     source_conn, source_user);
    if(source_port !=  0) sprintf(source_conn, "%s port=%i", source_conn, source_port);
    fprintf(stderr, "Connecting to FROM database %s\n", source_conn);
    if(strncmp(source_pass, "", 1) != 0) sprintf(source_conn, "%s password=%s", source_conn, source_pass);
    conn_source = PQconnectdb(source_conn);

    // connect to destination
    config_lookup_string(&cfg, "destination.conn.type", &destination_type);
    config_lookup_string(&cfg, "destination.conn.host", &destination_host);
    config_lookup_string(&cfg, "destination.conn.dbnm", &destination_data);
    config_lookup_string(&cfg, "destination.conn.user", &destination_user);
    config_lookup_string(&cfg, "destination.conn.pass", &destination_pass);
    config_lookup_int(   &cfg, "destination.conn.port", &destination_port);

    // TODO : make this connection inspecific
    if(destination_host != NULL && strncmp(destination_host, "", 1) != 0) sprintf(destination_conn, "%s host=%s",     destination_conn, destination_host);
    if(destination_data != NULL && strncmp(destination_data, "", 1) != 0) sprintf(destination_conn, "%s dbname=%s",   destination_conn, destination_data);
    if(destination_user != NULL && strncmp(destination_user, "", 1) != 0) sprintf(destination_conn, "%s user=%s",     destination_conn, destination_user);
    if(destination_port !=  0) sprintf(destination_conn, "%s port=%i", destination_conn, destination_port);
    fprintf(stderr, "Connecting to TO database %s\n", destination_conn);
    if(strncmp(destination_pass, "", 1) != 0) sprintf(destination_conn, "%s password=%s", destination_conn, destination_pass);
    conn_destination = PQconnectdb(destination_conn);

    // Check connections
    // TODO : make this check inspecific
    if (PQstatus(conn_source) != CONNECTION_OK) {
        fprintf(stderr, "Failed to connect to FROM database %s: %s\n", source_conn, PQerrorMessage(conn_source));
        if(PQstatus(conn_destination) != CONNECTION_OK) {
            fprintf(stderr, "Failed to connect to TO database %s: %s\n", destination_conn, PQerrorMessage(conn_destination));
        }
        exit_nicely(conn_source, conn_destination, &cfg, EXIT_FAILURE);
    } else {
        if(PQstatus(conn_destination) != CONNECTION_OK) {
            fprintf(stderr, "Failed to connect to TO database %s: %s\n", destination_conn, PQerrorMessage(conn_destination));
            exit_nicely(conn_source, conn_destination, &cfg, EXIT_FAILURE);
        }
    }

    /*
        Run Order
        =========
        1) destination->pre's
        2) source->pre's
        3) destination->key OR destination->date
        4) source>sql => destination->sql
        5) destination->post's
        6) source->post's
    */

    // TODO: stop on error

    // Run pre SQL's
    run_blind(destination_type, conn_destination, config_lookup(&cfg, "destination.pre")); // 1)
    run_blind(source_type,      conn_source,      config_lookup(&cfg, "source.pre"     )); // 2)

    // Get the SELECT and INSERT SQL
    config_lookup_string(&cfg, "destination.data", &data_destination);
    config_lookup_string(&cfg, "source.data",      &data_source     );

    // Get Key used to filter the extract
    config_lookup_string(&cfg, "destination.key", &data_destination); // 3a)
    if (data_destination != NULL && strlen(data_destination) > 0) {
        key = get_key(destination_type, conn_destination, data_destination);
        if (strlen(key) == 0) {
            key = "0";
        }
        fprintf(stdout, "Using key: %s\n", key);
    }

    // Run the extract and insert
    // 4)
    config_lookup_string(&cfg, "source.key",       &key_source);
    config_lookup_string(&cfg, "destination.data", &data_destination);
    extract_load(
        source_type,      conn_source,      data_source,     key,
        destination_type, conn_destination, data_destination
    );

    // Run post SQL's
    run_blind(destination_type, conn_destination, config_lookup(&cfg, "destination.post")); // 5)
    run_blind(source_type,      conn_source,      config_lookup(&cfg, "source.post"     )); // 6)

    // Time to clean up
    exit_nicely(conn_source, conn_destination, &cfg, EXIT_SUCCESS);
}