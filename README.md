# PELT
Very simple high performance c based PELT (Prepare->Extract->Load->Transform) solution
Warning, this is my first real c program and it is far far from perfect yet, there be bugs !

## Performance
On a basic test case these are the current results
* CREATE TABLE AS SELECT on PostgreSQL 11.2 1.6M rows in 11.686s
* COPY TO FILE, COPY FROM FILE on PostgreSQL 11.2 to PostgreSQL 11.2 1.6M rows in 19.809s
* pelt with 1 commit PostgreSQL 11.2 to PostgreSQL 11.2 in 35m7.348s
* pelt with 10,000 commit PostgreSQL 11.2 to PostgreSQL 11.2 in 6m55.410s
* pelt with final commit PostgreSQL 11.2 to PostgreSQL 11.2 in 6m52.138s

## Supported Sources & Destinations
* PostgreSQL
* MySQL (TODO)
* CSV (TODO)

## Usage
```./pelt configuration.conf```

## Build
Yeah, I should move to configure and make eventually, but it is easy for now.
```./build.sh```

## Requires
* libconfig
* libpq
* libmysql TODO
* (a CSV library) TODO

## Execution Model
1. Read configuration file
2. Run destination->pre's
3. Run source->pre's
4. Run destination->key
5. Run source->data => destination->data
6. Run destination->post's
7. Run source->post's

## Example Config
# Example application configuration
```
commit = 0;
notify = 10000;
source: {
    conn: {
        type = "postgresql"; // postgresql/mysql/csv
        host = "localhost";
        dbnm = "databaseA";
        user = "";
        pass = "";
        port = 5432;
    };
    pre: (
        "SELECT COUNT(*) FROM customer"
    ); // Array of SQL's or Script's to pre run
    key  = "customer_id"; // In source context this is the column name
    data = "SELECT * FROM customer WHERE customer_id > $1 ORDER BY customer_id"; // SELECT statement, or File
    post: (); // Array of SQL's or Script's to post run
};

destination: {
    conn: {
        type = "postgresql"; // postgresql/mysql/csv
        host = "localhost";
        dbnm = "databaseB";
        user = "";
        pass = "";
        port = 5432;
    };
    pre: (
        "SELECT COUNT(*) FROM customer_copy",
        "TRUNCATE TABLE customer_copy"
    ); // Array of SQL's or Script's to pre run
    key  = "SELECT MAX(customer_id) FROM customer_copy"; // In destination context this is the key value
    data = "INSERT INTO customer_copy VALUES"; // INSERT SQL prefix, or file
    post: (
        "SELECT COUNT(*) FROM customer_copy"
    ); // Array of SQL's or Script's to post run
};
```