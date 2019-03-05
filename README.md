#PELT
Very simple high performance c based PELT (Prepare->Extract->Load->Transform) solution
Warning, this is my first real c program and it is far far from perfect yet, there be bugs !

## Supported Databases
PostgreSQL
MySQL (TODO)
CSV (TODO)

## Usage
```./pelt configuration.conf```

## Build
Yeah, I should move to configure and make eventually, but it is easy for now.
```./build.sh```

## Requires
libconfig
libpq
libmysql TODO
(a CSV library) TODO

## Execution Model
Read configuration file
Run destination->pre's
Run source->pre's
Run destination->key
Run source->data => destination->data
Run destination->post's
Run source->post's

## Example Config
# Example application configuration
```
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