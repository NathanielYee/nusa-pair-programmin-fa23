# DB_connection
## DB_connection contains the interfaces and implementations of classes used to connect to various remote datasources

Currently DB_connection contains two files:

### db_base_connection
    db_base_connection contains the interfaces BaseConnection, DBBaseConnection, and QueryResult. These provide the fundamental methods needed to connect to a remote datasource, query it, and receive the response gracefully.

### db_implement
    db_implement contains the implementation of the DBBaseConnection interface. It implements functionality to connect to a PostgreSQL database, execute queries against the database, and write to tables in the database.