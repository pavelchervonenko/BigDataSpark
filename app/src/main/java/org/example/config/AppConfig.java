package org.example.config;

public class AppConfig {
    public static final String PG_URL = "jdbc:postgresql://postgres:5432/snowflake_db";
    public static final String PG_USER = "postgres";
    public static final String PG_PASSWORD = "postgres";
    public static final String PG_DRIVER = "org.postgresql.Driver";

    public static final String CH_URL = "jdbc:clickhouse://clickhouse:8123/reports";
    public static final String CH_USER = "default";
    public static final String CH_PASSWORD = "";

    public static final String CASSANDRA_HOST = "cassandra";
    public static final String CASSANDRA_PORT = "9042";
    public static final String CASSANDRA_KEYSPACE = "reports";

    public static final String MONGO_URI = "mongodb://mongo:27017";
    public static final String MONGO_DATABASE = "reports";
}
