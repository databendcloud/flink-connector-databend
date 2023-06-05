package org.apache.flink.connector.databend.config;

public class DatabendConfig {
    public static final String IDENTIFIER = "databend";

    public static final String PROPERTIES_PREFIX = "properties.";

    public static final String URL = "url";

    public static final String USERNAME = "username";

    public static final String PASSWORD = "password";

    public static final String DATABASE_NAME = "database-name";

    public static final String TABLE_NAME = "table-name";

    public static final String SINK_BATCH_SIZE = "sink.batch-size";

    public static final String SINK_FLUSH_INTERVAL = "sink.flush-interval";

    public static final String SINK_MAX_RETRIES = "sink.max-retries";

    public static final String SINK_UPDATE_STRATEGY = "sink.update-strategy";
    public static final String SINK_PRIMARY_KEYS = "sink.primary-key";

    public static final String SINK_IGNORE_DELETE = "sink.ignore-delete";
}
