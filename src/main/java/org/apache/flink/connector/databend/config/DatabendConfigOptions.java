package org.apache.flink.connector.databend.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;

public class DatabendConfigOptions {
    public static final ConfigOption<String> URL = ConfigOptions.key(DatabendConfig.URL).stringType().noDefaultValue().withDescription("The Databend url in format `databend://<host>:<port>`.");

    public static final ConfigOption<String> USERNAME = ConfigOptions.key(DatabendConfig.USERNAME).stringType().noDefaultValue().withDescription("The Databend username.");

    public static final ConfigOption<String> PASSWORD = ConfigOptions.key(DatabendConfig.PASSWORD).stringType().noDefaultValue().withDescription("The Databend password.");

    public static final ConfigOption<String> DATABASE_NAME = ConfigOptions.key(DatabendConfig.DATABASE_NAME).stringType().defaultValue("default").withDescription("The Databend database name. Default to `default`.");

    public static final ConfigOption<String> DEFAULT_DATABASE = ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY).stringType().noDefaultValue().withDescription("The Databend default database name.");

    public static final ConfigOption<String> TABLE_NAME = ConfigOptions.key(DatabendConfig.TABLE_NAME).stringType().noDefaultValue().withDescription("The Databend table name.");


    public static final ConfigOption<Integer> SINK_BATCH_SIZE = ConfigOptions.key(DatabendConfig.SINK_BATCH_SIZE).intType().defaultValue(1000).withDescription("The max flush size, over this number of records, will flush data. The default value is 1000.");

    public static final ConfigOption<Duration> SINK_FLUSH_INTERVAL = ConfigOptions.key(DatabendConfig.SINK_FLUSH_INTERVAL).durationType().defaultValue(Duration.ofSeconds(1L)).withDescription("The flush interval mills, over this time, asynchronous threads will flush data. The default value is 1s.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES = ConfigOptions.key(DatabendConfig.SINK_MAX_RETRIES).intType().defaultValue(3).withDescription("The max retry times if writing records to database failed.");

    public static final ConfigOption<SinkUpdateStrategy> SINK_UPDATE_STRATEGY = ConfigOptions.key(DatabendConfig.SINK_UPDATE_STRATEGY).enumType(SinkUpdateStrategy.class).defaultValue(SinkUpdateStrategy.UPDATE).withDescription("Convert a record of type UPDATE_AFTER to update/insert statement or just discard it, available: update, insert, discard." + " Additional: `table.exec.sink.upsert-materialize`, `org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializer`");


    public static final ConfigOption<Boolean> SINK_IGNORE_DELETE = ConfigOptions.key(DatabendConfig.SINK_IGNORE_DELETE).booleanType().defaultValue(true).withDescription("Whether to ignore deletes. defaults to true.");

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;


    /**
     * Update conversion strategy for sink operator.
     */
    public enum SinkUpdateStrategy {
        UPDATE("update", "Convert UPDATE_AFTER records to update statement."), INSERT("insert", "Convert UPDATE_AFTER records to insert statement."), DISCARD("discard", "Discard UPDATE_AFTER records.");

        private final String value;
        private final String description;

        SinkUpdateStrategy(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        public String getDescription() {
            return description;
        }
    }
}
