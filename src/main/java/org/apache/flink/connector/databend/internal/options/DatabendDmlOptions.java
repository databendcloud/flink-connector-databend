package org.apache.flink.connector.databend.internal.options;

import org.apache.flink.connector.databend.config.DatabendConfigOptions.SinkUpdateStrategy;

import javax.annotation.Nullable;
import java.time.Duration;


/**
 * Databend data modify language options.
 */
public class DatabendDmlOptions extends DatabendConnectionOptions {

    private static final long serialVersionUID = 1L;

    private final int batchSize;

    private final Duration flushInterval;

    private final int maxRetries;

    private final SinkUpdateStrategy updateStrategy;


    private final boolean ignoreDelete;

    private final Integer parallelism;

    public DatabendDmlOptions(String url, @Nullable String username, @Nullable String password, String databaseName, String tableName, int batchSize, Duration flushInterval, int maxRetires, SinkUpdateStrategy updateStrategy, boolean ignoreDelete, Integer parallelism) {
        super(url, username, password, databaseName, tableName);
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.maxRetries = maxRetires;
        this.updateStrategy = updateStrategy;
        this.ignoreDelete = ignoreDelete;
        this.parallelism = parallelism;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    public Duration getFlushInterval() {
        return this.flushInterval;
    }

    public int getMaxRetries() {
        return this.maxRetries;
    }


    public SinkUpdateStrategy getUpdateStrategy() {
        return updateStrategy;
    }


    public boolean isIgnoreDelete() {
        return this.ignoreDelete;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    /**
     * Builder for {@link DatabendDmlOptions}.
     */
    public static class Builder {
        private String url;
        private String username;
        private String password;
        private String databaseName;
        private String tableName;
        private int batchSize;
        private Duration flushInterval;
        private int maxRetries;
        private SinkUpdateStrategy updateStrategy;
        private boolean ignoreDelete;
        private Integer parallelism;

        public Builder() {
        }

        public DatabendDmlOptions.Builder withUrl(String url) {
            this.url = url;
            return this;
        }

        public DatabendDmlOptions.Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        public DatabendDmlOptions.Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public DatabendDmlOptions.Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public DatabendDmlOptions.Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public DatabendDmlOptions.Builder withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public DatabendDmlOptions.Builder withFlushInterval(Duration flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        public DatabendDmlOptions.Builder withMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public DatabendDmlOptions.Builder withUpdateStrategy(SinkUpdateStrategy updateStrategy) {
            this.updateStrategy = updateStrategy;
            return this;
        }


        public DatabendDmlOptions.Builder withIgnoreDelete(boolean ignoreDelete) {
            this.ignoreDelete = ignoreDelete;
            return this;
        }

        public DatabendDmlOptions.Builder withParallelism(Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public DatabendDmlOptions build() {
            return new DatabendDmlOptions(url, username, password, databaseName, tableName, batchSize, flushInterval, maxRetries, updateStrategy, ignoreDelete, parallelism);
        }
    }
}
