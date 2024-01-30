package org.apache.flink.connector.databend.internal.options;

import javax.annotation.Nullable;
import java.util.Properties;

public class DatabendReadOptions extends DatabendConnectionOptions {
    private static final long serialVersionUID = 1L;

    private DatabendReadOptions(
            String url,
            @Nullable String username,
            @Nullable String password,
            String databaseName,
            String tableName,
            Properties connectionProperties) {
        super(url, username, password, databaseName, tableName, connectionProperties);
    }

    /**
     * Builder for {@link DatabendReadOptions}.
     */
    public static class Builder {
        private String url;
        private String username;
        private String password;
        private String databaseName;
        private String tableName;
        private Properties connectionProperties;

        public DatabendReadOptions.Builder withUrl(String url) {
            this.url = url;
            return this;
        }

        public DatabendReadOptions.Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        public DatabendReadOptions.Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public DatabendReadOptions.Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public DatabendReadOptions.Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public DatabendReadOptions.Builder withConnectionProperties(Properties connectionProperties) {
            this.connectionProperties = connectionProperties;
            return this;
        }

        public DatabendReadOptions build() {
            return new DatabendReadOptions(
                    url,
                    username,
                    password,
                    databaseName,
                    tableName,
                    connectionProperties);
        }
    }
}
