package org.apache.flink.connector.databend.internal.options;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Optional;
import java.util.Properties;

/**
 * Databend connection options.
 */
public class DatabendConnectionOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String url;

    private final String username;

    private final String password;

    private final String databaseName;

    private final String tableName;

    private final Properties connectionProperties;

    public DatabendConnectionOptions(
            String url, @Nullable String username, @Nullable String password, String databaseName, String tableName, Properties connectionProperties) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.connectionProperties = connectionProperties;
    }

    public String getUrl() {
        return this.url;
    }

    public Optional<String> getUsername() {
        return Optional.ofNullable(this.username);
    }

    public Optional<String> getPassword() {
        return Optional.ofNullable(this.password);
    }

    public String getDatabaseName() {
        return this.databaseName;
    }

    public String getTableName() {
        return this.tableName;
    }

    public Properties getConnectionProperties() {
        return this.connectionProperties;
    }
}
