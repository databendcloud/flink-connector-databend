package org.apache.flink.connector.databend.internal.connection;

// import com.databend.jdbc.ConnectionProperties;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.flink.connector.databend.internal.options.DatabendConnectionOptions;
import org.apache.flink.connector.databend.util.DatabendUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Databend connection provider. Use DatabendDriver to create a connection.
 */
public class DatabendConnectionProvider implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DatabendConnectionProvider.class);
    private final DatabendConnectionOptions options;
    private final Properties connectionProperties;
    private transient Connection connection;

    public DatabendConnectionProvider(DatabendConnectionOptions options) {
        this(options, new Properties());
    }

    public DatabendConnectionProvider(DatabendConnectionOptions options, Properties connectionProperties) {
        this.options = options;
        this.connectionProperties = connectionProperties;
    }

    public synchronized Connection getOrCreateConnection() throws SQLException {
        if (connection == null) {
            connection = createConnection(options.getUrl(), options.getDatabaseName());
        }
        return connection;
    }

    private Connection createConnection(String url, String database) throws SQLException {
        LOG.info("connecting to {}, database {}", url, database);
        String jdbcUrl = DatabendUtil.getJdbcUrl(url, database);
        return DriverManager.getConnection(
                jdbcUrl,
                options.getUsername().orElse(null),
                options.getPassword().orElse(null));
    }

    public void closeConnections() {
        if (this.connection != null) {
            try {
                connection.close();
            } catch (SQLException exception) {
                LOG.warn("Databend connection could not be closed.", exception);
            } finally {
                connection = null;
            }
        }
    }
}
