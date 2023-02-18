package org.apache.flink.connector.databend.internal.executor;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.databend.internal.connection.DatabendConnectionProvider;
import org.apache.flink.connector.databend.internal.converter.DatabendRowConverter;
import org.apache.flink.connector.databend.internal.options.DatabendDmlOptions;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.databend.jdbc.DatabendPreparedStatement;

import java.sql.Connection;
import java.sql.DriverManager;

import java.sql.SQLException;

public class DatabendBatchExecutor implements DatabendExecutor {
    private static final long serialVersionUID = 1L;

    Logger LOG = LoggerFactory.getLogger(DatabendExecutor.class);

    private final String insertSql;

    private final DatabendRowConverter converter;

    private final int maxRetries;

    private transient DatabendPreparedStatement statement;

    private transient DatabendConnectionProvider connectionProvider;

    public DatabendBatchExecutor(
            String insertSql, DatabendRowConverter converter, DatabendDmlOptions options) {
        this.insertSql = insertSql;
        this.converter = converter;
        this.maxRetries = options.getMaxRetries();
    }

    @Override
    public void prepareStatement(Connection connection) throws SQLException {
        statement = (DatabendPreparedStatement) connection.prepareStatement(insertSql);
    }

    @Override
    public void prepareStatement(DatabendConnectionProvider connectionProvider)
            throws SQLException {
        this.connectionProvider = connectionProvider;
        prepareStatement(connectionProvider.getOrCreateConnection());
    }

    @Override
    public void setRuntimeContext(RuntimeContext context) {
    }

    @Override
    public void addToBatch(RowData record) throws SQLException {
        switch (record.getRowKind()) {
            case INSERT:
                converter.toExternal(record, statement);
                statement.addBatch();
                break;
            case UPDATE_AFTER:
            case DELETE:
            case UPDATE_BEFORE:
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE, but get: %s.",
                                record.getRowKind()));
        }
    }

    @Override
    public void executeBatch() throws SQLException {
        attemptExecuteBatch(statement, maxRetries);
    }

    @Override
    public void closeStatement() {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException exception) {
                LOG.warn("Databend batch statement could not be closed.", exception);
            } finally {
                statement = null;
            }
        }
    }

    @Override
    public String toString() {
        return "DatabendBatchExecutor{"
                + "insertSql='"
                + insertSql
                + '\''
                + ", maxRetries="
                + maxRetries
                + ", connectionProvider="
                + connectionProvider
                + '}';
    }
}
