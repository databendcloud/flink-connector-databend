package org.apache.flink.connector.databend.internal.executor;

import com.databend.jdbc.DatabendPreparedStatement;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.databend.config.DatabendConfigOptions.SinkUpdateStrategy;
import org.apache.flink.connector.databend.internal.connection.DatabendConnectionProvider;
import org.apache.flink.connector.databend.internal.converter.DatabendRowConverter;
import org.apache.flink.connector.databend.internal.options.DatabendDmlOptions;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Function;

import static org.apache.flink.connector.databend.config.DatabendConfigOptions.SinkUpdateStrategy.*;

/**
 * Databend's upsert executor.
 */
public class DatabendUpsertExecutor implements DatabendExecutor {
    private static final long serialVersionUID = 1L;

    Logger LOG = LoggerFactory.getLogger(DatabendExecutor.class);

    private final String insertSql;

    private final String updateSql;

    private final String deleteSql;

    private final DatabendRowConverter insertConverter;

    private final DatabendRowConverter updateConverter;

    private final DatabendRowConverter deleteConverter;

    private final Function<RowData, RowData> updateExtractor;

    private final Function<RowData, RowData> deleteExtractor;

    private final int maxRetries;

    private final SinkUpdateStrategy updateStrategy;

    private final boolean ignoreDelete;

    private transient DatabendPreparedStatement insertStmt;

    private transient DatabendPreparedStatement updateStmt;

    private transient DatabendPreparedStatement deleteStmt;

    private transient DatabendConnectionProvider connectionProvider;

    public DatabendUpsertExecutor(
            String insertSql,
            String updateSql,
            String deleteSql,
            DatabendRowConverter insertConverter,
            DatabendRowConverter updateConverter,
            DatabendRowConverter deleteConverter,
            Function<RowData, RowData> updateExtractor,
            Function<RowData, RowData> deleteExtractor,
            DatabendDmlOptions options) {
        this.insertSql = insertSql;
        this.updateSql = updateSql;
        this.deleteSql = deleteSql;
        this.insertConverter = insertConverter;
        this.updateConverter = updateConverter;
        this.deleteConverter = deleteConverter;
        this.updateExtractor = updateExtractor;
        this.deleteExtractor = deleteExtractor;
        this.maxRetries = options.getMaxRetries();
        this.updateStrategy = options.getUpdateStrategy();
        this.ignoreDelete = options.isIgnoreDelete();
    }

    @Override
    public void prepareStatement(Connection connection) throws SQLException {
        this.insertStmt = (DatabendPreparedStatement) connection.prepareStatement(this.insertSql);
        this.updateStmt = (DatabendPreparedStatement) connection.prepareStatement(this.updateSql);
        this.deleteStmt = (DatabendPreparedStatement) connection.prepareStatement(this.deleteSql);
    }

    @Override
    public void prepareStatement(DatabendConnectionProvider connectionProvider) throws SQLException {
        this.connectionProvider = connectionProvider;
        prepareStatement(connectionProvider.getOrCreateConnection());
    }

    @Override
    public void setRuntimeContext(RuntimeContext context) {}

    @Override
    public void addToBatch(RowData record) throws SQLException {
        switch (record.getRowKind()) {
            case INSERT:
                insertConverter.toExternal(record, insertStmt);
                insertStmt.addBatch();
                break;
            case UPDATE_AFTER:
                // config different update strategy according to dml config options
                if (INSERT.equals(updateStrategy)) {
                    insertConverter.toExternal(record, insertStmt);
                    insertStmt.addBatch();
                } else if (UPDATE.equals(updateStrategy)) {
                    updateConverter.toExternal(updateExtractor.apply(record), updateStmt);
                    updateStmt.addBatch();
                } else if (DISCARD.equals(updateStrategy)) {
                    LOG.debug("Discard a record of type UPDATE_AFTER: {}", record);
                } else {
                    throw new RuntimeException("Unknown update strategy: " + updateStrategy);
                }
                break;
            case DELETE:
                if (!ignoreDelete) {
                    deleteConverter.toExternal(deleteExtractor.apply(record), deleteStmt);
                    deleteStmt.addBatch();
                }
                break;
            case UPDATE_BEFORE:
                break;
            default:
                throw new UnsupportedOperationException(String.format(
                        "Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE, but get: %s.",
                        record.getRowKind()));
        }
    }

    @Override
    public void executeBatch() throws SQLException {
        for (DatabendPreparedStatement databendPreparedStatement : Arrays.asList(insertStmt, updateStmt, deleteStmt)) {
            if (databendPreparedStatement != null) {
                attemptExecuteBatch(databendPreparedStatement, maxRetries);
            }
        }
    }

    @Override
    public void closeStatement() {
        for (DatabendPreparedStatement databendPreparedStatement : Arrays.asList(insertStmt, updateStmt, deleteStmt)) {
            if (databendPreparedStatement != null) {
                try {
                    databendPreparedStatement.close();
                } catch (SQLException exception) {
                    LOG.warn("Databend upsert statement could not be closed.", exception);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "DatabendUpsertExecutor{" + "insertSql='" + insertSql + '\'' + ", updateSql='" + updateSql + '\''
                + ", deleteSql='" + deleteSql + '\'' + ", maxRetries=" + maxRetries + ", updateStrategy="
                + updateStrategy + ", ignoreDelete=" + ignoreDelete + ", connectionProvider=" + connectionProvider
                + '}';
    }
}
