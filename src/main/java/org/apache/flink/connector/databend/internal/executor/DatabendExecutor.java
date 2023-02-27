package org.apache.flink.connector.databend.internal.executor;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.databend.internal.DatabendStatementFactory;
import org.apache.flink.connector.databend.internal.connection.DatabendConnectionProvider;
import org.apache.flink.connector.databend.internal.converter.DatabendRowConverter;
import org.apache.flink.connector.databend.internal.options.DatabendDmlOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.databend.jdbc.DatabendPreparedStatement;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.sql.Connection;
import java.sql.DriverManager;

import static org.apache.flink.table.data.RowData.createFieldGetter;

/**
 * Executor interface for submitting data to Databend.
 */
public interface DatabendExecutor extends Serializable {
    Logger LOG = LoggerFactory.getLogger(DatabendExecutor.class);

    void prepareStatement(Connection connection) throws SQLException;

    void prepareStatement(DatabendConnectionProvider connectionProvider) throws SQLException;

    void setRuntimeContext(RuntimeContext context);

    void addToBatch(RowData rowData) throws SQLException;

    void executeBatch() throws SQLException;

    void closeStatement();

    default void attemptExecuteBatch(DatabendPreparedStatement stmt, int maxRetries) throws SQLException {
        for (int i = 0; i <= maxRetries; i++) {
            try {
                stmt.executeBatch();
                return;
            } catch (Exception exception) {
                LOG.error("Databend executeBatch error, retry times = {}", i, exception);
                if (i >= maxRetries) {
                    throw new SQLException(String.format("Attempt to execute batch failed, exhausted retry times = %d", maxRetries), exception);
                }
                try {
                    Thread.sleep(1000L * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Unable to flush; interrupted while doing another attempt", ex);
                }
            }
        }
    }

    static DatabendExecutor createDatabendExecutor(String tableName, String databaseName, String[] fieldNames, String[] keyFields, String[] partitionFields, LogicalType[] fieldTypes, DatabendDmlOptions options) {
        if (keyFields.length > 0) {
            return createUpsertExecutor(tableName, databaseName, fieldNames, keyFields, partitionFields, fieldTypes, options);
        } else {
            return createBatchExecutor(tableName, fieldNames, fieldTypes, options);
        }
    }

    static DatabendBatchExecutor createBatchExecutor(String tableName, String[] fieldNames, LogicalType[] fieldTypes, DatabendDmlOptions options) {
        String insertSql = DatabendStatementFactory.getInsertIntoStatement(tableName, fieldNames);
        DatabendRowConverter converter = new DatabendRowConverter(RowType.of(fieldTypes));
        return new DatabendBatchExecutor(insertSql, converter, options);
    }

    static DatabendUpsertExecutor createUpsertExecutor(String tableName, String databaseName, String[] fieldNames, String[] keyFields, String[] partitionFields, LogicalType[] fieldTypes, DatabendDmlOptions options) {
        String insertSql = DatabendStatementFactory.getInsertIntoStatement(tableName, fieldNames);
        String updateSql = DatabendStatementFactory.getUpdateStatement(tableName, databaseName, fieldNames, keyFields, partitionFields);
        String deleteSql = DatabendStatementFactory.getDeleteStatement(tableName, databaseName, keyFields);

        // Re-sort the order of fields to fit the sql statement.
        int[] delFields = Arrays.stream(keyFields).mapToInt(pk -> ArrayUtils.indexOf(fieldNames, pk)).toArray();
        int[] updatableFields = IntStream.range(0, fieldNames.length).filter(idx -> !ArrayUtils.contains(keyFields, fieldNames[idx])).filter(idx -> !ArrayUtils.contains(partitionFields, fieldNames[idx])).toArray();
        int[] updFields = ArrayUtils.addAll(updatableFields, delFields);

        LogicalType[] delTypes = Arrays.stream(delFields).mapToObj(f -> fieldTypes[f]).toArray(LogicalType[]::new);
        LogicalType[] updTypes = Arrays.stream(updFields).mapToObj(f -> fieldTypes[f]).toArray(LogicalType[]::new);

        return new DatabendUpsertExecutor(insertSql, updateSql, deleteSql, new DatabendRowConverter(RowType.of(fieldTypes)), new DatabendRowConverter(RowType.of(updTypes)), new DatabendRowConverter(RowType.of(delTypes)), createExtractor(fieldTypes, updFields), createExtractor(fieldTypes, delFields), options);
    }

    static Function<RowData, RowData> createExtractor(LogicalType[] logicalTypes, int[] fields) {
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fields.length];
        for (int i = 0; i < fields.length; i++) {
            fieldGetters[i] = createFieldGetter(logicalTypes[fields[i]], fields[i]);
        }

        return row -> {
            GenericRowData rowData = new GenericRowData(row.getRowKind(), fieldGetters.length);
            for (int i = 0; i < fieldGetters.length; i++) {
                rowData.setField(i, fieldGetters[i].getFieldOrNull(row));
            }
            return rowData;
        };
    }
}
