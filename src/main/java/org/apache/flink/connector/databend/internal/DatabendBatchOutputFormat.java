package org.apache.flink.connector.databend.internal;

import java.io.IOException;
import java.sql.SQLException;
import javax.annotation.Nonnull;
import org.apache.flink.connector.databend.internal.connection.DatabendConnectionProvider;
import org.apache.flink.connector.databend.internal.executor.DatabendExecutor;
import org.apache.flink.connector.databend.internal.options.DatabendDmlOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

public class DatabendBatchOutputFormat extends AbstractDatabendOutputFormat {
    private static final long serialVersionUID = 1L;

    private final DatabendConnectionProvider connectionProvider;

    private final String[] fieldNames;

    private final String[] keyFields;

    private final String[] partitionFields;

    private final LogicalType[] fieldTypes;

    private final DatabendDmlOptions options;

    private transient DatabendExecutor executor;

    private transient int batchCount = 0;

    public DatabendBatchOutputFormat(
            @Nonnull DatabendConnectionProvider connectionProvider,
            @Nonnull String[] fieldNames,
            @Nonnull String[] keyFields,
            @Nonnull String[] partitionFields,
            @Nonnull LogicalType[] fieldTypes,
            @Nonnull DatabendDmlOptions options) {
        this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
        this.fieldNames = Preconditions.checkNotNull(fieldNames);
        this.keyFields = Preconditions.checkNotNull(keyFields);
        this.partitionFields = Preconditions.checkNotNull(partitionFields);
        this.fieldTypes = Preconditions.checkNotNull(fieldTypes);
        this.options = Preconditions.checkNotNull(options);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            executor = DatabendExecutor.createDatabendExecutor(
                    options.getTableName(),
                    options.getDatabaseName(),
                    fieldNames,
                    keyFields,
                    partitionFields,
                    fieldTypes,
                    options);
            executor.prepareStatement(connectionProvider);
            //            executor.setRuntimeContext(getRuntimeContext());

            long flushIntervalMillis = options.getFlushInterval().toMillis();
            scheduledFlush(flushIntervalMillis, "databend-batch-output-format");
        } catch (Exception exception) {
            throw new IOException("Unable to establish connection with databend.", exception);
        }
    }

    @Override
    public synchronized void writeRecord(RowData record) throws IOException {
        checkFlushException();

        try {
            executor.addToBatch(record);
            System.out.println(record);
            batchCount++;
            if (batchCount >= options.getBatchSize()) {
                flush();
            }
        } catch (SQLException exception) {
            throw new IOException("Writing record to databend statement failed.", exception);
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        if (batchCount > 0) {
            checkBeforeFlush(executor);
            batchCount = 0;
        }
    }

    @Override
    public synchronized void closeOutputFormat() {
        executor.closeStatement();
        connectionProvider.closeConnections();
    }
}
