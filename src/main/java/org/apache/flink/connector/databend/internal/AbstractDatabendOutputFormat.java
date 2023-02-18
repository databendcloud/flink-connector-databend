package org.apache.flink.connector.databend.internal;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.databend.internal.connection.DatabendConnectionProvider;
import org.apache.flink.connector.databend.internal.executor.DatabendExecutor;
import org.apache.flink.connector.databend.internal.options.DatabendDmlOptions;
import org.apache.flink.connector.databend.internal.schema.Expression;
import org.apache.flink.connector.databend.internal.schema.FieldExpr;
import org.apache.flink.connector.databend.internal.schema.FunctionExpr;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.Data;
import java.sql.Connection;
import java.io.Flushable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

/**
 * Abstract class of Databend output format.
 */
public abstract class AbstractDatabendOutputFormat extends RichOutputFormat<RowData>
        implements Flushable {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractDatabendOutputFormat.class);

    protected transient volatile boolean closed = false;

    protected transient ScheduledExecutorService scheduler;

    protected transient ScheduledFuture<?> scheduledFuture;

    protected transient volatile Exception flushException;

    public AbstractDatabendOutputFormat() {
    }

    @Override
    public void configure(Configuration parameters) {
    }

    public void scheduledFlush(long intervalMillis, String executorName) {
        Preconditions.checkArgument(intervalMillis > 0, "flush interval must be greater than 0");
        scheduler = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory(executorName));
        scheduledFuture =
                scheduler.scheduleWithFixedDelay(
                        () -> {
                            synchronized (this) {
                                if (!closed) {
                                    try {
                                        flush();
                                    } catch (Exception e) {
                                        flushException = e;
                                    }
                                }
                            }
                        },
                        intervalMillis,
                        intervalMillis,
                        TimeUnit.MILLISECONDS);
    }

    public void checkBeforeFlush(final DatabendExecutor executor) throws IOException {
        checkFlushException();
        try {
            executor.executeBatch();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;

            try {
                flush();
            } catch (Exception exception) {
                LOG.warn("Flushing records to Databend failed.", exception);
            }

            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }

            closeOutputFormat();
            checkFlushException();
        }
    }

    protected void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Flush exception found.", flushException);
        }
    }

//    protected abstract void closeOutputFormat();

    /**
     * Builder for {@link DatabendBatchOutputFormat}.
     */
    protected abstract void closeOutputFormat();
    public static class Builder {

        private static final Logger LOG =
                LoggerFactory.getLogger(AbstractDatabendOutputFormat.Builder.class);

        private DataType[] fieldTypes;

        private LogicalType[] logicalTypes;

        private DatabendDmlOptions options;

        private Properties connectionProperties;

        private String[] fieldNames;

        private String[] primaryKeys;

        private String[] partitionKeys;

        public Builder() {
        }

        public AbstractDatabendOutputFormat.Builder withOptions(DatabendDmlOptions options) {
            this.options = options;
            return this;
        }

        public Builder withConnectionProperties(Properties connectionProperties) {
            this.connectionProperties = connectionProperties;
            return this;
        }

        public AbstractDatabendOutputFormat.Builder withFieldTypes(DataType[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            this.logicalTypes =
                    Arrays.stream(fieldTypes)
                            .map(DataType::getLogicalType)
                            .toArray(LogicalType[]::new);
            return this;
        }

        public AbstractDatabendOutputFormat.Builder withFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public AbstractDatabendOutputFormat.Builder withPrimaryKey(String[] primaryKeys) {
            this.primaryKeys = primaryKeys;
            return this;
        }

        public AbstractDatabendOutputFormat.Builder withPartitionKey(String[] partitionKeys) {
            this.partitionKeys = partitionKeys;
            return this;
        }


        public AbstractDatabendOutputFormat build() {
            Preconditions.checkNotNull(options);
            Preconditions.checkNotNull(fieldNames);
            Preconditions.checkNotNull(fieldTypes);
            Preconditions.checkNotNull(primaryKeys);
            Preconditions.checkNotNull(partitionKeys);
            if (primaryKeys.length > 0) {
                LOG.warn("If primary key is specified, connector will be in UPSERT mode.");
                LOG.warn(
                        "The data will be updated / deleted by the primary key, you will have significant performance loss.");
            }

            DatabendConnectionProvider connectionProvider = null;
            try {
                connectionProvider = new DatabendConnectionProvider(options);
                return createBatchOutputFormat();
            } catch (Exception exception) {
                throw new RuntimeException("Build Databend output format failed.", exception);
            } finally {
                if (connectionProvider != null) {
                    connectionProvider.closeConnections();
                }
            }
        }

        private DatabendBatchOutputFormat createBatchOutputFormat() {
            return new DatabendBatchOutputFormat(
                    new DatabendConnectionProvider(options, connectionProperties),
                    fieldNames,
                    primaryKeys,
                    partitionKeys,
                    logicalTypes,
                    options);
        }

        private List<FieldGetter> parseFieldGetters(FunctionExpr functionExpr) {
            return functionExpr.getArguments().stream()
                    .map(
                            expression -> parseFieldGetters((FunctionExpr) expression))
                    .flatMap(Collection::stream)
                    .collect(toList());
        }
    }
}

