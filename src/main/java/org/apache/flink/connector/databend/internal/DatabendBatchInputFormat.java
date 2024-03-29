package org.apache.flink.connector.databend.internal;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.databend.internal.connection.DatabendConnectionProvider;
import org.apache.flink.connector.databend.internal.converter.DatabendRowConverter;
import org.apache.flink.connector.databend.internal.options.DatabendReadOptions;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabendBatchInputFormat extends AbstractDatabendInputFormat {
    private static final Logger LOG = LoggerFactory.getLogger(DatabendBatchOutputFormat.class);

    private final DatabendConnectionProvider connectionProvider;

    private final DatabendRowConverter rowConverter;

    private final DatabendReadOptions readOptions;

    private transient PreparedStatement statement;
    private transient ResultSet resultSet;
    private transient boolean hasNext;

    public DatabendBatchInputFormat(
            DatabendConnectionProvider connectionProvider,
            DatabendRowConverter rowConverter,
            DatabendReadOptions readOptions,
            String[] fieldNames,
            TypeInformation<RowData> rowDataTypeInfo,
            Object[][] parameterValues,
            String parameterClause,
            String filterClause,
            long limit) {
        super(fieldNames, rowDataTypeInfo, parameterValues, parameterClause, filterClause, limit);
        this.connectionProvider = connectionProvider;
        this.rowConverter = rowConverter;
        this.readOptions = readOptions;
    }

    @Override
    public void openInputFormat() {
        try {
            Connection connection = connectionProvider.getOrCreateConnection();
            String query = getQuery(readOptions.getTableName(), readOptions.getDatabaseName());
            statement = connection.prepareStatement(query);
        } catch (SQLException se) {
            throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
        }
    }

    @Override
    public void closeInputFormat() {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException exception) {
            LOG.info("InputFormat Statement couldn't be closed.", exception);
        } finally {
            statement = null;
        }

        if (connectionProvider != null) {
            connectionProvider.closeConnections();
        }
    }

    @Override
    public void open(InputSplit split) {
        try {
            if (split != null && parameterValues != null) {
                for (int i = 0; i < parameterValues[split.getSplitNumber()].length; i++) {
                    Object param = parameterValues[split.getSplitNumber()][i];
                    statement.setObject(i + 1, param);
                }
            }

            resultSet = statement.executeQuery();
            hasNext = resultSet.next();
        } catch (SQLException se) {
            throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
        }
    }

    @Override
    public void close() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
        } catch (SQLException se) {
            LOG.info("InputFormat ResultSet couldn't be closed.", se);
        }
    }

    @Override
    public boolean reachedEnd() {
        return !hasNext;
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        if (!hasNext) {
            return null;
        }

        try {
            RowData row = rowConverter.toInternal(resultSet);
            // update hasNext after we've read the record
            hasNext = resultSet.next();
            return row;
        } catch (Exception exception) {
            throw new IOException("Couldn't read data from resultSet.", exception);
        }
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) {
        int splitNum = parameterValues != null ? parameterValues.length : 1;
        return createGenericInputSplits(splitNum);
    }
}
