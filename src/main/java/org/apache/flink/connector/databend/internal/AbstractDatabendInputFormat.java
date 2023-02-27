package org.apache.flink.connector.databend.internal;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.databend.internal.connection.DatabendConnectionProvider;
import org.apache.flink.connector.databend.internal.converter.DatabendRowConverter;
import org.apache.flink.connector.databend.internal.options.DatabendReadOptions;
import org.apache.flink.connector.databend.split.DatabendParametersProvider;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public abstract class AbstractDatabendInputFormat extends RichInputFormat<RowData, InputSplit> implements ResultTypeQueryable<RowData> {
    protected final String[] fieldNames;

    protected final TypeInformation<RowData> rowDataTypeInfo;

    protected final Object[][] parameterValues;

    protected final String parameterClause;

    protected final String filterClause;

    protected final long limit;

    protected AbstractDatabendInputFormat(String[] fieldNames, TypeInformation<RowData> rowDataTypeInfo, Object[][] parameterValues, String parameterClause, String filterClause, long limit) {
        this.fieldNames = fieldNames;
        this.rowDataTypeInfo = rowDataTypeInfo;
        this.parameterValues = parameterValues;
        this.parameterClause = parameterClause;
        this.filterClause = filterClause;
        this.limit = limit;
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
        return cachedStatistics;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return rowDataTypeInfo;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    protected InputSplit[] createGenericInputSplits(int splitNum) {
        GenericInputSplit[] ret = new GenericInputSplit[splitNum];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = new GenericInputSplit(i, ret.length);
        }
        return ret;
    }

    protected String getQuery(String table, String database) {
        String queryTemplate = DatabendStatementFactory.getSelectStatement(table, database, fieldNames);
        StringBuilder whereBuilder = new StringBuilder();
        if (filterClause != null) {
            if (filterClause.toLowerCase().contains(" or ")) {
                whereBuilder.append("(").append(filterClause).append(")");
            } else {
                whereBuilder.append(filterClause);
            }
        }

        if (parameterClause != null) {
            if (whereBuilder.length() > 0) {
                whereBuilder.append(" AND ");
            }
            whereBuilder.append(parameterClause);
        }

        String limitClause = "";
        if (limit >= 0) {
            limitClause = "LIMIT " + limit;
        }

        return whereBuilder.length() > 0 ? String.join(" ", queryTemplate, "WHERE", whereBuilder.toString(), limitClause) : String.join(" ", queryTemplate, limitClause);
    }

    /**
     * Builder.
     */
    public static class Builder {

        private DatabendReadOptions readOptions;

        private Properties connectionProperties;

        private int[] shardIds;

        private Map<Integer, String> shardMap;

        private Object[][] shardValues;

        private String[] fieldNames;

        private DataType[] fieldTypes;

        private TypeInformation<RowData> rowDataTypeInfo;

        private Object[][] parameterValues;

        private String parameterClause;

        private String filterClause;

        private long limit;

        public Builder withOptions(DatabendReadOptions readOptions) {
            this.readOptions = readOptions;
            return this;
        }

        public Builder withConnectionProperties(Properties connectionProperties) {
            this.connectionProperties = connectionProperties;
            return this;
        }

        public Builder withFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder withFieldTypes(DataType[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        public Builder withRowDataTypeInfo(TypeInformation<RowData> rowDataTypeInfo) {
            this.rowDataTypeInfo = rowDataTypeInfo;
            return this;
        }

        public Builder withFilterClause(String filterClause) {
            this.filterClause = filterClause;
            return this;
        }

        public Builder withLimit(long limit) {
            this.limit = limit;
            return this;
        }

        public AbstractDatabendInputFormat build() {
            Preconditions.checkNotNull(readOptions);
            Preconditions.checkNotNull(connectionProperties);
            Preconditions.checkNotNull(fieldNames);
            Preconditions.checkNotNull(fieldTypes);
            Preconditions.checkNotNull(rowDataTypeInfo);

            DatabendConnectionProvider connectionProvider = null;
            try {
                connectionProvider = new DatabendConnectionProvider(readOptions, connectionProperties);

                LogicalType[] logicalTypes = Arrays.stream(fieldTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new);
                return createBatchInputFormat(logicalTypes);
            } catch (Exception e) {
                throw new RuntimeException("Build Databend input format failed.", e);
            } finally {
                if (connectionProvider != null) {
                    connectionProvider.closeConnections();
                }
            }
        }


        private AbstractDatabendInputFormat createBatchInputFormat(LogicalType[] logicalTypes) {
            return new DatabendBatchInputFormat(new DatabendConnectionProvider(readOptions, connectionProperties), new DatabendRowConverter(RowType.of(logicalTypes)), readOptions, fieldNames, rowDataTypeInfo, parameterValues, parameterClause, filterClause, limit);
        }
    }
}
