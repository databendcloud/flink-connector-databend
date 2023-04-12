package org.apache.flink.connector.databend;

import java.util.Properties;
import org.apache.flink.connector.databend.internal.AbstractDatabendInputFormat;
import org.apache.flink.connector.databend.internal.options.DatabendReadOptions;
// import org.apache.flink.connector.databend.util.FilterPushDownHelper;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.types.DataType;

public class DatabendDynamicTableSource implements ScanTableSource, SupportsProjectionPushDown, SupportsLimitPushDown {
    private final DatabendReadOptions readOptions;

    private final Properties connectionProperties;

    private DataType physicalRowDataType;

    private String filterClause;

    private long limit = -1L;

    public DatabendDynamicTableSource(
            DatabendReadOptions readOptions, Properties properties, DataType physicalRowDataType) {
        this.readOptions = readOptions;
        this.connectionProperties = properties;
        this.physicalRowDataType = physicalRowDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        AbstractDatabendInputFormat.Builder builder = new AbstractDatabendInputFormat.Builder()
                .withOptions(readOptions)
                .withConnectionProperties(connectionProperties)
                .withFieldNames(DataType.getFieldNames(physicalRowDataType).toArray(new String[0]))
                .withFieldTypes(DataType.getFieldDataTypes(physicalRowDataType).toArray(new DataType[0]))
                .withRowDataTypeInfo(runtimeProviderContext.createTypeInformation(physicalRowDataType))
                .withFilterClause(filterClause)
                .withLimit(limit);
        return InputFormatProvider.of(builder.build());
    }

    @Override
    public DynamicTableSource copy() {
        DatabendDynamicTableSource source =
                new DatabendDynamicTableSource(readOptions, connectionProperties, physicalRowDataType);
        source.filterClause = filterClause;
        source.limit = limit;
        return source;
    }

    @Override
    public String asSummaryString() {
        return "Databend table source";
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.physicalRowDataType = Projection.of(projectedFields).project(physicalRowDataType);
    }
}
