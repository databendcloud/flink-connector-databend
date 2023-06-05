package org.apache.flink.connector.databend;

import org.apache.flink.connector.databend.internal.AbstractDatabendOutputFormat;
import org.apache.flink.connector.databend.internal.options.DatabendDmlOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Properties;

/**
 * A {@link DynamicTableSink} that describes how to create a {@link DatabendDynamicTableSink} from
 * a logical description.
 *
 * <p>TODO: Partitioning strategy isn't well implemented.
 */
public class DatabendDynamicTableSink implements DynamicTableSink {
    private final String[] primaryKeys;

    private final String[] partitionKeys;

    private final DataType physicalRowDataType;

    private final DatabendDmlOptions options;

    private final Properties connectionProperties;

    private boolean dynamicGrouping = false;

    private LinkedHashMap<String, String> staticPartitionSpec = new LinkedHashMap<>();

    public DataType getPhysicalRowDataType() {
        return physicalRowDataType;
    }

    public String[] getPartitionKeys() {
        return partitionKeys;
    }

    public String[] getPrimaryKeys() {
        return primaryKeys;
    }

    public Properties getConnectionProperties() {
        return connectionProperties;
    }

    public DatabendDmlOptions getOptions() {
        return options;
    }

    public DatabendDynamicTableSink(
            @Nonnull DatabendDmlOptions options,
            @Nonnull Properties connectionProperties,
            @Nonnull String[] primaryKeys,
            @Nonnull String[] partitionKeys,
            @Nonnull DataType physicalRowDataType) {
        this.options = options;
        this.connectionProperties = connectionProperties;
        this.primaryKeys = primaryKeys;
        this.partitionKeys = partitionKeys;
        this.physicalRowDataType = physicalRowDataType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        AbstractDatabendOutputFormat outputFormat = new AbstractDatabendOutputFormat.Builder()
                .withOptions(options)
                .withConnectionProperties(connectionProperties)
                .withFieldNames(DataType.getFieldNames(physicalRowDataType).toArray(new String[0]))
                .withFieldTypes(DataType.getFieldDataTypes(physicalRowDataType).toArray(new DataType[0]))
                .withPrimaryKeys(primaryKeys)
                .withPartitionKey(partitionKeys)
                .build();
        return OutputFormatProvider.of(outputFormat, options.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        DatabendDynamicTableSink sink = new DatabendDynamicTableSink(
                options, connectionProperties, primaryKeys, partitionKeys, physicalRowDataType);
        sink.dynamicGrouping = dynamicGrouping;
        sink.staticPartitionSpec = staticPartitionSpec;
        return sink;
    }

    @Override
    public String asSummaryString() {
        return "Databend table sink";
    }
}
