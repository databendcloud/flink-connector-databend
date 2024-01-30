package org.apache.flink;

import org.apache.flink.connector.databend.DatabendDynamicTableSink;
import org.apache.flink.connector.databend.config.DatabendConfigOptions;
import org.apache.flink.connector.databend.internal.options.DatabendDmlOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDatabendDynamicTableSink {

    @Test
    public void testConstructor() {
        String[] primaryKeys = {"id"};
        String[] partitionKeys = {"dt"};
        DataType physicalRowDataType = DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.INT()),
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("age", DataTypes.INT()),
                DataTypes.FIELD("dt", DataTypes.STRING()));

        Properties connectionProperties = new Properties();
        connectionProperties.put("url", "databend://localhost:8000");
        connectionProperties.put("user", "databend");
        connectionProperties.put("password", "databend");

        DatabendDmlOptions databendDmlOptions = new DatabendDmlOptions(
                "databend://localhost:8000",
                "databend",
                "databend",
                "test_output_format",
                "test",
                connectionProperties,
                1,
                Duration.ofSeconds(100),
                3,
                DatabendConfigOptions.SinkUpdateStrategy.INSERT,
                new String[]{},
                true,
                1);

        DatabendDynamicTableSink sink = new DatabendDynamicTableSink(
                databendDmlOptions, connectionProperties, primaryKeys, partitionKeys, physicalRowDataType);

        assertEquals(databendDmlOptions, sink.getOptions());
        assertEquals(connectionProperties, sink.getConnectionProperties());
        assertArrayEquals(primaryKeys, sink.getPrimaryKeys());
        assertArrayEquals(partitionKeys, sink.getPartitionKeys());
        assertEquals(physicalRowDataType, sink.getPhysicalRowDataType());
    }

    @Test
    public void testGetChangelogMode() {
        String[] primaryKeys = {"id"};
        String[] partitionKeys = {"dt"};
        DataType physicalRowDataType = DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.INT()),
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("age", DataTypes.INT()),
                DataTypes.FIELD("dt", DataTypes.STRING()));

        Properties connectionProperties = new Properties();
        connectionProperties.put("url", "databend://localhost:8000");
        connectionProperties.put("user", "databend");
        connectionProperties.put("password", "databend");

        DatabendDmlOptions databendDmlOptions = new DatabendDmlOptions(
                "databend://localhost:8000",
                "databend",
                "databend",
                "test_output_format",
                "test",
                connectionProperties,
                1,
                Duration.ofSeconds(100),
                3,
                DatabendConfigOptions.SinkUpdateStrategy.INSERT,
                new String[]{},
                true,
                1);

        DatabendDynamicTableSink sink = new DatabendDynamicTableSink(
                databendDmlOptions, connectionProperties, primaryKeys, partitionKeys, physicalRowDataType);

        ChangelogMode expectedMode = ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
        ChangelogMode actualMode = sink.getChangelogMode(ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build());
        System.out.println(actualMode);

        assertEquals(expectedMode, actualMode);
    }
}
