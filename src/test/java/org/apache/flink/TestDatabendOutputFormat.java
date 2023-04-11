package org.apache.flink;

import org.apache.flink.connector.databend.config.DatabendConfigOptions;
import org.apache.flink.connector.databend.internal.AbstractDatabendOutputFormat;
import org.apache.flink.connector.databend.internal.DatabendBatchOutputFormat;
import org.apache.flink.connector.databend.internal.connection.DatabendConnectionProvider;
import org.apache.flink.connector.databend.internal.options.DatabendConnectionOptions;
import org.apache.flink.connector.databend.internal.options.DatabendDmlOptions;
import org.apache.flink.connector.databend.util.DatabendUtil;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestDatabendOutputFormat {

    // Create a list of DataType objects to mock
    DataType[] dataTypeList = {
            DataTypes.STRING(),
            DataTypes.STRING()
    };

    LogicalType[] logicalTypeList = Arrays.stream(dataTypeList).map(DataType::getLogicalType).toArray(LogicalType[]::new);

    private static Connection createConnection()
            throws SQLException {
        String url = "jdbc:databend://localhost:8000";
        return DriverManager.getConnection(url, "root", "root");
    }

    @BeforeAll
    public static void setUp()
            throws SQLException {
        // create table
        Connection c = createConnection();
        c.createStatement().execute("drop database if exists test_output_format");
        c.createStatement().execute("create database test_output_format");
        c.createStatement().execute("create table test_output_format.test(x int,y varchar)");
    }

    @AfterAll
    public static void tearDown() throws SQLException {
        Connection c = createConnection();
        c.createStatement().execute("drop database if exists test_output_format");
    }

    @Test
    public void TestAbstractDatabendOutput() throws SQLException, IOException {
        MockitoAnnotations.initMocks(this);
        HashMap<String, String> m = new HashMap<>();
        m.put("properties.url", "databend://localhost:8000");
        m.put("properties.username", "root");
        m.put("properties.password", "root");
        m.put("properties.database-name", "test_output_format");
        m.put("properties.table-name", "test");
        Properties properties = DatabendUtil.getDatabendProperties(m);
        DatabendConnectionOptions databendConnectionOptions = new DatabendConnectionOptions(
                "databend://localhost:8000",
                "root",
                "root",
                "test_output_format",
                "test"
        );

        DatabendDmlOptions databendDmlOptions = new DatabendDmlOptions(
                "databend://localhost:8000",
                "root",
                "root",
                "test_output_format",
                "test",
                3,
                Duration.ofSeconds(100),
                3,
                DatabendConfigOptions.SinkUpdateStrategy.INSERT,
                true,
                1
        );

        DatabendConnectionProvider databendConnectionProvider = new DatabendConnectionProvider(
                databendConnectionOptions, properties
        );
        Connection connection = databendConnectionProvider.getOrCreateConnection();

        String[] fields = {"x", "y"};
        String[] primaryKeys = {};
        String[] partitionKeys = {"x"};


        AbstractDatabendOutputFormat abstractDatabendOutputFormat = new AbstractDatabendOutputFormat.Builder()
                .withOptions(databendDmlOptions)
                .withFieldTypes(dataTypeList)
                .withFieldNames(fields)
                .withConnectionProperties(properties)
                .withPartitionKey(fields)
                .withPrimaryKey(primaryKeys)
                .build();


        DatabendBatchOutputFormat databendBatchOutputFormat = new DatabendBatchOutputFormat(
                databendConnectionProvider,
                fields,
                primaryKeys,
                partitionKeys,
                logicalTypeList,
                databendDmlOptions
        );

        assertNotNull(databendBatchOutputFormat);
        databendBatchOutputFormat.open(1, 1);

        assertNotNull(abstractDatabendOutputFormat);

        // test writeRecord
        RowData record = GenericRowData.of(StringData.fromString("112"), StringData.fromString("test"));
        RowData record1 = GenericRowData.of(StringData.fromString("113"), StringData.fromString("test"));
        RowData record2 = GenericRowData.of(StringData.fromString("114"), StringData.fromString("test"));
        databendBatchOutputFormat.writeRecord(record);
        databendBatchOutputFormat.writeRecord(record1);
        databendBatchOutputFormat.writeRecord(record2);
        databendBatchOutputFormat.closeOutputFormat();
    }
}
