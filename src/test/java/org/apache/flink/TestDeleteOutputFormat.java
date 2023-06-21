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
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;

import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestDeleteOutputFormat {
    // Create a list of DataType objects to mock
    DataType[] dataTypeList = {DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()};

    LogicalType[] logicalTypeList = Arrays.stream(dataTypeList).map(DataType::getLogicalType).toArray(LogicalType[]::new);

    private static Connection createConnection() throws SQLException {
        String url = "jdbc:databend://localhost:8000";
        return DriverManager.getConnection(url, "databend", "databend");
    }

    @BeforeAll
    public static void setUp() throws SQLException {
        // create table
        Connection c = createConnection();
        c.createStatement().execute("drop database if exists test_delete_output");
        c.createStatement().execute("create database test_delete_output");
        c.createStatement().execute("create table test_delete_output.test(x int,y varchar,z varchar)");
    }

    @AfterAll
    public static void tearDown() throws SQLException {
        Connection c = createConnection();
        c.createStatement().execute("drop database if exists test_delete_output");
    }

    @Test
    public void TestAbstractDatabendOutput() throws SQLException, IOException {
        MockitoAnnotations.initMocks(this);
        HashMap<String, String> m = new HashMap<>();
        m.put("properties.url", "databend://0.0.0.0:8000");
        m.put("properties.username", "databend");
        m.put("properties.password", "databend");
        m.put("properties.database-name", "test_delete_output");
        m.put("properties.table-name", "test");
        Properties properties = DatabendUtil.getDatabendProperties(m);
        DatabendConnectionOptions databendConnectionOptions = new DatabendConnectionOptions("databend://0.0.0.0:8000", "databend", "databend", "test_delete_output", "test");

        DatabendDmlOptions databendDmlOptions = new DatabendDmlOptions("databend://0.0.0.0:8000", "databend", "databend", "test_delete_output", "test", 1, Duration.ofSeconds(100), 3, DatabendConfigOptions.SinkUpdateStrategy.UPDATE, new String[]{}, false, 1);
        String[] fields = {"x", "y", "z"};
        String[] primaryKeys = {"x"};
        String[] partitionKeys = {"x"};
        DatabendConnectionProvider databendConnectionProvider = new DatabendConnectionProvider(databendConnectionOptions, properties);
        Connection connection = databendConnectionProvider.getOrCreateConnection();


        AbstractDatabendOutputFormat abstractDatabendOutputFormat = new AbstractDatabendOutputFormat.Builder().withOptions(databendDmlOptions).withFieldTypes(dataTypeList).withFieldNames(fields).withConnectionProperties(properties).withPartitionKey(fields).withPrimaryKeys(primaryKeys).build();

        DatabendBatchOutputFormat databendBatchOutputFormat = new DatabendBatchOutputFormat(databendConnectionProvider, fields, primaryKeys, partitionKeys, logicalTypeList, databendDmlOptions);

        assertNotNull(databendBatchOutputFormat);
        databendBatchOutputFormat.open(1, 1);

        assertNotNull(abstractDatabendOutputFormat);

        // test writeRecord insert and delete
        RowData record = GenericRowData.of(112, StringData.fromString("test"), StringData.fromString("x"));
        RowData record1 = GenericRowData.of(113, StringData.fromString("test"), StringData.fromString("y"));
        RowData record2 = GenericRowData.of(112, StringData.fromString("test"), StringData.fromString("x"));
        record2.setRowKind(RowKind.DELETE);
        databendBatchOutputFormat.writeRecord(record);
        databendBatchOutputFormat.writeRecord(record1);
        databendBatchOutputFormat.writeRecord(record2);
        Statement statement = connection.createStatement();
        statement.execute("select * from test_delete_output.test");
        ResultSet r = statement.getResultSet();
        while (r.next()) {
            System.out.println(r.getString("x"));
            // after delete event we expect one row (113,test,y)
            Assert.assertEquals(r.getInt(1), 113);
        }

        databendBatchOutputFormat.closeOutputFormat();
    }
}
