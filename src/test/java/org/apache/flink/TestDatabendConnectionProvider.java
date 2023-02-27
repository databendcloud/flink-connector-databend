package org.apache.flink;

import org.apache.flink.connector.databend.internal.connection.DatabendConnectionProvider;
import org.apache.flink.connector.databend.internal.options.DatabendConnectionOptions;
import org.apache.flink.connector.databend.util.DatabendUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Properties;

public class TestDatabendConnectionProvider {
    @Test
    public void TestCreateConnection() throws SQLException {
        HashMap<String, String> m = new HashMap<>();
        m.put("properties.url", "databend://localhost:8002");
        m.put("properties.username", "root");
        m.put("properties.password", "root");
        m.put("properties.database-name", "default");
        m.put("properties.table-name", "test");
        Properties properties = DatabendUtil.getDatabendProperties(m);
        DatabendConnectionOptions databendConnectionOptions = new DatabendConnectionOptions(
                "databend://localhost:8002",
                "root",
                "root",
                "default",
                "test"
        );

        DatabendConnectionProvider databendConnectionProvider = new DatabendConnectionProvider(
                databendConnectionOptions, properties
        );
        Connection connection = databendConnectionProvider.getOrCreateConnection();
        Statement stmt = connection.createStatement();
        Boolean r = stmt.execute("select 1");
        assertEquals(true, r);
    }
}
