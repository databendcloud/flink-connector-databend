package org.apache.flink;

import com.databend.jdbc.DatabendColumnInfo;
import org.apache.flink.connector.databend.util.DatabendUtil;
import org.junit.jupiter.api.Test;

import javax.xml.crypto.Data;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

import static org.apache.flink.table.utils.DateTimeUtils.toLocalDate;
import static org.junit.jupiter.api.Assertions.*;

public class TestDatabendUtil {

    @Test
    public void TestDatabendUtil() {

        // getJdbcUrl
        String jdbcUrl = "jdbc:databend://localhost:8000/testDb";
        String url = "databend://localhost:8000";
        String databaseName = "testDb";
        String afterJdbc = DatabendUtil.getJdbcUrl(url, databaseName);
        assertEquals(jdbcUrl, afterJdbc);

        // toEpochDayOneTimestamp
        LocalTime localTime = LocalTime.ofNanoOfDay((toLocalDate(1000).toEpochDay()) * 1_000_000L);
        Timestamp epochDayOneTime = DatabendUtil.toEpochDayOneTimestamp(localTime);
        String epochStr = epochDayOneTime.toString();
        assertEquals("1970-01-02 00:00:01.0", epochStr);

        // getDatabendProperties
        HashMap<String, String> m = new HashMap<>();
        m.put("properties.url", "databend://localhost:8000");
        m.put("properties.username", "root");
        m.put("properties.password", "root");
        m.put("properties.database-name", "default");
        m.put("properties.table-name", "test");
        Properties properties = DatabendUtil.getDatabendProperties(m);
        assertSame(properties.get("url"), "databend://localhost:8000");
        assertSame(properties.get("username"), "root");
    }
}

