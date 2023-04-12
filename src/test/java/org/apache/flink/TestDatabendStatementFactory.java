package org.apache.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.flink.connector.databend.internal.DatabendStatementFactory;
import org.junit.jupiter.api.Test;

public class TestDatabendStatementFactory {
    @Test
    public void DatabendStatementFactory() {
        String[] fields = {"column1", "column2"};
        String selectStatement = DatabendStatementFactory.getSelectStatement("test", "default", fields);
        assertEquals("SELECT `column1`, `column2` FROM `default`.`test`", selectStatement);

        String insertStatement = DatabendStatementFactory.getInsertIntoStatement("test", fields);
        assertEquals("INSERT INTO `test`(`column1`, `column2`) VALUES (?, ?)", insertStatement);

        String[] keyFields = {"column1"};
        String[] partitionFields = {"column2"};

        // TODO: use upsert of databend
        String updateStatement =
                DatabendStatementFactory.getUpdateStatement("test", "default", fields, keyFields, partitionFields);
        System.out.println(updateStatement);
    }
}
