package org.apache.flink;

import org.apache.flink.connector.databend.internal.DatabendStatementFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDatabendStatementFactory {
    @Test
    public void DatabendStatementFactory() {
        String[] fields = {"column1", "column2"};
        String selectStatement = DatabendStatementFactory.getSelectStatement("test", "default", fields);
        assertEquals("SELECT `column1`, `column2` FROM `default`.`test`", selectStatement);

        String insertStatement = DatabendStatementFactory.getInsertIntoStatement("test", fields);
        assertEquals("INSERT INTO `test`(`column1`, `column2`) VALUES (?, ?)", insertStatement);

        String[] keyFields = {"column1", "column2"};
//        String[] partitionFields = {"column2"};

        String replaceIntoStatement = DatabendStatementFactory.getReplaceIntoStatement("test", fields, keyFields);
        System.out.println(replaceIntoStatement);
        assertEquals("REPLACE INTO `test`(`column1`, `column2`) ON (`column1`, `column2`) VALUES (?, ?)", replaceIntoStatement);

        String deleteStatement = DatabendStatementFactory.getDeleteStatement("test", "default", keyFields);
        System.out.println(deleteStatement);
        assertEquals("DELETE FROM `default`.`test` WHERE `column1`=? AND `column2`=?", deleteStatement);
    }
}
