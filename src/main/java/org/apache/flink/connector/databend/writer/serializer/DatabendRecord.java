package org.apache.flink.connector.databend.writer.serializer;

import java.io.Serializable;

public class DatabendRecord implements Serializable {
    public static DatabendRecord empty = new DatabendRecord();

    private String database;
    private String table;
    private byte[] row;

    public DatabendRecord() {
    }

    public DatabendRecord(String database, String table, byte[] row) {
        this.database = database;
        this.table = table;
        this.row = row;
    }

    public String getTableIdentifier() {
        if (database == null || table == null) {
            return null;
        }
        return database + "." + table;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public byte[] getRow() {
        return row;
    }

    public void setRow(byte[] row) {
        this.row = row;
    }

    public static DatabendRecord of(String database, String table, byte[] row) {
        return new DatabendRecord(database, table, row);
    }

    public static DatabendRecord of(String tableIdentifier, byte[] row) {
        if (tableIdentifier != null) {
            String[] dbTbl = tableIdentifier.split("\\.");
            if (dbTbl.length == 2) {
                String database = dbTbl[0];
                String table = dbTbl[1];
                return new DatabendRecord(database, table, row);
            }
        }
        return null;
    }

    public static DatabendRecord of(byte[] row) {
        return new DatabendRecord(null, null, row);
    }
}