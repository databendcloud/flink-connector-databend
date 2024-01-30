package org.apache.flink.connector.databend.catalog.databend;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableSchema {
    private String database;
    private String table;
    private String tableComment;
    private Map<String, FieldSchema> fields;
    private List<String> keys = new ArrayList<>();
    private DataModel model = DataModel.DUPLICATE;
    private Map<String, String> properties = new HashMap<>();


    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getTableComment() {
        return tableComment;
    }

    public Map<String, FieldSchema> getFields() {
        return fields;
    }

    public List<String> getKeys() {
        return keys;
    }

    public DataModel getModel() {
        return model;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setTableComment(String tableComment) {
        this.tableComment = tableComment;
    }

    public void setFields(Map<String, FieldSchema> fields) {
        this.fields = fields;
    }

    public void setKeys(List<String> keys) {
        this.keys = keys;
    }

    public void setModel(DataModel model) {
        this.model = model;
    }


    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

}
