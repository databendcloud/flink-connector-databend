package org.apache.flink.connector.databend.catalog.databend;

public class FieldSchema {
    private String name;
    private String typeString;
    private String comment;

    private String defaultValue;

    public FieldSchema() {
    }

    public FieldSchema(String name, String typeString, String comment, String defaultValue) {
        this.name = name;
        this.typeString = typeString;
        this.defaultValue = defaultValue;
        this.comment = comment;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTypeString() {
        return typeString;
    }

    public void setTypeString(String typeString) {
        this.typeString = typeString;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getDefaultValue() {
        return defaultValue;
    }
}


