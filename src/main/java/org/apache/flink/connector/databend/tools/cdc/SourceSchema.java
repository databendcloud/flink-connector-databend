package org.apache.flink.connector.databend.tools.cdc;

import org.apache.flink.connector.databend.catalog.databend.DataModel;
import org.apache.flink.connector.databend.catalog.databend.FieldSchema;
import org.apache.flink.connector.databend.catalog.databend.TableSchema;
import org.apache.flink.util.StringUtils;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.*;

public abstract class SourceSchema {
    private final String databaseName;
    private final String schemaName;
    private final String tableName;
    private final String tableComment;
    private final LinkedHashMap<String, FieldSchema> fields;
    public final List<String> primaryKeys;
    public DataModel model = DataModel.UNIQUE;

    public SourceSchema(
            DatabaseMetaData metaData,
            String databaseName,
            String schemaName,
            String tableName,
            String tableComment)
            throws Exception {
        this.databaseName = databaseName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableComment = tableComment;

        fields = new LinkedHashMap<>();
        try (ResultSet rs = metaData.getColumns(databaseName, schemaName, tableName, null)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                String comment = rs.getString("REMARKS");
                String fieldType = rs.getString("TYPE_NAME");
                String defaultValue = rs.getString("COLUMN_DEF");
                Integer precision = rs.getInt("COLUMN_SIZE");
                if (rs.wasNull()) {
                    precision = null;
                }

                Integer scale = rs.getInt("DECIMAL_DIGITS");
                if (rs.wasNull()) {
                    scale = null;
                }
                String dorisTypeStr = convertToDorisType(fieldType, precision, scale);
                fields.put(
                        fieldName, new FieldSchema(fieldName, dorisTypeStr, defaultValue, comment));
            }
        }

        primaryKeys = new ArrayList<>();
        try (ResultSet rs = metaData.getPrimaryKeys(databaseName, schemaName, tableName)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                primaryKeys.add(fieldName);
            }
        }
    }

    public abstract String convertToDorisType(String fieldType, Integer precision, Integer scale);

    public String getTableIdentifier() {
        return getString(databaseName, schemaName, tableName);
    }

    public static String getString(String databaseName, String schemaName, String tableName) {
        StringJoiner identifier = new StringJoiner(".");
        if (!StringUtils.isNullOrWhitespaceOnly(databaseName)) {
            identifier.add(databaseName);
        }
        if (!StringUtils.isNullOrWhitespaceOnly(schemaName)) {
            identifier.add(schemaName);
        }

        if (!StringUtils.isNullOrWhitespaceOnly(tableName)) {
            identifier.add(tableName);
        }

        return identifier.toString();
    }

    public TableSchema convertTableSchema(Map<String, String> tableProps) {
        TableSchema tableSchema = new TableSchema();
        tableSchema.setModel(this.model);
        tableSchema.setFields(this.fields);
        tableSchema.setKeys(buildKeys());
        tableSchema.setTableComment(this.tableComment);
        tableSchema.setProperties(tableProps);
        return tableSchema;
    }

    private List<String> buildKeys() {
        return buildDistributeKeys();
    }

    private List<String> buildDistributeKeys() {
        if (!this.primaryKeys.isEmpty()) {
            return primaryKeys;
        }
        if (!this.fields.isEmpty()) {
            Map.Entry<String, FieldSchema> firstField = this.fields.entrySet().iterator().next();
            return Collections.singletonList(firstField.getKey());
        }
        return new ArrayList<>();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, FieldSchema> getFields() {
        return fields;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public String getTableComment() {
        return tableComment;
    }

    public DataModel getModel() {
        return model;
    }

    public void setModel(DataModel model) {
        this.model = model;
    }
}

