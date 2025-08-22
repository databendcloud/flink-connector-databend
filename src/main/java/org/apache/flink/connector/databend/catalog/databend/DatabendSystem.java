package org.apache.flink.connector.databend.catalog.databend;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.annotation.Public;
import org.apache.flink.connector.databend.exception.DatabendRuntimeException;
import org.apache.flink.connector.databend.exception.DatabendSystemException;
import org.apache.flink.connector.databend.internal.connection.DatabendConnectionProvider;
import org.apache.flink.connector.databend.internal.options.DatabendConnectionOptions;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Databend System Operate.
 */
@Public
public class DatabendSystem implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DatabendSystem.class);
    private final DatabendConnectionProvider jdbcConnectionProvider;
    private static final List<String> builtinDatabases =
            Collections.singletonList("information_schema");

    public DatabendSystem(DatabendConnectionOptions options) {
        this.jdbcConnectionProvider = new DatabendConnectionProvider(options, options.getConnectionProperties());
    }

    public List<String> listDatabases() {
        return extractColumnValuesBySQL(
                "select schema_name from information_schema.schemata;;",
                1,
                dbName -> !builtinDatabases.contains(dbName));
    }

    public boolean databaseExists(String database) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(database));
        return listDatabases().contains(database);
    }

    private static List<String> identifier(List<String> name) {
        List<String> result = name.stream().map(m -> identifier(m)).collect(Collectors.toList());
        return result;
    }

    public boolean createDatabase(String database) {
        execute(String.format("CREATE DATABASE IF NOT EXISTS %s", database));
        return true;
    }

    public void execute(String sql) {
        try (Statement statement =
                     jdbcConnectionProvider.getOrCreateConnection().createStatement()) {
            statement.execute(sql);
        } catch (Exception e) {
            throw new DatabendSystemException(
                    String.format("SQL query could not be executed: %s", sql), e);
        }
    }

    public boolean tableExists(String database, String table) {
        return databaseExists(database) && listTables(database).contains(table);
    }

    public List<String> listTables(String databaseName) {
        if (!databaseExists(databaseName)) {
            throw new DatabendRuntimeException("database" + databaseName + " is not exists");
        }
        return extractColumnValuesBySQL(
                "SELECT TABLE_NAME FROM information_schema.`TABLES` WHERE TABLE_SCHEMA = ?",
                1,
                null,
                databaseName);
    }

    public void createTable(TableSchema schema) {
        String ddl = buildCreateTableDDL(schema);
        LOG.info("Create table with ddl:{}", ddl);
        execute(ddl);
    }

    public static String buildCreateTableDDL(TableSchema schema) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        sb.append(identifier(schema.getDatabase()))
                .append(".")
                .append(identifier(schema.getTable()))
                .append("(");

        Map<String, FieldSchema> fields = schema.getFields();

        // append values
        for (Map.Entry<String, FieldSchema> entry : fields.entrySet()) {
            FieldSchema field = entry.getValue();
            buildColumn(sb, field, false);
        }
        sb = sb.deleteCharAt(sb.length() - 1);
        sb.append(" ) ");

        // append table comment
        if (!StringUtils.isNullOrWhitespaceOnly(schema.getTableComment())) {
            sb.append(" COMMENT '").append(quoteComment(schema.getTableComment())).append("' ");
        }


        return sb.toString();
    }

    private static void buildColumn(StringBuilder sql, FieldSchema field, boolean isKey) {
        String fieldType = field.getTypeString();
        if (isKey && DatabendType.STRING.equals(fieldType)) {
            fieldType = String.format("%s(%s)", DatabendType.VARCHAR, 65533);
        }
        sql.append(identifier(field.getName())).append(" ").append(fieldType);

        if (field.getDefaultValue() != null) {
            sql.append(" DEFAULT " + quoteDefaultValue(field.getDefaultValue()));
        }
        sql.append(" COMMENT '").append(quoteComment(field.getComment())).append("',");
    }

    public static String quoteComment(String comment) {
        if (comment == null) {
            return "";
        } else {
            return comment.replaceAll("'", "\\\\'");
        }
    }

    public static String quoteDefaultValue(String defaultValue) {
        // DEFAULT current_timestamp not need quote
        if (defaultValue.equalsIgnoreCase("current_timestamp")) {
            return defaultValue;
        }
        return "'" + defaultValue + "'";
    }


    public List<String> extractColumnValuesBySQL(
            String sql, int columnIndex, Predicate<String> filterFunc, Object... params) {

        List<String> columnValues = Lists.newArrayList();
        try (PreparedStatement ps =
                     jdbcConnectionProvider.getOrCreateConnection().prepareStatement(sql)) {
            if (Objects.nonNull(params) && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
            }
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String columnValue = rs.getString(columnIndex);
                if (Objects.isNull(filterFunc) || filterFunc.test(columnValue)) {
                    columnValues.add(columnValue);
                }
            }
            return columnValues;
        } catch (Exception e) {
            throw new DatabendSystemException(
                    String.format("The following SQL query could not be executed: %s", sql), e);
        }
    }

    private static String identifier(String name) {
        return "`" + name + "`";
    }


    private String quoteProperties(String name) {
        return "'" + name + "'";
    }
}