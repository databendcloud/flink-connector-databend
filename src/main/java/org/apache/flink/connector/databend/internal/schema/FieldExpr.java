package org.apache.flink.connector.databend.internal.schema;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

import javax.annotation.Nonnull;

/** Column expression. */
public class FieldExpr extends Expression {

    private final String columnName;

    private FieldExpr(String columnName) {
        checkArgument(!isNullOrWhitespaceOnly(columnName), "columnName cannot be null or empty");
        this.columnName = columnName;
    }

    public static FieldExpr of(@Nonnull String columnName) {
        return new FieldExpr(columnName);
    }

    public String getColumnName() {
        return columnName;
    }

    @Override
    public String explain() {
        return columnName;
    }
}
