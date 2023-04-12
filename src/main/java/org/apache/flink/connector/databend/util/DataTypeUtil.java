package org.apache.flink.connector.databend.util;

import com.databend.jdbc.DatabendColumnInfo;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.DataType;

/**
 * Type utils.
 */
public class DataTypeUtil {

    private static final Pattern INTERNAL_TYPE_PATTERN = Pattern.compile(".*?\\((?<type>.*)\\)");

    /**
     * Convert databend data type to flink data type. <br>
     * TODO: Whether to indicate nullable?
     */
    public static DataType toFlinkType(DatabendColumnInfo columnInfo) {
        switch ((columnInfo.getColumnTypeName().toLowerCase(Locale.US))) {
            case DatabendTypes.INT8:
                return DataTypes.TINYINT();
            case DatabendTypes.INT16:
            case DatabendTypes.UINT8:
                return DataTypes.SMALLINT();
            case DatabendTypes.INT32:
            case DatabendTypes.UINT16:
                return DataTypes.INT();
            case DatabendTypes.INT64:
            case DatabendTypes.UINT32:
                return DataTypes.BIGINT();
            case DatabendTypes.FLOAT32:
                return DataTypes.FLOAT();
            case DatabendTypes.FLOAT64:
                return DataTypes.DOUBLE();
            case DatabendTypes.STRING:
                return DataTypes.STRING();
            case DatabendTypes.DATE:
                return DataTypes.DATE();
            case DatabendTypes.DATETIME:
            case DatabendTypes.DATETIME64:
                return DataTypes.TIMESTAMP(columnInfo.getScale());
            case DatabendTypes.ARRAY:
                //                String arrayBaseType =
                //                        getInternalDatabendType(databendColumnInfo.getOriginalTypeName());
                //                DatabendColumnInfo arrayBaseColumnInfo =
                //                        DatabendColumnInfo.parse(
                //                                arrayBaseType,
                //                                databendColumnInfo.getColumnName() + ".array_base",
                //                                databendColumnInfo.getTimeZone());
                //                return DataTypes.ARRAY(toFlinkType(arrayBaseColumnInfo));
            case DatabendTypes.VARIANT_OBJECT:
                //                return DataTypes.MAP(
                //                        toFlinkType(databendColumnInfo.getKeyInfo()),
                //                        toFlinkType(databendColumnInfo.getValueInfo()));
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type:" + columnInfo.getColumnTypeName().toLowerCase(Locale.US));
        }
    }

    private static String getInternalDatabendType(String databendTypeLiteral) {
        Matcher matcher = INTERNAL_TYPE_PATTERN.matcher(databendTypeLiteral);
        if (matcher.find()) {
            return matcher.group("type");
        } else {
            throw new CatalogException(String.format("No content found in the bucket of '%s'", databendTypeLiteral));
        }
    }
}
