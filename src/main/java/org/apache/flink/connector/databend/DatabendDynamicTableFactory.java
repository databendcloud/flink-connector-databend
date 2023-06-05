package org.apache.flink.connector.databend;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.databend.internal.options.DatabendDmlOptions;
import org.apache.flink.connector.databend.internal.options.DatabendReadOptions;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connector.databend.config.DatabendConfig.IDENTIFIER;
import static org.apache.flink.connector.databend.config.DatabendConfig.PROPERTIES_PREFIX;
import static org.apache.flink.connector.databend.config.DatabendConfigOptions.*;
import static org.apache.flink.connector.databend.util.DatabendUtil.getDatabendProperties;

/**
 * A {@link DynamicTableSinkFactory} for discovering {@link DatabendDynamicTableSink}.
 */
public class DatabendDynamicTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    public DatabendDynamicTableFactory() {
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validateExcept(PROPERTIES_PREFIX);
        validateConfigOptions(config);

        ResolvedCatalogTable catalogTable = context.getCatalogTable();
        String[] primaryKeys = catalogTable
                .getResolvedSchema()
                .getPrimaryKey()
                .map(UniqueConstraint::getColumns)
                .map(keys -> keys.toArray(new String[0]))
                .orElse(new String[0]);
        Properties databendProperties =
                getDatabendProperties(context.getCatalogTable().getOptions());
        return new DatabendDynamicTableSink(
                getDmlOptions(config),
                databendProperties,
                getDmlOptions(config).getPrimaryKeys(),
                catalogTable.getPartitionKeys().toArray(new String[0]),
                context.getPhysicalRowDataType());
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validateExcept(PROPERTIES_PREFIX);
        validateConfigOptions(config);

        Properties databendProperties =
                getDatabendProperties(context.getCatalogTable().getOptions());
        return new DatabendDynamicTableSource(
                getReadOptions(config), databendProperties, context.getPhysicalRowDataType());
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(TABLE_NAME);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(USERNAME);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(DATABASE_NAME);
        optionalOptions.add(SINK_BATCH_SIZE);
        optionalOptions.add(SINK_FLUSH_INTERVAL);
        optionalOptions.add(SINK_MAX_RETRIES);
        optionalOptions.add(SINK_UPDATE_STRATEGY);
        optionalOptions.add(SINK_PRIMARY_KEYS);
        optionalOptions.add(SINK_IGNORE_DELETE);
        optionalOptions.add(SINK_PARALLELISM);
        return optionalOptions;
    }

    private void validateConfigOptions(ReadableConfig config) {
        if (config.getOptional(USERNAME).isPresent()
                ^ config.getOptional(PASSWORD).isPresent()) {
            throw new IllegalArgumentException("Either all or none of username and password should be provided");
        }
    }

    public DatabendDmlOptions getDmlOptions(ReadableConfig config) {
        return new DatabendDmlOptions.Builder()
                .withUrl(config.get(URL))
                .withUsername(config.get(USERNAME))
                .withPassword(config.get(PASSWORD))
                .withDatabaseName(config.get(DATABASE_NAME))
                .withTableName(config.get(TABLE_NAME))
                .withBatchSize(config.get(SINK_BATCH_SIZE))
                .withFlushInterval(config.get(SINK_FLUSH_INTERVAL))
                .withMaxRetries(config.get(SINK_MAX_RETRIES))
                .withUpdateStrategy(config.get(SINK_UPDATE_STRATEGY))
                .withPrimaryKey(config.get(SINK_PRIMARY_KEYS).toArray(new String[0]))
                .withIgnoreDelete(config.get(SINK_IGNORE_DELETE))
                .withParallelism(config.get(SINK_PARALLELISM))
                .build();
    }

    private DatabendReadOptions getReadOptions(ReadableConfig config) {
        return new DatabendReadOptions.Builder()
                .withUrl(config.get(URL))
                .withUsername(config.get(USERNAME))
                .withPassword(config.get(PASSWORD))
                .withDatabaseName(config.get(DATABASE_NAME))
                .withTableName(config.get(TABLE_NAME))
                .build();
    }
}
