package org.apache.flink.connector.databend.catalog;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.databend.config.DatabendConfig.IDENTIFIER;
import static org.apache.flink.connector.databend.config.DatabendConfig.PROPERTIES_PREFIX;
import static org.apache.flink.connector.databend.config.DatabendConfigOptions.DATABASE_NAME;
import static org.apache.flink.connector.databend.config.DatabendConfigOptions.PASSWORD;
import static org.apache.flink.connector.databend.config.DatabendConfigOptions.SINK_BATCH_SIZE;
import static org.apache.flink.connector.databend.config.DatabendConfigOptions.SINK_FLUSH_INTERVAL;
import static org.apache.flink.connector.databend.config.DatabendConfigOptions.SINK_IGNORE_DELETE;
import static org.apache.flink.connector.databend.config.DatabendConfigOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.databend.config.DatabendConfigOptions.SINK_UPDATE_STRATEGY;
import static org.apache.flink.connector.databend.config.DatabendConfigOptions.URL;
import static org.apache.flink.connector.databend.config.DatabendConfigOptions.USERNAME;
import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;


public class DatabendCatalogFactory implements CatalogFactory {
    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(URL);
        options.add(USERNAME);
        options.add(PASSWORD);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DATABASE_NAME);
        options.add(PROPERTY_VERSION);

        options.add(SINK_BATCH_SIZE);
        options.add(SINK_FLUSH_INTERVAL);
        options.add(SINK_MAX_RETRIES);
        options.add(SINK_UPDATE_STRATEGY);
        options.add(SINK_IGNORE_DELETE);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validateExcept(PROPERTIES_PREFIX);

        return new DatabendCatalog(
                context.getName(),
                helper.getOptions().get(DATABASE_NAME),
                helper.getOptions().get(URL),
                helper.getOptions().get(USERNAME),
                helper.getOptions().get(PASSWORD),
                ((Configuration) helper.getOptions()).toMap());
    }
}
