package org.apache.flink.connector.databend.catalog.databend;

import org.apache.flink.annotation.Public;
import org.apache.flink.connector.databend.internal.connection.DatabendConnectionProvider;
import org.apache.flink.connector.databend.internal.options.DatabendConnectionOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Doris System Operate.
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


    private List<String> identifier(List<String> name) {
        List<String> result = name.stream().map(m -> identifier(m)).collect(Collectors.toList());
        return result;
    }

    private String identifier(String name) {
        return "`" + name + "`";
    }

    private String quoteProperties(String name) {
        return "'" + name + "'";
    }
}