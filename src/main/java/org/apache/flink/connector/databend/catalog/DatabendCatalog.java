package org.apache.flink.connector.databend.catalog;

import com.databend.jdbc.DatabendColumnInfo;
import com.databend.jdbc.DatabendResultSetMetaData;
import org.apache.flink.connector.databend.DatabendDynamicTableFactory;
import org.apache.flink.connector.databend.util.DataTypeUtil;
import org.apache.flink.connector.databend.util.DatabendUtil;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

import static org.apache.flink.connector.databend.config.DatabendConfig.*;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

public class DatabendCatalog extends AbstractCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(DatabendCatalog.class);

    public static final String DEFAULT_DATABASE = "default";

    private final String baseUrl;

    private final String username;

    private final String password;

    private final Map<String, String> properties;

    private Connection connection;

    public DatabendCatalog(String catalogName, Map<String, String> properties) {
        this(
                catalogName,
                properties.get(DATABASE_NAME),
                properties.get(URL),
                properties.get(USERNAME),
                properties.get(PASSWORD),
                properties);
    }

    public DatabendCatalog(
            String catalogName, @Nullable String defaultDatabase, String baseUrl, String username, String password) {
        this(catalogName, defaultDatabase, baseUrl, username, password, Collections.emptyMap());
    }

    public DatabendCatalog(
            String catalogName,
            @Nullable String defaultDatabase,
            String baseUrl,
            String username,
            String password,
            Map<String, String> properties) {
        super(catalogName, defaultDatabase == null ? DEFAULT_DATABASE : defaultDatabase);

        checkArgument(!isNullOrWhitespaceOnly(baseUrl), "baseUrl cannot be null or empty");
        checkArgument(!isNullOrWhitespaceOnly(username), "username cannot be null or empty");
        checkArgument(!isNullOrWhitespaceOnly(password), "password cannot be null or empty");

        this.baseUrl = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
        this.username = username;
        this.password = password;
        this.properties = Collections.unmodifiableMap(properties);
    }

    @Override
    public void open() throws CatalogException {
        try {
            Properties configuration = new Properties();
            configuration.putAll(properties);
            configuration.setProperty("user", username);
            configuration.setProperty("password", password);
            String jdbcUrl = DatabendUtil.getJdbcUrl(baseUrl, getDefaultDatabase());

            connection = DriverManager.getConnection(baseUrl, configuration);
            LOG.info("Created catalog {}, established connection to {}", getName(), jdbcUrl);
        } catch (Exception e) {
            throw new CatalogException(String.format("Opening catalog %s failed.", getName()), e);
        }
    }

    @Override
    public synchronized void close() throws CatalogException {
        try {
            connection.close();
            LOG.info("Closed catalog {} ", getName());
        } catch (Exception e) {
            throw new CatalogException(String.format("Closing catalog %s failed.", getName()), e);
        }
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new DatabendDynamicTableFactory());
    }

    // ------------- databases -------------

    @Override
    public synchronized List<String> listDatabases() throws CatalogException {
        // Sometimes we need to look up database `system`, so we won't get rid of it.
        try (PreparedStatement stmt = connection.prepareStatement("SELECT name from `system`.databases");
             ResultSet rs = stmt.executeQuery()) {
            List<String> databases = new ArrayList<>();

            while (rs.next()) {
                databases.add(rs.getString(1));
            }

            return databases;
        } catch (Exception e) {
            throw new CatalogException(String.format("Failed listing database in catalog %s", getName()), e);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (listDatabases().contains(databaseName)) {
            return new CatalogDatabaseImpl(Collections.emptyMap(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        checkArgument(!isNullOrWhitespaceOnly(databaseName));

        return listDatabases().contains(databaseName);
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotEmptyException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    // ------------- tables -------------

    @Override
    public synchronized List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        try (PreparedStatement stmt = connection.prepareStatement(
                String.format("SELECT name from `system`.tables where database = '%s'", databaseName));
             ResultSet rs = stmt.executeQuery()) {
            List<String> tables = new ArrayList<>();

            while (rs.next()) {
                tables.add(rs.getString(1));
            }

            return tables;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing tables in catalog %s database %s", getName(), databaseName), e);
        }
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        Map<String, String> configuration = new HashMap<>(properties);
        configuration.put(URL, baseUrl);
        configuration.put(DATABASE_NAME, tablePath.getDatabaseName());
        configuration.put(TABLE_NAME, tablePath.getObjectName());
        configuration.put(USERNAME, username);
        configuration.put(PASSWORD, password);

        String databaseName = tablePath.getDatabaseName();
        String tableName = tablePath.getObjectName();

        return new CatalogTableImpl(createTableSchema(databaseName, tableName), configuration, "");
    }

    private synchronized TableSchema createTableSchema(String databaseName, String tableName) {
        try (PreparedStatement stmt = connection.prepareStatement(
                String.format("SELECT * from `%s`.`%s` limit 1", databaseName, tableName))) {
            DatabendResultSetMetaData metaData = stmt.getMetaData().unwrap(DatabendResultSetMetaData.class);
            Method getColMethod = metaData.getClass().getDeclaredMethod("getCol", int.class);
            getColMethod.setAccessible(true);

            List<String> primaryKeys = getPrimaryKeys(databaseName, tableName);
            TableSchema.Builder builder = TableSchema.builder();
            for (int idx = 1; idx <= metaData.getColumnCount(); idx++) {
                DatabendColumnInfo columnInfo = (DatabendColumnInfo) getColMethod.invoke(metaData, idx);
                String columnName = columnInfo.getColumnName();
                DataType columnType = DataTypeUtil.toFlinkType(columnInfo);
                if (primaryKeys.contains(columnName)) {
                    columnType = columnType.notNull();
                }
                builder.field(columnName, columnType);
            }

            if (!primaryKeys.isEmpty()) {
                builder.primaryKey(primaryKeys.toArray(new String[0]));
            }
            return builder.build();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed getting columns in catalog %s database %s table %s",
                            getName(), databaseName, tableName),
                    e);
        }
    }

    private List<String> getPrimaryKeys(String databaseName, String tableName) {
        return Collections.emptyList();
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        try {
            return databaseExists(tablePath.getDatabaseName())
                    && listTables(tablePath.getDatabaseName()).contains(tablePath.getObjectName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    // ------------- partitions -------------

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
            CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists)
            throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
            PartitionAlreadyExistsException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    // ------------- functions -------------

    @Override
    public List<String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    // ------------- statistics -------------

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException, TablePartitionedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }
}
