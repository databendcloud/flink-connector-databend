package org.apache.flink.connector.databend.tools.cdc;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * cdc sync tools
 */
public class CdcTools {
    private static final String MYSQL_SYNC_DATABASE = "mysql-sync-database";
    private static final List<String> EMPTY_KEYS = Arrays.asList("password");

    public static void main(String[] args) throws Exception {
        String operation = args[0].toLowerCase();
        String[] opArgs = Arrays.copyOfRange(args, 1, args.length);
        System.out.println();
        switch (operation) {
            case MYSQL_SYNC_DATABASE:
                createMySQLSyncDatabase(opArgs);
                break;
            default:
                System.out.println("Unknown operation " + operation);
                System.exit(1);
        }
    }

    private static void createMySQLSyncDatabase(String[] opArgs) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(opArgs);
        String jobName = params.get("job-name");
        String database = params.get("database");
        String tablePrefix = params.get("table-prefix");
        String tableSuffix = params.get("table-suffix");
        String includingTables = params.get("including-tables");
        String excludingTables = params.get("excluding-tables");

        Map<String, String> mysqlMap = getConfigMap(params, "mysql-conf");
        Map<String, String> sinkMap = getConfigMap(params, "sink-conf");
        Map<String, String> tableMap = getConfigMap(params, "table-conf");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration mysqlConfig = Configuration.fromMap(mysqlMap);
        Configuration sinkConfig = Configuration.fromMap(sinkMap);

        DatabaseSync databaseSync = new MysqlDatabaseSync();
        databaseSync.create(env, database, mysqlConfig, tablePrefix, tableSuffix, includingTables, excludingTables, sinkConfig, tableMap);
        databaseSync.build();

        if(StringUtils.isNullOrWhitespaceOnly(jobName)){
            jobName = String.format("MySQL-Databend Sync Database: %s", mysqlMap.get("database-name"));
        }
        env.execute(jobName);
    }

    private static Map<String, String> getConfigMap(MultipleParameterTool params, String key) {
        if (!params.has(key)) {
            return null;
        }

        Map<String, String> map = new HashMap<>();
        for (String param : params.getMultiParameter(key)) {
            String[] kv = param.split("=");
            if (kv.length == 2) {
                map.put(kv[0], kv[1]);
                continue;
            }else if(kv.length == 1 && EMPTY_KEYS.contains(kv[0])){
                map.put(kv[0], "");
                continue;
            }

            System.err.println(
                    "Invalid " + key + " " + param + ".\n");
            return null;
        }
        return map;
    }
}
