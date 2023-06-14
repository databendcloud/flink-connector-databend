# Flink Databend Connector

[Flink](https://github.com/apache/flink) SQL connector
for [Databend](https://github.com/datafuselabs/databend) database, this project Powered
by [Databend JDBC](https://github.com/databendcloud/databend-jdbc).

Currently, the project supports `Sink Table`.  
Please create issues if you encounter bugs and any help for the project is greatly appreciated.

## Connector Options

| Option               | Required | Default | Type      | Description                                                                                                              |
|:---------------------| :------- |:--------|:----------|:-------------------------------------------------------------------------------------------------------------------------|
| url                  | required | none    | String    | The Databend jdbc url in format `databend://<host>:<port>`.                                                              |
| username             | optional | none    | String    | The 'username' and 'password' must both be specified if any of them is specified.                                        |
| password             | optional | none    | String    | The Databend password.                                                                                                   |
| database-name        | optional | default | String    | The Databend database name.                                                                                              |
| table-name           | required | none    | String    | The Databend table name.                                                                                                 |
| sink.batch-size      | optional | 1000    | Integer   | The max flush size, over this will flush data.                                                                           |
| sink.flush-interval  | optional | 1s      | Duration  | Over this flush interval mills, asynchronous threads will flush data.                                                    |
| sink.max-retries     | optional | 3       | Integer   | The max retry times when writing records to the database failed.                                                         |
| sink.update-strategy | optional | update  | String    | Convert a record of type UPDATE_AFTER to update/insert statement or just discard it, available: update, insert, discard. |
| sink.primary-key     | optional | ["id"]  | String    | The primary key used in upsert                                                                                           |

**Upsert Data Considerations:**

## Data Type Mapping

| Flink Type          | Databend Type                                          |
| :------------------ |:-------------------------------------------------------|
| CHAR                | String                                                 |
| VARCHAR             | String                                                 |
| STRING              | String                                                 |
| BOOLEAN             | Boolean                                                |
| BYTES               | String                                                 |
| DECIMAL             | Decimal / Int128 / Int256 / UInt64 / UInt128 / UInt256 |
| TINYINT             | Int8                                                   |
| SMALLINT            | Int16 / UInt8                                          |
| INTEGER             | Int32 / UInt16 / Interval                              |
| BIGINT              | Int64 / UInt32                                         |
| FLOAT               | Float                                                  |
| DOUBLE              | Double                                                 |
| DATE                | Date                                                   |
| TIME                | DateTime                                               |
| TIMESTAMP           | DateTime                                               |
| TIMESTAMP_LTZ       | DateTime                                               |
| INTERVAL_YEAR_MONTH | Int32                                                  |
| INTERVAL_DAY_TIME   | Int64                                                  |
| ARRAY               | Array                                                  |
| MAP                 | Map                                                    |
| ROW                 | Not supported                                          |
| MULTISET            | Not supported                                          |
| RAW                 | Not supported                                          |

## Maven Dependency

The project isn't published to the maven central repository, we need to deploy/install to our own
repository before use it, step as follows:

```bash
# clone the project
git clone https://github.com/databendcloud/flink-connector-databend.git

# enter the project directory
cd flink-connector-databend/

# display remote branches
git branch -r

# checkout the branch you need
git checkout $branch_name

# install or deploy the project to our own repository
mvn clean install -DskipTests
mvn clean deploy -DskipTests
```

```xml

<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-databend</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## How to use

### Create and read/write table

```SQL

-- register a databend table `t_user` in flink sql.
CREATE TABLE t_user (
    `user_id` BIGINT,
    `user_type` INTEGER,
    `language` STRING,
    `country` STRING,
    `gender` STRING,
    `score` DOUBLE,
    `list` ARRAY<STRING>,
    `map` Map<STRING, BIGINT>,
    PRIMARY KEY (user_id)
) WITH (
    'connector' = 'databend',
    'url' = 'databend://{ip}:{port}',
    'database-name' = 'default',
    'table-name' = 'users',
    'sink.batch-size' = '500',
    'sink.flush-interval' = '1000',
    'sink.max-retries' = '3'
);

-- read data from databend 
SELECT user_id, user_type from t_user;

-- write data into the databend table from the table `T`
INSERT INTO t_user
SELECT cast(`user_id` as BIGINT), `user_type`, `lang`, `country`, `gender`, `score`, ARRAY['CODER', 'SPORTSMAN'], CAST(MAP['BABA', cast(10 as BIGINT), 'NIO', cast(8 as BIGINT)] AS MAP<STRING, BIGINT>) FROM T;

```

### Create and use DatabendCatalog

#### Scala

```scala
val tEnv = TableEnvironment.create(setting)

val props = new util.HashMap[String, String]()
props.put(DatabendConfig.DATABASE_NAME, "default")
props.put(DatabendConfig.URL, "databend://127.0.0.1:8000")
props.put(DatabendConfig.USERNAME, "username")
props.put(DatabendConfig.PASSWORD, "password")
props.put(DatabendConfig.SINK_FLUSH_INTERVAL, "30s")
val cHcatalog = new DatabendConfig("databend", props)
tEnv.registerCatalog("databend", datbendcatalog)
tEnv.useCatalog("databend")

tEnv.executeSql("insert into `databend`.`default`.`t_table` select...");
```

#### Java

```java
TableEnvironment tEnv = TableEnvironment.create(setting);

Map<String, String> props = new HashMap<>();
props.put(DatabendConfig.DATABASE_NAME, "default")
props.put(DatabendConfig.URL, "databend://127.0.0.1:8000")
props.put(DatabendConfig.USERNAME, "username")
props.put(DatabendConfig.PASSWORD, "password")
props.put(DatabendConfig.SINK_FLUSH_INTERVAL, "30s");
Catalog cHcatalog = new DatabendConfig("databend", props);
tEnv.registerCatalog("databend", databendcatalog);
tEnv.useCatalog("databend");

tEnv.executeSql("insert into `databend`.`default`.`t_table` select...");
```

## Roadmap

- [x] Implement the Flink SQL Sink function.
- [x] Support DatabendCatalog.
- [x] Implement the Flink SQL Source function.
- [ ] Support array and Map types.

