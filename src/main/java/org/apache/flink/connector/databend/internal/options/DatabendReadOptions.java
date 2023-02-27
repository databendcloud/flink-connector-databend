package org.apache.flink.connector.databend.internal.options;

import javax.annotation.Nullable;

public class DatabendReadOptions extends DatabendConnectionOptions {
    private static final long serialVersionUID = 1L;


    private final String partitionColumn;
    private final Integer partitionNum;
    private final Long partitionLowerBound;
    private final Long partitionUpperBound;

    private DatabendReadOptions(String url, @Nullable String username, @Nullable String password, String databaseName, String tableName, String partitionColumn, Integer partitionNum, Long partitionLowerBound, Long partitionUpperBound) {
        super(url, username, password, databaseName, tableName);
        this.partitionColumn = partitionColumn;
        this.partitionNum = partitionNum;
        this.partitionLowerBound = partitionLowerBound;
        this.partitionUpperBound = partitionUpperBound;
    }


    public String getPartitionColumn() {
        return partitionColumn;
    }

    public Integer getPartitionNum() {
        return partitionNum;
    }

    public Long getPartitionLowerBound() {
        return partitionLowerBound;
    }

    public Long getPartitionUpperBound() {
        return partitionUpperBound;
    }

    /**
     * Builder for {@link DatabendReadOptions}.
     */
    public static class Builder {
        private String url;
        private String username;
        private String password;
        private String databaseName;
        private String tableName;
        private String partitionColumn;
        private Integer partitionNum;
        private Long partitionLowerBound;
        private Long partitionUpperBound;

        public DatabendReadOptions.Builder withUrl(String url) {
            this.url = url;
            return this;
        }

        public DatabendReadOptions.Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        public DatabendReadOptions.Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public DatabendReadOptions.Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public DatabendReadOptions.Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }


        public DatabendReadOptions.Builder withPartitionColumn(String partitionColumn) {
            this.partitionColumn = partitionColumn;
            return this;
        }

        public DatabendReadOptions.Builder withPartitionNum(Integer partitionNum) {
            this.partitionNum = partitionNum;
            return this;
        }

        public Builder withPartitionLowerBound(Long partitionLowerBound) {
            this.partitionLowerBound = partitionLowerBound;
            return this;
        }

        public Builder withPartitionUpperBound(Long partitionUpperBound) {
            this.partitionUpperBound = partitionUpperBound;
            return this;
        }

        public DatabendReadOptions build() {
            return new DatabendReadOptions(url, username, password, databaseName, tableName, partitionColumn, partitionNum, partitionLowerBound, partitionUpperBound);
        }
    }
}
