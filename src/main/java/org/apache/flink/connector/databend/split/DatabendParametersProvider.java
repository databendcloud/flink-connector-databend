package org.apache.flink.connector.databend.split;

import java.io.Serializable;

/**
 * Databend parameters provider.
 */
public abstract class DatabendParametersProvider {

    protected Serializable[][] parameterValues;
    protected Serializable[][] shardIdValues;
    protected int batchNum;

    /**
     * Returns the necessary parameters array to use for query in parallel a table.
     */
    public Serializable[][] getParameterValues() {
        return parameterValues;
    }

    /**
     * Returns the shard ids that the parameter values act on.
     */
    public Serializable[][] getShardIdValues() {
        return shardIdValues;
    }

    public abstract String getParameterClause();

    public abstract DatabendParametersProvider ofBatchNum(Integer batchNum);

    public abstract DatabendParametersProvider calculate();

    // -------------------------- Methods for local tables --------------------------

    /**
     * Builder.
     */
    public static class Builder {

        private Long minVal;

        private Long maxVal;

        private Integer batchNum;

        private int[] shardIds;

        private boolean useLocal;

        public Builder setMinVal(Long minVal) {
            this.minVal = minVal;
            return this;
        }

        public Builder setMaxVal(Long maxVal) {
            this.maxVal = maxVal;
            return this;
        }

        public Builder setBatchNum(Integer batchNum) {
            this.batchNum = batchNum;
            return this;
        }

        public Builder setShardIds(int[] shardIds) {
            this.shardIds = shardIds;
            return this;
        }

        public Builder setUseLocal(boolean useLocal) {
            this.useLocal = useLocal;
            return this;
        }
    }
}
