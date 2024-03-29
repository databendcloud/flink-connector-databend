package org.apache.flink.connector.databend.internal.schema;

import java.io.Serializable;

/** Expression. */
public abstract class Expression implements Serializable {

    private static final long serialVersionUID = 1L;

    public abstract String explain();
}
