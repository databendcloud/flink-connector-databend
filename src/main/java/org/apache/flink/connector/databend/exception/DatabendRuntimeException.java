package org.apache.flink.connector.databend.exception;

public class DatabendRuntimeException extends RuntimeException {
    public DatabendRuntimeException() {
        super();
    }

    public DatabendRuntimeException(String message) {
        super(message);
    }

    public DatabendRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatabendRuntimeException(Throwable cause) {
        super(cause);
    }

    protected DatabendRuntimeException(
            String message,
            Throwable cause,
            boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
