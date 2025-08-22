package org.apache.flink.connector.databend.exception;

public class DatabendSystemException extends RuntimeException {
    public DatabendSystemException() {
        super();
    }

    public DatabendSystemException(String message) {
        super(message);
    }

    public DatabendSystemException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatabendSystemException(Throwable cause) {
        super(cause);
    }

    protected DatabendSystemException(
            String message,
            Throwable cause,
            boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
