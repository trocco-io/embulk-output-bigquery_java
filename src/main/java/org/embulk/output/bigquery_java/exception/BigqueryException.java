package org.embulk.output.bigquery_java.exception;

public class BigqueryException extends RuntimeException {
    public BigqueryException(String message) {
        super(message);
    }
}
