package org.embulk.output.bigquery_java.exception;

public class BigqueryTypeCastException extends BigqueryException {
    public BigqueryTypeCastException(String message) {
        super(message);
    }
}
