package org.embulk.output.bigquery_java.exception;

public class BigqueryException extends Exception {
    public BigqueryException(String message) {
        super(message);
    }
}
