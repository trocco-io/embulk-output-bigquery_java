package org.embulk.output.bigquery_java.exception;

public class BigqueryRateLimitExceededException extends BigqueryException {
    public BigqueryRateLimitExceededException(String message) {
        super(message);
    }
}
