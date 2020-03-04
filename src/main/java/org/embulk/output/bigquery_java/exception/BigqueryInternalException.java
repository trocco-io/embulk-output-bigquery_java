package org.embulk.output.bigquery_java.exception;

public class BigqueryInternalException extends BigqueryException {
    public BigqueryInternalException(String message){
        super(message);
    }
}
