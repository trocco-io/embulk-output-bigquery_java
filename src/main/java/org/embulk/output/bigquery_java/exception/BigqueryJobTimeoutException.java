package org.embulk.output.bigquery_java.exception;

public class BigqueryJobTimeoutException extends BigqueryException{
    public BigqueryJobTimeoutException(String message){
        super(message);
    }
}
