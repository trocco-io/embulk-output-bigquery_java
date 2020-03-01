package org.embulk.output.bigquery_java.exception;

import com.google.api.services.bigquery.Bigquery;

public class BigqueryInternalException extends BigqueryException {
    public BigqueryInternalException(String message){
        super(message);
    }
}
