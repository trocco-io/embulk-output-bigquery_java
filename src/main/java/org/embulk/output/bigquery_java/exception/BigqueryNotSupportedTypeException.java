package org.embulk.output.bigquery_java.exception;

public class BigqueryNotSupportedTypeException extends BigqueryException {
  public BigqueryNotSupportedTypeException(String message) {
    super(message);
  }
}
