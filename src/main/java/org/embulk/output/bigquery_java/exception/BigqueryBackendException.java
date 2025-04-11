package org.embulk.output.bigquery_java.exception;

public class BigqueryBackendException extends BigqueryException {
  public BigqueryBackendException(String message) {
    super(message);
  }
}
