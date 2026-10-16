package org.embulk.output.bigquery_java.exception;

public class BigqueryUploadException extends BigqueryException {
  public BigqueryUploadException(String message, Throwable cause) {
    super(message, cause);
  }
}
