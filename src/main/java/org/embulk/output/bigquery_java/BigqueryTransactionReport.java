package org.embulk.output.bigquery_java;

import java.math.BigInteger;

public class BigqueryTransactionReport {
  private BigInteger numInputRows;
  private BigInteger numResponseRows;
  private BigInteger numOutputRows;
  private BigInteger numRejectedRows;

  @Override
  public String toString() {
    return "BigqueryTransactionReport{"
        + "numInputRows="
        + numInputRows
        + ", numResponseRows="
        + numResponseRows
        + ", numOutputRows="
        + numOutputRows
        + ", numRejectedRows="
        + numRejectedRows
        + '}';
  }

  public BigqueryTransactionReport(
      BigInteger numInputRows,
      BigInteger numResponseRows,
      BigInteger numOutputRows,
      BigInteger numRejectedRows) {
    this.numInputRows = numInputRows;
    this.numResponseRows = numResponseRows;
    this.numOutputRows = numOutputRows;
    this.numRejectedRows = numRejectedRows;
  }

  public BigqueryTransactionReport(BigInteger numInputRows) {
    this.numInputRows = numInputRows;
  }

  public BigInteger getNumInputRows() {
    return numInputRows;
  }

  public BigInteger getNumResponseRows() {
    return numResponseRows;
  }

  public BigInteger getNumOutputRows() {
    return numOutputRows;
  }

  public BigInteger getNumRejectedRows() {
    return numRejectedRows;
  }
}
