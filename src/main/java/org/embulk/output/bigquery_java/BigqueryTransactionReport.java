package org.embulk.output.bigquery_java;

import java.math.BigInteger;
import java.util.StringJoiner;

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

  // The report as ruby logs it: JSON with ruby's keys, and only num_input_rows when the job
  // result check is skipped (the other counts are null then).
  public String toJson() {
    StringJoiner json = new StringJoiner(",", "{", "}");
    addCount(json, "num_input_rows", numInputRows);
    addCount(json, "num_response_rows", numResponseRows);
    addCount(json, "num_output_rows", numOutputRows);
    addCount(json, "num_rejected_rows", numRejectedRows);
    return json.toString();
  }

  private static void addCount(StringJoiner json, String key, BigInteger count) {
    if (count != null) {
      json.add("\"" + key + "\":" + count);
    }
  }
}
