package org.embulk.output.bigquery_java;

import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class BigqueryTransactionReport {
  private BigInteger numInputRows;
  private BigInteger numResponseRows;
  private BigInteger numOutputRows;
  private BigInteger numRejectedRows;

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
  @Override
  public String toString() {
    Map<String, BigInteger> counts = new LinkedHashMap<>();
    counts.put("num_input_rows", numInputRows);
    counts.put("num_response_rows", numResponseRows);
    counts.put("num_output_rows", numOutputRows);
    counts.put("num_rejected_rows", numRejectedRows);
    counts.values().removeIf(Objects::isNull);
    return BigqueryUtil.getObjectMapper().valueToTree(counts).toString();
  }
}
