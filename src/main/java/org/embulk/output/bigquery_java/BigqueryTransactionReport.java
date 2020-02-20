package org.embulk.output.bigquery_java;

import java.math.BigInteger;

public class BigqueryTransactionReport {
    private long numInputRows;
    private long numResponseRows;
    private BigInteger numOutputRows;
    private BigInteger numRejectedRows;

    @Override
    public String toString() {
        return "BigqueryTransactionReport{" +
                "numInputRows=" + numInputRows +
                ", numResponseRows=" + numResponseRows +
                ", numOutputRows=" + numOutputRows +
                ", numRejectedRows=" + numRejectedRows +
                '}';
    }

    public BigqueryTransactionReport(long numInputRows, long numResponseRows, BigInteger numOutputRows, BigInteger numRejectedRows) {
        this.numInputRows = numInputRows;
        this.numResponseRows = numResponseRows;
        this.numOutputRows = numOutputRows;
        this.numRejectedRows = numRejectedRows;
    }

    public BigqueryTransactionReport(long numInputRows){
        this.numInputRows = numInputRows;
    }

    public long getNumInputRows() {
        return numInputRows;
    }

    public long getNumResponseRows() {
        return numResponseRows;
    }

    public BigInteger getNumOutputRows() {
        return numOutputRows;
    }

    public BigInteger getNumRejectedRows() {
        return numRejectedRows;
    }
}
