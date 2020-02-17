package org.embulk.output.bigquery_java;

public class BigqueryTransactionReport {
    private long numInputRows;
    private long numResponseRows;
    private long numOutputRows;
    private long numRejectedRows;

    public BigqueryTransactionReport(long numInputRows, long numResponseRows, long numOutputRows, long numRejectedRows) {
        this.numInputRows = numInputRows;
        this.numResponseRows = numResponseRows;
        this.numOutputRows = numOutputRows;
        this.numRejectedRows = numRejectedRows;
    }

    public long getNumInputRows() {
        return numInputRows;
    }

    public long getNumResponseRows() {
        return numResponseRows;
    }

    public long getNumOutputRows() {
        return numOutputRows;
    }

    public long getNumRejectedRows() {
        return numRejectedRows;
    }
}
