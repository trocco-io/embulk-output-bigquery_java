package org.embulk.output.bigquery_java.visitor;

import org.embulk.spi.ColumnVisitor;

// visitor
// https://github.com/tomykaira/embulk-output-s3_per_record/blob/master/src/main/java/org/embulk/output/s3_per_record/visitor/JsonMultiColumnVisitor.java
public interface BigqueryColumnVisitor extends ColumnVisitor {
    public byte[] getByteArray();
}
