package org.embulk.output.bigquery_java.visitor;

import org.embulk.spi.ColumnVisitor;

public interface BigqueryColumnVisitor extends ColumnVisitor {
    public byte[] getByteArray();
}
