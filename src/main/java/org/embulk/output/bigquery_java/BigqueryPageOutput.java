package org.embulk.output.bigquery_java;

import java.util.Collections;
import org.embulk.config.TaskReport;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.output.bigquery_java.visitor.BigqueryColumnVisitor;
import org.embulk.output.bigquery_java.visitor.JsonColumnVisitor;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.util.config.ConfigMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigqueryPageOutput implements TransactionalPageOutput {
  private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();
  private final Logger logger = LoggerFactory.getLogger(BigqueryPageOutput.class);
  private PageReader pageReader;
  private final Schema schema;
  private PluginTask task;

  @SuppressWarnings("deprecation") // The use of new PageReader(schema)
  public BigqueryPageOutput(PluginTask task, Schema schema) {
    this.task = task;
    this.schema = schema;
    this.pageReader = new PageReader(schema);
  }

  @Override
  public void add(Page page) {
    pageReader.setPage(page);
    BigqueryThreadLocalFileWriter.setFileWriter(this.task);
    try {
      while (pageReader.nextRecord()) {
        BigqueryColumnVisitor visitor =
            new JsonColumnVisitor(
                this.task,
                pageReader,
                this.task.getColumnOptions().orElse(Collections.emptyList()));
        pageReader.getSchema().getColumns().forEach(col -> col.visit(visitor));
        BigqueryThreadLocalFileWriter.write(visitor.getByteArray());
      }
    } catch (Exception e) {
      logger.info(e.getMessage());
    }
  }

  @Override
  public void finish() {
    close();
  }

  @Override
  public void close() {
    if (pageReader != null) {
      pageReader.close();
      pageReader = null;
    }
  }

  @Override
  public void abort() {}

  @Override
  public TaskReport commit() {
    return CONFIG_MAPPER_FACTORY.newTaskReport();
  }
}
