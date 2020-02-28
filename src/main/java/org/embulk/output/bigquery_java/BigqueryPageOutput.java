package org.embulk.output.bigquery_java;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import org.embulk.config.TaskReport;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.output.bigquery_java.visitor.JsonColumnVisitor;
import org.embulk.output.bigquery_java.visitor.BigqueryColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BigqueryPageOutput implements TransactionalPageOutput {
    private final Logger logger = LoggerFactory.getLogger(BigqueryPageOutput.class);
    private PageReader pageReader;
    private final Schema schema;
    private PluginTask task;
    private ConcurrentHashMap<Long, BigqueryFileWriter> writers;
    private BigqueryFileWriter fileWriter;

    public BigqueryPageOutput(PluginTask task, Schema schema) {
        this.task = task;
        this.schema = schema;
        this.pageReader = new PageReader(schema);
    }

    @Override
    public void add(Page page) {
        pageReader.setPage(page);
        fileWriter = getFileWriter(this.task);
        try {
            while (pageReader.nextRecord()) {
                BigqueryColumnVisitor visitor = new JsonColumnVisitor(this.task,
                        pageReader, this.task.getColumnOptions().orElse(Collections.emptyList()));
                pageReader.getSchema().getColumns().forEach(col -> col.visit(visitor));
                fileWriter.write(visitor.getByteArray());
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
    public void abort() {
    }

    @Override
    public TaskReport commit() {
        return Exec.newTaskReport();
    }

    private BigqueryFileWriter getFileWriter(PluginTask task){
        Long key = Thread.currentThread().getId();
        writers = BigqueryUtil.getFileWriters();
        return writers.computeIfAbsent(key, (k)-> new BigqueryFileWriter(task));
    }
}
