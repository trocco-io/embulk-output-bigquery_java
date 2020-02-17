package org.embulk.output.bigquery_java;

import org.embulk.config.TaskReport;
import org.embulk.output.bigquery_java.visitor.JacksonJsonColumnVisitor;
import org.embulk.output.bigquery_java.visitor.JsonColumnVisitor;
import org.embulk.output.bigquery_java.visitor.BigqueryColumnVisitor;
import org.embulk.spi.*;

import java.io.OutputStream;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigqueryPageOutput implements TransactionalPageOutput {
    private final Logger logger = LoggerFactory.getLogger(BigqueryPageOutput.class);
    private PageReader pageReader;
    private final Schema schema;
    private PluginTask task;
    private OutputStream os;

    public BigqueryPageOutput(PluginTask task, Schema schema) {
        this.task = task;
        this.schema = schema;
        this.pageReader = new PageReader(schema);
    }

    @Override
    public void add(Page page) {
        pageReader.setPage(page);
        BigqueryFileWriter writer = new BigqueryFileWriter(this.task);
        try {
            this.os = writer.outputStream();
            while (pageReader.nextRecord()) {
                BigqueryColumnVisitor visitor = new JsonColumnVisitor(pageReader,
                        this.task.getColumnOptions().or(Collections.emptyList()));
                pageReader.getSchema().getColumns().forEach(col-> col.visit(visitor));
                os.write(visitor.getByteArray());
            }
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
    }

    @Override
    public void finish()
    {
        close();
    }

    @Override
    public void close()
    {
        if (os != null){
            try {
                os.flush();
                os.close();
            }catch (Exception e){
                logger.info(e.getMessage());
            }
        }
        if (pageReader != null) {
            pageReader.close();
            pageReader = null;
        }
    }

    @Override
    public void abort() {
    }

    @Override
    public TaskReport commit()
    {
        return Exec.newTaskReport();
    }
}
