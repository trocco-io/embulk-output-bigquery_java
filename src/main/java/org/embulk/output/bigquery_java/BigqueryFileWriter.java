package org.embulk.output.bigquery_java;

import org.embulk.spi.Schema;

import java.io.*;
import java.util.zip.GZIPOutputStream;

// provide function to write record to file
// jsonl and csv should be handled this class

public class BigqueryFileWriter {
    private PluginTask task;
    private String compression;
    private OutputStream os;

    public BigqueryFileWriter(PluginTask task){
        this.task = task;
        this.compression = this.task.getCompression();
    }

    public OutputStream open(String path) throws IOException {
        this.os = new FileOutputStream(path, true);
        if (this.compression.equals("GZIP")){
            this.os = new GZIPOutputStream(this.os);
        }
        return this.os;
    }

    public OutputStream outputStream() throws IOException {
        if (this.os != null) {
            return this.os;
        }
        // TODO: pid, thread id format config
        String path = String.format("%s.%d.%d%s",
                this.task.getPathPrefix(),
                BigqueryUtil.getPID(),
                Thread.currentThread().getId(),
                this.task.getFileExt());
        return open(path);
    }
}

