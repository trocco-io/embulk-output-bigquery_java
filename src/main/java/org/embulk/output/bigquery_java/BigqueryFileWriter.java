package org.embulk.output.bigquery_java;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigqueryFileWriter {
  // embulk default page size
  public static final int BUFFER_SIZE = 1024 * 32;

  private final Logger logger = LoggerFactory.getLogger(BigqueryFileWriter.class);
  private PluginTask task;
  private String compression;
  private OutputStream os;
  private long count = 0;

  public BigqueryFileWriter(PluginTask task) {
    this.task = task;
    this.compression = this.task.getCompression();
  }

  public BigqueryFileWriter() {}

  public void setTask(PluginTask task) {
    this.task = task;
  }

  public void setCompression(String compression) {
    this.compression = compression;
  }

  public OutputStream open(String path) throws IOException {
    logger.info("embulk-output-bigquery: create {}", path);

    this.os = new FileOutputStream(path);
    if (this.compression.equals("GZIP")) {
      this.os = new GZIPOutputStream(this.os);
    }
    this.os = new BufferedOutputStream(this.os, BUFFER_SIZE);

    return this.os;
  }

  public OutputStream outputStream() throws IOException {
    if (this.os != null) {
      return this.os;
    }
    // TODO: pid, thread id format config
    String path =
        String.format(
            "%s.%d.%d%s",
            this.task.getPathPrefix().get(),
            BigqueryUtil.getPID(),
            Thread.currentThread().getId(),
            this.task.getFileExt().get());
    return open(path);
  }

  public void write(byte[] bytes) {
    try {
      outputStream().write(bytes);
      this.count++;
    } catch (IOException e) {
      logger.error("embulk-output-bigquery: failed to write an intermediate file", e);
      throw new RuntimeException(e);
    }
  }

  public long getCount() {
    return this.count;
  }

  public void close() {
    // Swallowed to match ruby (file_writer#close does `io.close rescue nil`). A file truncated by
    // a failed flush/close is still caught later, by the load job error or the row-count check.
    try {
      this.outputStream().flush();
      this.outputStream().close();
    } catch (IOException e) {
      logger.info(e.getMessage());
    }
  }
}
