package org.embulk.output.bigquery_java;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.output.bigquery_java.exception.BigqueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigqueryFileWriter {
  // embulk default page size
  static final int BUFFER_SIZE = 1024 * 32;

  private final Logger logger = LoggerFactory.getLogger(BigqueryFileWriter.class);
  private PluginTask task;
  private String compression;
  private OutputStream os;
  // The file under os, kept so close() can release it even when closing os fails; see close().
  private FileOutputStream file;
  private String path;
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

    this.path = path;
    this.file = new FileOutputStream(path);
    OutputStream stream = this.file;
    if (this.compression.equals("GZIP")) {
      stream = new GZIPOutputStream(stream);
    }
    this.os = new BufferedOutputStream(stream, BUFFER_SIZE);

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
      String msg = "embulk-output-bigquery: failed to write an intermediate file";
      logger.error(msg, e);
      throw new BigqueryException(msg, e);
    }
  }

  public long getCount() {
    return this.count;
  }

  // The intermediate file this writer opened, or null if it never wrote a record (ruby's
  // file_writer exposes the same through io.path).
  public String getPath() {
    return this.path;
  }

  public void close() {
    // A writer that never wrote a record never opened its file; don't create an empty one now
    // (it would be picked up as a load file).
    if (this.os == null) {
      return;
    }
    // close() flushes, so no separate flush().
    try {
      this.os.close();
    } catch (IOException e) {
      // Swallowed to match ruby (file_writer#close does `io.close rescue nil`), but logged, since
      // a file truncated here is not always caught later: the row-count check is skipped when
      // abort_on_error is false (max_bad_records > 0) or is_skip_job_result_check is true.
      logger.warn("embulk-output-bigquery: failed to close {}", this.path, e);
    } finally {
      // FilterOutputStream#close() closes what it wraps even when the flush before it fails, but
      // DeflaterOutputStream#close() (compression: GZIP) runs finish(), end() and out.close() in
      // a row with no finally, so a failure writing the gzip trailer would leave the file open.
      // Closing it here is a no-op in every other case. The Deflater's native memory is not
      // reachable from here and is left to its finalizer.
      if (this.file != null) {
        try {
          this.file.close();
        } catch (IOException e) {
          logger.warn("embulk-output-bigquery: failed to close the file under {}", this.path, e);
        }
      }
    }
  }
}
