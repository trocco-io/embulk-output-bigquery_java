package org.embulk.output.bigquery_java;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import org.embulk.output.bigquery_java.exception.BigqueryException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.slf4j.Logger;

public class TestBigqueryFileWriter {
  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  private BigqueryFileWriter openWriterWithClosedFile() throws IOException {
    BigqueryFileWriter writer = new BigqueryFileWriter();
    writer.setCompression("NONE");
    OutputStream stream = writer.open(testFolder.newFile().getPath());
    stream.close();
    return writer;
  }

  private static void setField(BigqueryFileWriter writer, String name, Object value)
      throws ReflectiveOperationException {
    field(name).set(writer, value);
  }

  private static Object getField(BigqueryFileWriter writer, String name)
      throws ReflectiveOperationException {
    return field(name).get(writer);
  }

  private static java.lang.reflect.Field field(String name) throws ReflectiveOperationException {
    java.lang.reflect.Field field = BigqueryFileWriter.class.getDeclaredField(name);
    field.setAccessible(true);
    return field;
  }

  private static Logger mockLogger(BigqueryFileWriter writer) throws ReflectiveOperationException {
    Logger mockLogger = Mockito.mock(Logger.class);
    setField(writer, "logger", mockLogger);
    return mockLogger;
  }

  @Test
  public void testWriteThrowsInsteadOfSwallowingIOException()
      throws IOException, ReflectiveOperationException {
    BigqueryFileWriter writer = openWriterWithClosedFile();
    Logger mockLogger = mockLogger(writer);

    // The plugin's own exception type, with the context message, so that Embulk's error output
    // says more than "java.lang.RuntimeException: java.io.IOException: ...".
    BigqueryException thrown =
        assertThrows(
            BigqueryException.class, () -> writer.write(new byte[BigqueryFileWriter.BUFFER_SIZE]));

    assertTrue(thrown.getMessage().contains("failed to write an intermediate file"));
    assertTrue(thrown.getCause() instanceof IOException);
    assertEquals(0, writer.getCount());
    Mockito.verify(mockLogger)
        .error(
            Mockito.eq("embulk-output-bigquery: failed to write an intermediate file"),
            Mockito.any(IOException.class));
  }

  // Like ruby's `io.close rescue nil`, no exception; unlike ruby, the failure is logged (WARN),
  // since a file truncated here is not always caught by a later check.
  @Test
  public void testCloseSwallowsIOExceptionWithAWarning()
      throws IOException, ReflectiveOperationException {
    BigqueryFileWriter writer = openWriterWithClosedFile();
    Logger mockLogger = mockLogger(writer);
    writer.write(new byte[BigqueryFileWriter.BUFFER_SIZE - 1]);

    writer.close();

    assertEquals(1, writer.getCount());
    Mockito.verify(mockLogger)
        .warn(Mockito.anyString(), Mockito.any(), Mockito.any(IOException.class));
    Mockito.verifyNoMoreInteractions(mockLogger);
  }

  // With compression: GZIP, close() writes the gzip trailer first, and DeflaterOutputStream
  // skips out.close() when that write fails (no finally there), so the file descriptor would
  // leak. close() releases the file itself for that case.
  @Test
  public void testCloseReleasesTheFileWhenClosingTheStreamFails()
      throws IOException, ReflectiveOperationException {
    BigqueryFileWriter writer = new BigqueryFileWriter();
    writer.setCompression("GZIP");
    writer.open(testFolder.newFile().getPath());
    mockLogger(writer);
    FileOutputStream file = (FileOutputStream) getField(writer, "file");
    // Stands in for the gzip trailer failing on a full disk: what close() sees is a stream whose
    // close() throws without having closed the file under it.
    setField(
        writer,
        "os",
        new OutputStream() {
          @Override
          public void write(int b) {}

          @Override
          public void close() throws IOException {
            throw new IOException("No space left on device");
          }
        });

    writer.close();

    // getChannel() would hand out a fresh channel that reports open even for a closed stream.
    assertFalse("the file was left open", file.getFD().valid());
  }

  // open() creates the file before the wrappers over it, and with compression: GZIP the
  // GZIPOutputStream constructor writes the gzip header and can fail, leaving the file open with
  // no stream over it. close() releases it even then.
  @Test
  public void testCloseReleasesTheFileWhenOpenFailedAfterCreatingIt()
      throws IOException, ReflectiveOperationException {
    BigqueryFileWriter writer = new BigqueryFileWriter();
    writer.setCompression("GZIP");
    writer.open(testFolder.newFile().getPath());
    FileOutputStream file = (FileOutputStream) getField(writer, "file");
    // open() as it is left when the header write throws: the file is there, os is not.
    setField(writer, "os", null);

    writer.close();

    assertFalse("the file was left open", file.getFD().valid());
  }

  // A failed flush() must not leak the file: close() has to be called on the stream regardless.
  @Test
  public void testCloseClosesStreamEvenIfFlushFails()
      throws IOException, ReflectiveOperationException {
    BigqueryFileWriter writer = new BigqueryFileWriter();
    mockLogger(writer);
    AtomicBoolean closed = new AtomicBoolean(false);
    OutputStream failingFlush =
        new OutputStream() {
          @Override
          public void write(int b) {}

          @Override
          public void flush() throws IOException {
            throw new IOException("No space left on device");
          }

          @Override
          public void close() {
            closed.set(true);
          }
        };
    setField(writer, "os", failingFlush);

    writer.close();

    assertTrue(closed.get());
  }

  // A writer that never wrote a record (an empty page, or a run that failed before any record)
  // has no file; close() must not open one just to close it, since an empty intermediate file
  // would be picked up as a load file. No task is set here, so opening a file would fail with a
  // NullPointerException rather than silently create one.
  @Test
  public void testCloseWithoutWriteDoesNotOpenAFile() {
    BigqueryFileWriter writer = new BigqueryFileWriter();
    writer.setCompression("NONE");

    writer.close();
  }
}
