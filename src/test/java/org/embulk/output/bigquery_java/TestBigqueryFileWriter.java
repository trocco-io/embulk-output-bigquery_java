package org.embulk.output.bigquery_java;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

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
    java.lang.reflect.Field field = BigqueryFileWriter.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(writer, value);
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

  @Test
  public void testCloseSwallowsIOException() throws IOException, ReflectiveOperationException {
    BigqueryFileWriter writer = openWriterWithClosedFile();
    Logger mockLogger = mockLogger(writer);
    writer.write(new byte[BigqueryFileWriter.BUFFER_SIZE - 1]);

    writer.close();

    assertEquals(1, writer.getCount());
    Mockito.verify(mockLogger).info(Mockito.contains("Stream Closed"));
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
}
