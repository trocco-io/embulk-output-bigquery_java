package org.embulk.output.bigquery_java;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
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

  private static Logger mockLogger(BigqueryFileWriter writer) throws ReflectiveOperationException {
    Logger mockLogger = Mockito.mock(Logger.class);
    java.lang.reflect.Field loggerField = BigqueryFileWriter.class.getDeclaredField("logger");
    loggerField.setAccessible(true);
    loggerField.set(writer, mockLogger);
    return mockLogger;
  }

  @Test
  public void testWriteThrowsInsteadOfSwallowingIOException()
      throws IOException, ReflectiveOperationException {
    BigqueryFileWriter writer = openWriterWithClosedFile();
    Logger mockLogger = mockLogger(writer);

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class, () -> writer.write(new byte[BigqueryFileWriter.BUFFER_SIZE]));

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
}
