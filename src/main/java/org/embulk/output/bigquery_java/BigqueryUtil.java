package org.embulk.output.bigquery_java;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.*;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.embulk.output.bigquery_java.config.BigqueryColumnOption;

public class BigqueryUtil {
  private static final String PARTITION_DECORATOR_REGEXP = "\\$.+\\z";

  public static long getPID() {
    String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
    return Long.parseLong(processName.split("@")[0]);
  }

  public static ObjectMapper getObjectMapper() {
    return ObjectMapperInstanceHolder.INSTANCE;
  }

  public static class ObjectMapperInstanceHolder {
    private static final ObjectMapper INSTANCE = new ObjectMapper();
  }

  public static ConcurrentHashMap<Long, BigqueryFileWriter> getFileWriters() {
    return FileWriterHolder.INSTANCE;
  }

  public static class FileWriterHolder {
    private static final ConcurrentHashMap<Long, BigqueryFileWriter> INSTANCE =
        new ConcurrentHashMap<>();
  }

  public static Optional<BigqueryColumnOption> findColumnOption(
      String columnName, List<BigqueryColumnOption> columnOptions) {
    return columnOptions.stream().filter(colOpt -> colOpt.getName().equals(columnName)).findFirst();
  }

  public static String chompPartitionDecorator(String table) {
    return table.replaceAll("\\$.+\\z", "");
  }
}
