package org.embulk.output.bigquery_java;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.*;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.embulk.output.bigquery_java.config.BigqueryColumnOption;

public class BigqueryUtil {
  private static final String PARTITION_DECORATOR_REGEXP = "\\$.+\\z";

  // The intermediate files the given writers opened (a writer that never wrote has none), as in
  // ruby (`file_writers.map(&:io).map(&:path)`). Globbing path_prefix*file_ext instead, as before,
  // also picked up files left behind by an earlier run with the same path_prefix and loaded their
  // rows again; ruby only globs under skip_file_generation, which this plugin doesn't support.
  public static List<Path> getIntermediateFiles(Collection<BigqueryFileWriter> writers) {
    return writers.stream()
        .map(BigqueryFileWriter::getPath)
        .filter(Objects::nonNull)
        .map(Paths::get)
        .collect(Collectors.toList());
  }

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
