package org.embulk.output.bigquery_java;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
import org.embulk.output.bigquery_java.config.PluginTask;

public class BigqueryUtil {
  private static final String PARTITION_DECORATOR_REGEXP = "\\$.+\\z";

  public static List<Path> getIntermediateFiles(PluginTask task) {
    Path startDir;
    String pathPrefix = task.getPathPrefix().get();
    String fileExt = task.getFileExt().get();
    File pathPrefixFile = new File(pathPrefix);
    String glob = String.format("glob:%s*%s", pathPrefix, fileExt);
    List<Path> r = new ArrayList<Path>() {};

    if (pathPrefixFile.isDirectory()) {
      startDir = pathPrefixFile.toPath();
    } else {
      startDir = pathPrefixFile.toPath().getParent();
    }

    File[] files = startDir.toFile().listFiles();
    if (files == null) {
      return r;
    }
    PathMatcher m = FileSystems.getDefault().getPathMatcher(glob);
    for (File file : files) {
      Path path = file.toPath();
      if (m.matches(path)) {
        r.add(path);
      }
    }
    return r;
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
