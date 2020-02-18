package org.embulk.output.bigquery_java;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Optional;


public class BigqueryUtil {
    public static List<Path> getIntermediateFiles(PluginTask task) throws IOException {
        String glob = String.format("glob:%s*", task.getPathPrefix().get());

        FileSystem fs = FileSystems.getDefault();
        PathMatcher matcher = fs.getPathMatcher(glob);

        Path startDir = Paths.get(task.getPathPrefix().get());
        return Files.walk(startDir).filter(matcher::matches).collect(Collectors.toList());
    }

    public static long getPID() {
        String processName =
                java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        return Long.parseLong(processName.split("@")[0]);
    }

    public static ObjectMapper getObjectMapper(){
        return ObjectMapperInstanceHolder.INSTANCE;
    }

    public static class ObjectMapperInstanceHolder {
        private static final ObjectMapper INSTANCE = new ObjectMapper();
    }

    public static Optional<BigqueryColumnOption> findColumnOption(String columnName, List<BigqueryColumnOption> columnOptions) {
        return columnOptions.stream()
                .filter(colOpt-> colOpt.getName().equals(columnName))
                .findFirst();
    }
}
