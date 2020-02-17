package org.embulk.output.bigquery_java;

import com.fasterxml.jackson.databind.ObjectMapper;
import javafx.beans.binding.ObjectExpression;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.Optional;

public class BigqueryUtil {
    public static Path[] getIntermediateFiles(PluginTask task) throws IOException {
        String glob = String.format("glob:%s*", task.getPathPrefix());

        FileSystem fs = FileSystems.getDefault();
        PathMatcher matcher = fs.getPathMatcher(glob);

        Path startDir = Paths.get(task.getPathPrefix());
        return Files.walk(startDir).filter(matcher::matches).toArray(Path[]::new);
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
