package org.embulk.output.bigquery_java;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
import org.embulk.output.bigquery_java.config.PluginTask;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Optional;


public class BigqueryUtil {
    public static List<Path> getIntermediateFiles(PluginTask task) throws IOException {
        Path startDir;
        String pathPrefix =  task.getPathPrefix().get();
        String fileExt = task.getFileExt().get();
        File pathPrefixFile = new File(pathPrefix);
        String glob = String.format("glob:%s*%s", pathPrefix, fileExt);

        if (pathPrefixFile.isDirectory()){
            startDir = pathPrefixFile.toPath();
        }else{
            startDir = pathPrefixFile.toPath().getParent();
        }

        FileSystem fs = FileSystems.getDefault();
        PathMatcher matcher = fs.getPathMatcher(glob);
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
