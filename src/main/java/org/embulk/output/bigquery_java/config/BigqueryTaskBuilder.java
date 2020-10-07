package org.embulk.output.bigquery_java.config;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;

public class BigqueryTaskBuilder {
    private static final String uniqueName = UUID.randomUUID().toString().replace("-", "_");

    public static PluginTask build(PluginTask task) {
        setPathPrefix(task);
        setFileExt(task);
        setTempTable(task);
        setAbortOnError(task);
        return task;
    }

    @VisibleForTesting
    protected static void setPathPrefix(PluginTask task) {
        if (!task.getPathPrefix().isPresent()) {
            try {
                File tmpFile = File.createTempFile("embulk_output_bigquery_java", "");
                task.setPathPrefix(Optional.of(tmpFile.getPath()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @VisibleForTesting
    protected static void setFileExt(PluginTask task) {
        if (!task.getFileExt().isPresent()) {
            if (task.getSourceFormat().equals("CSV")) {
                task.setFileExt(Optional.of(".csv"));
            } else {
                task.setFileExt(Optional.of(".jsonl"));
            }
        }

        if (task.getCompression().equals("GZIP")) {
            String fileExt = task.getFileExt().get() + ".gz";
            task.setFileExt(Optional.of(fileExt));
        }
    }

    @VisibleForTesting
    protected static void setTempTable(PluginTask task) {
        // TODO: support replace_backup
        String[] modeForTempTable = {"replace", "append", "delete_in_advance"};
        if (Arrays.asList(modeForTempTable).contains(task.getMode())) {
            String tempTable = task.getTempTable().orElse(String.format("LOAD_TEMP_%s_%s", uniqueName, task.getTable()));
            task.setTempTable(Optional.of(tempTable));
        }
    }

    @VisibleForTesting
    protected static void setAbortOnError(PluginTask task) {
        if (!task.getAbortOnError().isPresent()) {
            task.setAbortOnError(Optional.of(task.getMaxBadRecords() == 0));
        }
    }
}
