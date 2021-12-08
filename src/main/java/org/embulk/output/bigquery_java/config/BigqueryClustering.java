package org.embulk.output.bigquery_java.config;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

import java.util.List;
import java.util.Optional;

public interface BigqueryClustering extends Task {
    @Config("fields")
    @ConfigDefault("null")
    public Optional<List<String>> getFields();
}
