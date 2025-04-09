package org.embulk.output.bigquery_java.config;

import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;

import java.util.Optional;

public interface BigqueryTimePartitioning extends Task {
    @Config("type")
    public String getType();

    @Config("expiration_ms")
    @ConfigDefault("null")
    public Optional<Long> getExpirationMs();

    @Config("field")
    @ConfigDefault("null")
    public Optional<String> getField();
}
