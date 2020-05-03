package org.embulk.output.bigquery_java.config;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

import java.util.Optional;

public interface BigqueryTimePartitioning  extends Task {
    @Config("type")
    public String getType();

    @Config("expiration_ms")
    @ConfigDefault("null")
    public Optional<Long> getExpirationMs();

    @Config("field")
    @ConfigDefault("null")
    public Optional<String> getField();
}
