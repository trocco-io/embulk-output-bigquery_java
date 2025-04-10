package org.embulk.output.bigquery_java.config;

import java.util.List;
import java.util.Optional;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;

public interface BigqueryClustering extends Task {
  @Config("fields")
  @ConfigDefault("null")
  public Optional<List<String>> getFields();
}
