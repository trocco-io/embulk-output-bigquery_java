package org.embulk.output.bigquery_java.config;

import java.util.List;
import java.util.Optional;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;

public interface BigqueryFieldOption extends Task {

  @Config("name")
  String getName();

  @Config("type")
  String getType();

  @Config("mode")
  @ConfigDefault("\"NULLABLE\"")
  String getMode();

  @Config("timestamp_format")
  @ConfigDefault("null")
  Optional<String> getTimestampFormat();

  @Config("timezone")
  @ConfigDefault("\"UTC\"")
  String getTimezone();

  @Config("description")
  @ConfigDefault("null")
  Optional<String> getDescription();

  @Config("fields")
  @ConfigDefault("null")
  Optional<List<BigqueryFieldOption>> getFields();
}
