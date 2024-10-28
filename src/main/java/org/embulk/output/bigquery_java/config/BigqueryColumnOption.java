package org.embulk.output.bigquery_java.config;

import java.util.Optional;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;

public interface BigqueryColumnOption extends Task {
  // name: column name
  // type: BigQuery type such as BOOLEAN, INTEGER, FLOAT, STRING, TIMESTAMP, DATETIME, DATE, and
  // RECORD. See belows for supported conversion type.
  // boolean: BOOLEAN, STRING (default: BOOLEAN)
  // long: BOOLEAN, INTEGER, FLOAT, STRING, TIMESTAMP (default: INTEGER)
  // double: INTEGER, FLOAT, STRING, TIMESTAMP (default: FLOAT)
  // string: BOOLEAN, INTEGER, FLOAT, STRING, TIMESTAMP, DATETIME, DATE, RECORD (default: STRING)
  // timestamp: INTEGER, FLOAT, STRING, TIMESTAMP, DATETIME, DATE (default: TIMESTAMP)
  // json: STRING, RECORD (default: STRING)
  // mode: BigQuery mode such as NULLABLE, REQUIRED, and REPEATED (string, default: NULLABLE)
  // fields: Describes the nested schema fields if the type property is set to RECORD. Please note
  // that this is required for RECORD column.
  // timestamp_format: timestamp format to convert into/from timestamp (string, default is
  // default_timestamp_format)

  @Config("name")
  public String getName();

  @Config("type")
  @ConfigDefault("null")
  public Optional<String> getType();

  @Config("mode")
  @ConfigDefault("\"NULLABLE\"")
  public String getMode();

  // TODO: Task builder should set value
  @Config("timestamp_format")
  @ConfigDefault("null")
  public Optional<String> getTimestampFormat();

  @Config("timezone")
  @ConfigDefault("\"UTC\"")
  public String getTimezone();

  @Config("description")
  @ConfigDefault("null")
  public Optional<String> getDescription();

  @Config("scale")
  @ConfigDefault("9")
  public int getScale();

  // TODO: fields
}
