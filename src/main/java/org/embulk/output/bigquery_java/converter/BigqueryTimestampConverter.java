package org.embulk.output.bigquery_java.converter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
import org.embulk.output.bigquery_java.config.BigqueryColumnOptionType;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.output.bigquery_java.exception.BigqueryNotSupportedTypeException;
import org.embulk.util.timestamp.TimestampFormatter;

public class BigqueryTimestampConverter {
  @SuppressWarnings("deprecation") // The use of org.embulk.spi.time.Timestamp
  public static void convertAndSet(
      ObjectNode node,
      String name,
      org.embulk.spi.time.Timestamp src,
      BigqueryColumnOptionType bigqueryColumnOptionType,
      BigqueryColumnOption columnOption,
      PluginTask task) {
    TimestampFormatter timestampFormat;
    String timezone;
    switch (bigqueryColumnOptionType) {
      case INTEGER:
        node.put(name, src.toEpochMilli());
        break;
      case FLOAT:
        node.put(name, Double.valueOf(src.toEpochMilli()));
        break;
      case STRING:
        String format = columnOption.getTimestampFormat().orElse(task.getDefaultTimestampFormat());
        timezone = columnOption.getTimezone();
        timestampFormat =
            TimestampFormatter.builder(format, true).setDefaultZoneFromString(timezone).build();
        node.put(name, timestampFormat.format(src.getInstant()));
        break;
      case TIMESTAMP:
        if (src == null) {
          node.putNull(name);
        } else {
          timestampFormat =
              TimestampFormatter.builder("%Y-%m-%d %H:%M:%S.%6N %:z", true)
                  .setDefaultZoneFromString("UTC")
                  .build();
          node.put(name, timestampFormat.format(src.getInstant()));
        }
        break;
      case DATETIME:
        if (src == null) {
          node.putNull(name);
        } else {
          timezone = columnOption.getTimezone();
          timestampFormat =
              TimestampFormatter.builder("%Y-%m-%d %H:%M:%S.%6N", true)
                  .setDefaultZoneFromString(timezone)
                  .build();
          node.put(name, timestampFormat.format(src.getInstant()));
        }
        break;
      case DATE:
        if (src == null) {
          node.putNull(name);
        } else {
          timezone = columnOption.getTimezone();
          timestampFormat =
              TimestampFormatter.builder("%Y-%m-%d", true)
                  .setDefaultZoneFromString(timezone)
                  .build();
          node.put(name, timestampFormat.format(src.getInstant()));
        }
        break;
      default:
        throw new BigqueryNotSupportedTypeException("Invalid data convert for timestamp");
    }
  }
}
