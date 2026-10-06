package org.embulk.output.bigquery_java.converter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
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
        node.put(name, src.getEpochSecond());
        break;
      case FLOAT:
        node.put(
            name,
            BigDecimal.valueOf(src.getEpochSecond())
                .add(BigDecimal.valueOf(src.getNano(), 9))
                .doubleValue());
        break;
      case STRING:
        String format =
            BigqueryTimestampFormatters.resolveTimestampFormat(
                columnOption.getTimestampFormat(), task);
        timezone = BigqueryTimestampFormatters.resolveTimezone(columnOption.getTimezone(), task);
        timestampFormat = BigqueryTimestampFormatters.get(format, timezone);
        node.put(name, timestampFormat.format(src.getInstant()));
        break;
      case TIMESTAMP:
        if (src == null) {
          node.putNull(name);
        } else {
          timestampFormat = BigqueryTimestampFormatters.get("%Y-%m-%d %H:%M:%S.%6N %:z", "UTC");
          node.put(name, timestampFormat.format(src.getInstant()));
        }
        break;
      case DATETIME:
        if (src == null) {
          node.putNull(name);
        } else {
          timezone = BigqueryTimestampFormatters.resolveTimezone(columnOption.getTimezone(), task);
          timestampFormat = BigqueryTimestampFormatters.get("%Y-%m-%d %H:%M:%S.%6N", timezone);
          node.put(name, timestampFormat.format(src.getInstant()));
        }
        break;
      case DATE:
        if (src == null) {
          node.putNull(name);
        } else {
          timezone = BigqueryTimestampFormatters.resolveTimezone(columnOption.getTimezone(), task);
          timestampFormat = BigqueryTimestampFormatters.get("%Y-%m-%d", timezone);
          node.put(name, timestampFormat.format(src.getInstant()));
        }
        break;
      default:
        throw new BigqueryNotSupportedTypeException("Invalid data convert for timestamp");
    }
  }
}
