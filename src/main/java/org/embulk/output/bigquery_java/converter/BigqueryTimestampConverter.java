package org.embulk.output.bigquery_java.converter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
import org.embulk.output.bigquery_java.config.BigqueryColumnOptionType;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.output.bigquery_java.exception.BigqueryNotSupportedTypeException;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.time.TimestampParser;

public class BigqueryTimestampConverter {
    private static TimestampFormatter timestampFormat;
    private static String timezone;

    public static void convertAndSet(ObjectNode node, String name, Timestamp src, BigqueryColumnOptionType bigqueryColumnOptionType, BigqueryColumnOption columnOption, PluginTask task) {
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
                timestampFormat = TimestampFormatter.of(format, timezone);
                node.put(name, timestampFormat.format(src));
                break;
            case TIMESTAMP:
                if (src == null) {
                    node.putNull(name);
                } else {
                    timestampFormat = TimestampFormatter.of("%Y-%m-%d %H:%M:%S.%6N %:z", "UTC");
                    node.put(name, timestampFormat.format(src));
                }
                break;
            case DATETIME:
                if (src == null) {
                    node.putNull(name);
                } else {
                    timestampFormat = TimestampFormatter.of("%Y-%m-%d %H:%M:%S.%6N", "UTC");
                    node.put(name, timestampFormat.format(src));
                }
                break;
            case DATE:
                if (src == null) {
                    node.putNull(name);
                } else {
                    timestampFormat = TimestampFormatter.of("%Y-%m-%d", "UTC");
                    node.put(name, timestampFormat.format(src));
                }
                break;
            default:
                throw new BigqueryNotSupportedTypeException("Invalid data convert for timestamp");
        }
    }
}
