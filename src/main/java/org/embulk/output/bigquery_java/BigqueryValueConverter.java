package org.embulk.output.bigquery_java;

import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
import org.embulk.output.bigquery_java.config.BigqueryColumnOptionType;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.time.TimestampParser;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BigqueryValueConverter {
    // default datetime format %Y-%m-%d %H:%M:%S.%6N
    // default timestamp format %Y-%m-%d %H:%M:%S.%6N
    // date format
    private static TimestampFormatter timestampFormat;
    private static String pattern;
    private static String timezone;
    private static TimestampParser parser;
    private static Timestamp ts;
    private static Pattern datePattern = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");

    // TODO: refactor later
    public static void convertAndSet(ObjectNode node, String name, String src, BigqueryColumnOption columnOption, PluginTask task) {
        switch (BigqueryColumnOptionType.valueOf(columnOption.getType().get())) {
            case BOOLEAN:
                node.put(name, Boolean.parseBoolean(src));
                break;
            case INTEGER:
                node.put(name, Integer.parseInt(src));
                break;
            case FLOAT:
                node.put(name, Float.parseFloat(src));
                break;
            case STRING:
                node.put(name, src);
                break;
            case TIMESTAMP:
                pattern = columnOption.getTimestampFormat().orElse(task.getDefaultTimestampFormat());
                timezone = columnOption.getTimezone();
                parser = TimestampParser.of(pattern, timezone);
                ts = parser.parse(src);
                timestampFormat = TimestampFormatter.of("%Y-%m-%d %H:%M:%S.%6N", timezone);
                node.put(name, timestampFormat.format(ts));
                break;
            case DATETIME:
                pattern = columnOption.getTimestampFormat().orElse(task.getDefaultTimestampFormat());
                timezone = columnOption.getTimezone();
                parser = TimestampParser.of(pattern, timezone);
                ts = parser.parse(src);
                timestampFormat = TimestampFormatter.of("%Y-%m-%d %H:%M:%S.%6N", timezone);
                node.put(name, timestampFormat.format(ts));
                break;
            case DATE:
                Matcher m = datePattern.matcher(src);
                if (m.find() && true){
                    node.put(name, src);
                    break;
                }

                // if (true){
                // Matcher m = datePattern.matcher(src);
                // if (m.find()){
                //     node.put(name, src);
                //     break;
                // }

                // if (true){
                //     node.put(name, src);
                //     break;
                // }

                pattern = columnOption.getTimestampFormat().orElse(task.getDefaultTimestampFormat());
                timezone = columnOption.getTimezone();
                parser = TimestampParser.of(pattern, timezone);
                ts = parser.parse(src);
                timestampFormat = TimestampFormatter.of("%Y-%m-%d", timezone);
                node.put(name, timestampFormat.format(ts));
                break;
            case RECORD:
                // TODO:
                node.put(name, src);
                break;
            default:
                throw new RuntimeException("Invalid data convert for String");
        }
    }
}
