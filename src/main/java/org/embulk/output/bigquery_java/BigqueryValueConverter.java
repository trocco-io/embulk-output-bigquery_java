package org.embulk.output.bigquery_java;


import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.time.TimestampParser;


// provide function to concert value to specific type
// e.g. string to date
public class BigqueryValueConverter {
    // default datetime format %Y-%m-%d %H:%M:%S.%6N
    // default timestamp format %Y-%m-%d %H:%M:%S.%6N
    // date format
    private static TimestampFormatter timestampFormat;

    private static String pattern;
    private static String timezone;
    private static TimestampParser parser;
    private static Timestamp ts;

    // public static Value convert(String src, BigqueryColumnOption columnOption){
    //     switch (BigqueryColumnOptionType.valueOf(columnOption.getType())) {
    //         case BOOLEAN:
    //             value = ValueFactory.newBoolean(Boolean.parseBoolean(src));
    //             break;
    //         case INTEGER:
    //             value = ValueFactory.newInteger(Integer.parseInt(src));
    //             break;
    //         case FLOAT:
    //             value = ValueFactory.newFloat(Float.parseFloat(src));
    //             break;
    //         case STRING:
    //             value = ValueFactory.newString(src);
    //             break;
    //         case TIMESTAMP:
    //             pattern = columnOption.getTimestampFormat();
    //             timezone = columnOption.getTimezone();
    //             parser = TimestampParser.of(pattern, timezone);
    //             ts = parser.parse(src);
    //             timestampFormat = TimestampFormatter.of("\t%Y-%m-%d %H:%M:%S.%6N",timezone);
    //             value = ValueFactory.newString(timestampFormat.format(ts));
    //             break;
    //         case DATETIME:
    //             pattern = columnOption.getTimestampFormat();
    //             timezone = columnOption.getTimezone();
    //             parser = TimestampParser.of(pattern, timezone);
    //             ts = parser.parse(src);
    //             timestampFormat = TimestampFormatter.of("\t%Y-%m-%d %H:%M:%S.%6N",timezone);
    //             value = ValueFactory.newString(timestampFormat.format(ts));
    //             break;
    //         case DATE:
    //             pattern = columnOption.getTimestampFormat();
    //             timezone = columnOption.getTimezone();
    //             parser = TimestampParser.of(pattern, timezone);
    //             ts = parser.parse(src);
    //             timestampFormat = TimestampFormatter.of("%Y-%m-%d",timezone);
    //             value = ValueFactory.newString(timestampFormat.format(ts));
    //             break;
    //         case RECORD:
    //             // TODO:
    //             value = ValueFactory.newString(src);
    //             break;
    //         default:
    //             throw new RuntimeException("Invalid data convert for String");
    //     }
    //     return value;
    // }

    // TODO: refactor later
    public static void convertAndSet(ObjectNode node, String name, String src, BigqueryColumnOption columnOption, PluginTask task){
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
                timestampFormat = TimestampFormatter.of("%Y-%m-%d %H:%M:%S.%6N",timezone);
                node.put(name, timestampFormat.format(ts));
                break;
            case DATETIME:
                pattern = columnOption.getTimestampFormat().orElse(task.getDefaultTimestampFormat());
                timezone = columnOption.getTimezone();
                parser = TimestampParser.of(pattern, timezone);
                ts = parser.parse(src);
                timestampFormat = TimestampFormatter.of("%Y-%m-%d %H:%M:%S.%6N",timezone);
                node.put(name, timestampFormat.format(ts));
                break;
            case DATE:
                pattern = columnOption.getTimestampFormat().orElse(task.getDefaultTimestampFormat());
                timezone = columnOption.getTimezone();
                parser = TimestampParser.of(pattern, timezone);
                ts = parser.parse(src);
                timestampFormat = TimestampFormatter.of("%Y-%m-%d",timezone);
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

    // public static Value convert(long src, BigqueryColumnOption columnOption){
    //     String srcStr = String.valueOf(src);
    //     switch (BigqueryColumnOptionType.valueOf(columnOption.getType())) {
    //         case BOOLEAN:
    //             value = ValueFactory.newBoolean(Boolean.parseBoolean(srcStr));
    //             break;
    //         case INTEGER:
    //             value = ValueFactory.newInteger(Integer.parseInt(srcStr));
    //             break;
    //         case FLOAT:
    //             value = ValueFactory.newFloat(Float.parseFloat(srcStr));
    //             break;
    //         case STRING:
    //             value = ValueFactory.newString(srcStr);
    //             break;
    //         case TIMESTAMP:
    //             pattern = columnOption.getTimestampFormat().get();
    //             timezone = columnOption.getTimezone();
    //             ts = Timestamp.ofEpochMilli(src);
    //             value = ValueFactory.newString(timestampFormat.format(ts));
    //             break;
    //         default:
    //             throw new RuntimeException("Invalid data convert for long");
    //     }
    //     return value;
    // }
}
