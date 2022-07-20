package org.embulk.output.bigquery_java.converter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
import org.embulk.output.bigquery_java.config.BigqueryColumnOptionType;
import org.embulk.output.bigquery_java.exception.BigqueryNotSupportedTypeException;
import org.embulk.output.bigquery_java.exception.BigqueryTypeCastException;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.time.TimestampParseException;
import org.embulk.spi.time.TimestampParser;

import java.math.BigDecimal;

public class BigqueryStringConverter {

    public static void convertAndSet(ObjectNode node, String name, String src, BigqueryColumnOptionType bigqueryColumnOptionType, BigqueryColumnOption columnOption) {
        TimestampFormatter timestampFormat;
        String pattern;
        String timezone;
        TimestampParser parser;
        Timestamp ts;
        switch (bigqueryColumnOptionType) {
            case BOOLEAN:
                if (src == null) {
                    node.putNull(name);
                } else if (src.toLowerCase().equals("true")) {
                    node.put(name, true);
                } else if (src.toLowerCase().equals("false")) {
                    node.put(name, false);
                } else {
                    throw new BigqueryTypeCastException(String.format("%s cannot be converted to BOOLEAN", src));
                }
                break;
            case INTEGER:
                int intVal;
                try {
                    intVal = Integer.parseInt(src);
                } catch (NumberFormatException e) {
                    throw new BigqueryTypeCastException(String.format("%s cannot be converted to INTEGER", src));
                }
                node.put(name, intVal);
                break;
            case FLOAT:
                float floatVal;
                try {
                    floatVal = Float.parseFloat(src);
                } catch (NumberFormatException e) {
                    throw new BigqueryTypeCastException(String.format("%s cannot be converted to FLOAT", src));
                }
                node.put(name, floatVal);
                break;
            case STRING:
                node.put(name, src);
                break;
            case TIMESTAMP:
                if (columnOption.getTimestampFormat().isPresent()) {
                    pattern = columnOption.getTimestampFormat().get();
                    timezone = columnOption.getTimezone();
                    parser = TimestampParser.of(pattern, timezone);
                    ts = parser.parse(src);
                    timestampFormat = TimestampFormatter.of("%Y-%m-%d %H:%M:%S.%6N %:z", timezone);
                    node.put(name, timestampFormat.format(ts));
                } else {
                    // Users must care of BQ timestamp format by themselves with no timestamp_format
                    if (src == null) {
                        node.putNull(name);
                    } else {
                        node.put(name, src);
                    }
                }
                break;
            case DATETIME:
                if (columnOption.getTimestampFormat().isPresent()) {
                    pattern = columnOption.getTimestampFormat().get();
                    timezone = columnOption.getTimezone();
                    parser = TimestampParser.of(pattern, timezone);
                    ts = parser.parse(src);
                    timestampFormat = TimestampFormatter.of("%Y-%m-%d %H:%M:%S.%6N", timezone);
                    node.put(name, timestampFormat.format(ts));
                } else {
                    // Users must care of BQ datetime format by themselves with no timestamp_format
                    if (src == null) {
                        node.putNull(name);
                    } else {
                        node.put(name, src);
                    }
                }
                break;
            case DATE:
                if (columnOption.getTimestampFormat().isPresent()) {
                    pattern = columnOption.getTimestampFormat().get();
                    timezone = columnOption.getTimezone();
                    parser = TimestampParser.of(pattern, timezone);
                    try {
                        ts = parser.parse(src);
                    } catch (TimestampParseException e) {
                        throw new BigqueryTypeCastException(e.getMessage());
                    }
                    timestampFormat = TimestampFormatter.of("%Y-%m-%d", timezone);
                    node.put(name, timestampFormat.format(ts));
                } else {
                    // Users must care of BQ date format by themselves with no timestamp_format
                    if (src == null) {
                        node.putNull(name);
                    } else {
                        node.put(name, src);
                    }
                }
                break;
            case NUMERIC:
                // Default value: 9, BigQuery NUMERIC type has a maximum scale of 9
                int scale = columnOption != null ? columnOption.getScale() : 9;
                node.put(name, new BigDecimal(src).setScale(scale, BigDecimal.ROUND_CEILING));
                break;
            default:
                throw new BigqueryNotSupportedTypeException("Invalid data convert for String");
        }
    }
}
