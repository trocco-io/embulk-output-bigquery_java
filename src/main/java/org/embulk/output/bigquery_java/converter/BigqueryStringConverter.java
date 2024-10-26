package org.embulk.output.bigquery_java.converter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
import org.embulk.output.bigquery_java.config.BigqueryColumnOptionType;
import org.embulk.output.bigquery_java.exception.BigqueryNotSupportedTypeException;
import org.embulk.output.bigquery_java.exception.BigqueryTypeCastException;
import org.embulk.util.timestamp.TimestampFormatter;

import java.math.BigDecimal;

public class BigqueryStringConverter {
    @SuppressWarnings("deprecation") // The use of org.embulk.spi.time.Timestamp
    public static void convertAndSet(ObjectNode node, String name, String src, BigqueryColumnOptionType bigqueryColumnOptionType, BigqueryColumnOption columnOption) {
        TimestampFormatter timestampFormat;
        String pattern;
        String timezone;
        TimestampFormatter parser;
        org.embulk.spi.time.Timestamp ts;
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
                    parser = TimestampFormatter.builder(pattern, true).setDefaultZoneFromString(timezone).build();
                    ts = org.embulk.spi.time.Timestamp.ofInstant(parser.parse(src));
                    timestampFormat = TimestampFormatter.builder("%Y-%m-%d %H:%M:%S.%6N %:z", true).setDefaultZoneFromString(timezone).build();
                    node.put(name, timestampFormat.format(ts.getInstant()));
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
                    parser = TimestampFormatter.builder(pattern, true).setDefaultZoneFromString(timezone).build();
                    ts = org.embulk.spi.time.Timestamp.ofInstant(parser.parse(src));
                    timestampFormat = TimestampFormatter.builder("%Y-%m-%d %H:%M:%S.%6N", true).setDefaultZoneFromString(timezone).build();
                    node.put(name, timestampFormat.format(ts.getInstant()));
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
                    parser = TimestampFormatter.builder(pattern, true).setDefaultZoneFromString(timezone).build();
                    try {
                        ts = org.embulk.spi.time.Timestamp.ofInstant(parser.parse(src));
                    } catch (org.embulk.util.rubytime.RubyDateTimeParseException e) {
                        throw new BigqueryTypeCastException(e.getMessage());
                    }
                    timestampFormat = TimestampFormatter.builder("%Y-%m-%d", true).setDefaultZoneFromString(timezone).build();
                    node.put(name, timestampFormat.format(ts.getInstant()));
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
