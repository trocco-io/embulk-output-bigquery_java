package org.embulk.output.bigquery_java;

import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
import org.embulk.output.bigquery_java.config.BigqueryColumnOptionType;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.output.bigquery_java.converter.*;
import org.embulk.spi.time.Timestamp;

import com.fasterxml.jackson.databind.node.ObjectNode;


public class BigqueryValueConverter {
    // TODO: refactor later
    public static void convertAndSet(ObjectNode node, String name, String src, BigqueryColumnOptionType bigqueryColumnOptionType, BigqueryColumnOption columnOption) {
        BigqueryStringConverter.convertAndSet(node, name, src, bigqueryColumnOptionType, columnOption);
    }

    public static void convertAndSet(ObjectNode node, String name, Timestamp src, BigqueryColumnOptionType bigqueryColumnOptionType, BigqueryColumnOption columnOption, PluginTask task) {
        BigqueryTimestampConverter.convertAndSet(node, name, src, bigqueryColumnOptionType, columnOption, task);
    }

    public static void convertAndSet(ObjectNode node, String name, long src, BigqueryColumnOptionType bigqueryColumnOptionType, BigqueryColumnOption columnOption) {
        BigqueryLongConverter.convertAndSet(node, name, src, bigqueryColumnOptionType, columnOption);
    }

    public static void convertAndSet(ObjectNode node, String name, double src, BigqueryColumnOptionType bigqueryColumnOptionType, BigqueryColumnOption columnOption) {
        BigqueryDoubleConverter.convertAndSet(node, name, src, bigqueryColumnOptionType, columnOption);
    }

    public static void convertAndSet(ObjectNode node, String name, boolean src, BigqueryColumnOptionType bigqueryColumnOptionType) {
        BigqueryBooleanConverter.convertAndSet(node, name, src, bigqueryColumnOptionType);
    }
}
