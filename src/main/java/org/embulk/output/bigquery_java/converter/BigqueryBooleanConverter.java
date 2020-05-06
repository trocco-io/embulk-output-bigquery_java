package org.embulk.output.bigquery_java.converter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.output.bigquery_java.config.BigqueryColumnOptionType;
import org.embulk.output.bigquery_java.exception.BigqueryNotSupportedTypeException;

public class BigqueryBooleanConverter {
    public static void convertAndSet(ObjectNode node, String name, boolean src, BigqueryColumnOptionType bigqueryColumnOptionType) {
        switch (bigqueryColumnOptionType) {
            case BOOLEAN:
                node.put(name, src);
                break;
            case STRING:
                node.put(name, String.valueOf(src));
                break;
            default:
                throw new BigqueryNotSupportedTypeException("Invalid data convert for Boolean");
        }
    }
}
