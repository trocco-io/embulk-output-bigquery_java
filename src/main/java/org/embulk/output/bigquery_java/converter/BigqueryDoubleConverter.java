package org.embulk.output.bigquery_java.converter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
import org.embulk.output.bigquery_java.config.BigqueryColumnOptionType;
import org.embulk.output.bigquery_java.exception.BigqueryNotSupportedTypeException;

public class BigqueryDoubleConverter {
    public static void convertAndSet(ObjectNode node, String name, double src, BigqueryColumnOptionType bigqueryColumnOptionType) {
        switch (bigqueryColumnOptionType) {
            case INTEGER:
                node.put(name, (int) src);
                break;
            case FLOAT:
                node.put(name, src);
                break;
            case TIMESTAMP:
                node.put(name, src);
                break;
            case STRING:
                node.put(name, String.valueOf(src));
                break;
            default:
                throw new BigqueryNotSupportedTypeException("Invalid data convert for double");
        }
    }
}
