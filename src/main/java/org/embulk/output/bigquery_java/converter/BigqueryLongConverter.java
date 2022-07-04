package org.embulk.output.bigquery_java.converter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
import org.embulk.output.bigquery_java.config.BigqueryColumnOptionType;
import org.embulk.output.bigquery_java.exception.BigqueryNotSupportedTypeException;
import org.embulk.output.bigquery_java.exception.BigqueryTypeCastException;

import java.math.BigDecimal;

public class BigqueryLongConverter {
    public static void convertAndSet(ObjectNode node, String name, long src, BigqueryColumnOptionType bigqueryColumnOptionType, BigqueryColumnOption columnOption) {
        switch (bigqueryColumnOptionType) {
            case BOOLEAN:
                if (src == 0) {
                    node.put(name, false);
                } else if (src == 1) {
                    node.put(name, true);
                } else {
                    throw new BigqueryTypeCastException("cannot convert");
                }
                break;
            case INTEGER:
                node.put(name, src);
                break;
            case FLOAT:
                node.put(name, Double.valueOf(src));
                break;
            case TIMESTAMP:
                node.put(name, src);
                break;
            case STRING:
                node.put(name, String.valueOf(src));
                break;
            case NUMERIC:
                // BigQuery NUMERIC型のスケール最大値が9なのでデフォルト値を9にする
                int scale = columnOption != null ? columnOption.getScale() : 9;
                node.put(name, BigDecimal.valueOf(src).setScale(scale, BigDecimal.ROUND_CEILING));
                break;
            default:
                throw new BigqueryNotSupportedTypeException("Invalid data convert for double");

        }
    }
}
