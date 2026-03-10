package org.embulk.output.bigquery_java.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import org.embulk.output.bigquery_java.BigqueryUtil;
import org.embulk.output.bigquery_java.config.BigqueryFieldOption;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.output.bigquery_java.exception.BigqueryNotSupportedTypeException;
import org.embulk.output.bigquery_java.exception.BigqueryTypeCastException;
import org.embulk.util.timestamp.TimestampFormatter;

public class BigqueryRecordConverter {

  @SuppressWarnings("deprecation")
  public static JsonNode convertRecordValue(
      JsonNode data, List<BigqueryFieldOption> fields, PluginTask task) {
    if (data == null || data.isNull()) {
      return data;
    }
    if (data.isArray()) {
      ArrayNode arrayNode = BigqueryUtil.getObjectMapper().createArrayNode();
      for (JsonNode element : data) {
        arrayNode.add(convertRecordValue(element, fields, task));
      }
      return arrayNode;
    }
    if (!data.isObject()) {
      return data;
    }

    ObjectNode result = ((ObjectNode) data).deepCopy();
    for (BigqueryFieldOption field : fields) {
      String fieldName = field.getName();
      if (!result.has(fieldName) || result.get(fieldName).isNull()) {
        continue;
      }
      JsonNode value = result.get(fieldName);
      if (field.getMode().equalsIgnoreCase("REPEATED") && value.isArray()) {
        ArrayNode convertedArray = BigqueryUtil.getObjectMapper().createArrayNode();
        for (JsonNode element : value) {
          convertedArray.add(convertFieldValue(element, field, task));
        }
        result.set(fieldName, convertedArray);
      } else {
        result.set(fieldName, convertFieldValue(value, field, task));
      }
    }
    return result;
  }

  @SuppressWarnings("deprecation")
  public static JsonNode convertFieldValue(
      JsonNode value, BigqueryFieldOption field, PluginTask task) {
    if (value == null || value.isNull()) {
      return value;
    }

    String type = field.getType().toUpperCase();
    switch (type) {
      case "STRING":
      case "JSON":
      case "NUMERIC":
        return value;
      case "BOOLEAN":
        return convertToBoolean(value);
      case "INTEGER":
        return convertToInteger(value);
      case "FLOAT":
        return convertToFloat(value);
      case "TIMESTAMP":
        return convertTimestamp(value, field, task, "%Y-%m-%d %H:%M:%S.%6N %:z");
      case "DATETIME":
        return convertTimestamp(value, field, task, "%Y-%m-%d %H:%M:%S.%6N");
      case "DATE":
        return convertTimestamp(value, field, task, "%Y-%m-%d");
      case "RECORD":
        if (field.getFields().isPresent()) {
          return convertRecordValue(value, field.getFields().get(), task);
        }
        return value;
      default:
        throw new BigqueryNotSupportedTypeException(
            String.format("Unsupported field type: %s", type));
    }
  }

  private static JsonNode convertToBoolean(JsonNode value) {
    if (value.isBoolean()) {
      return value;
    }
    String str = value.asText();
    if (str.equalsIgnoreCase("true")) {
      return BigqueryUtil.getObjectMapper().getNodeFactory().booleanNode(true);
    } else if (str.equalsIgnoreCase("false")) {
      return BigqueryUtil.getObjectMapper().getNodeFactory().booleanNode(false);
    }
    throw new BigqueryTypeCastException(String.format("%s cannot be converted to BOOLEAN", str));
  }

  private static JsonNode convertToInteger(JsonNode value) {
    if (value.isIntegralNumber()) {
      return value;
    }
    try {
      long longVal = Long.parseLong(value.asText());
      return BigqueryUtil.getObjectMapper().getNodeFactory().numberNode(longVal);
    } catch (NumberFormatException e) {
      throw new BigqueryTypeCastException(
          String.format("%s cannot be converted to INTEGER", value.asText()));
    }
  }

  private static JsonNode convertToFloat(JsonNode value) {
    if (value.isFloatingPointNumber()) {
      return value;
    }
    try {
      double doubleVal = Double.parseDouble(value.asText());
      return BigqueryUtil.getObjectMapper().getNodeFactory().numberNode(doubleVal);
    } catch (NumberFormatException e) {
      throw new BigqueryTypeCastException(
          String.format("%s cannot be converted to FLOAT", value.asText()));
    }
  }

  @SuppressWarnings("deprecation")
  private static JsonNode convertTimestamp(
      JsonNode value, BigqueryFieldOption field, PluginTask task, String outputFormat) {
    String src = value.asText();
    String pattern;
    String timezone;

    if (field.getTimestampFormat().isPresent()) {
      pattern = field.getTimestampFormat().get();
      timezone = field.getTimezone();
    } else {
      // Users must care of BQ format by themselves with no timestamp_format
      return value;
    }

    try {
      TimestampFormatter parser =
          TimestampFormatter.builder(pattern, true).setDefaultZoneFromString(timezone).build();
      org.embulk.spi.time.Timestamp ts = org.embulk.spi.time.Timestamp.ofInstant(parser.parse(src));
      TimestampFormatter formatter =
          TimestampFormatter.builder(outputFormat, true).setDefaultZoneFromString(timezone).build();
      return BigqueryUtil.getObjectMapper()
          .getNodeFactory()
          .textNode(formatter.format(ts.getInstant()));
    } catch (Exception e) {
      // Fall back to original value if parsing fails (consistent with Ruby version)
      return value;
    }
  }
}
