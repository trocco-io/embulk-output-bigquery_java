package org.embulk.output.bigquery_java.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import org.embulk.config.ConfigSource;
import org.embulk.output.bigquery_java.BigqueryUtil;
import org.embulk.output.bigquery_java.config.BigqueryFieldOption;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.units.LocalFile;
import org.junit.Before;
import org.junit.Test;

public class TestBigqueryRecordConverter {
  private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();

  private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
  private static final ObjectMapper OBJECT_MAPPER = BigqueryUtil.getObjectMapper();

  private PluginTask task;

  @Before
  public void setUp() {
    ConfigSource configSource = CONFIG_MAPPER_FACTORY.newConfigSource();
    configSource.set("mode", "replace");
    configSource.set("json_keyfile", LocalFile.ofContent(""));
    configSource.set("dataset", "test");
    configSource.set("table", "test");
    configSource.set("source_format", "NEWLINE_DELIMITED_JSON");
    configSource.set("default_timestamp_format", "%Y-%m-%d %H:%M:%S.%6N %:z");
    configSource.set("default_timezone", "UTC");
    task = CONFIG_MAPPER.map(configSource, PluginTask.class);
  }

  private BigqueryFieldOption createFieldOption(String name, String type) {
    ConfigSource configSource = CONFIG_MAPPER_FACTORY.newConfigSource();
    configSource.set("name", name);
    configSource.set("type", type);
    return CONFIG_MAPPER.map(configSource, BigqueryFieldOption.class);
  }

  private BigqueryFieldOption createFieldOption(String name, String type, String timestampFormat) {
    ConfigSource configSource = CONFIG_MAPPER_FACTORY.newConfigSource();
    configSource.set("name", name);
    configSource.set("type", type);
    configSource.set("timestamp_format", timestampFormat);
    return CONFIG_MAPPER.map(configSource, BigqueryFieldOption.class);
  }

  private BigqueryFieldOption createFieldOptionWithMode(String name, String type, String mode) {
    ConfigSource configSource = CONFIG_MAPPER_FACTORY.newConfigSource();
    configSource.set("name", name);
    configSource.set("type", type);
    configSource.set("mode", mode);
    return CONFIG_MAPPER.map(configSource, BigqueryFieldOption.class);
  }

  @Test
  public void testConvertStringField() throws Exception {
    ObjectNode data = OBJECT_MAPPER.createObjectNode();
    data.put("name", "test");

    List<BigqueryFieldOption> fields = new ArrayList<>();
    fields.add(createFieldOption("name", "STRING"));

    JsonNode result = BigqueryRecordConverter.convertRecordValue(data, fields, task);
    assertEquals("test", result.get("name").asText());
  }

  @Test
  public void testConvertBooleanField() throws Exception {
    ObjectNode data = OBJECT_MAPPER.createObjectNode();
    data.put("flag", "true");

    List<BigqueryFieldOption> fields = new ArrayList<>();
    fields.add(createFieldOption("flag", "BOOLEAN"));

    JsonNode result = BigqueryRecordConverter.convertRecordValue(data, fields, task);
    assertTrue(result.get("flag").isBoolean());
    assertEquals(true, result.get("flag").asBoolean());
  }

  @Test
  public void testConvertBooleanFieldNative() throws Exception {
    ObjectNode data = OBJECT_MAPPER.createObjectNode();
    data.put("flag", true);

    List<BigqueryFieldOption> fields = new ArrayList<>();
    fields.add(createFieldOption("flag", "BOOLEAN"));

    JsonNode result = BigqueryRecordConverter.convertRecordValue(data, fields, task);
    assertTrue(result.get("flag").isBoolean());
    assertEquals(true, result.get("flag").asBoolean());
  }

  @Test
  public void testConvertIntegerField() throws Exception {
    ObjectNode data = OBJECT_MAPPER.createObjectNode();
    data.put("count", "42");

    List<BigqueryFieldOption> fields = new ArrayList<>();
    fields.add(createFieldOption("count", "INTEGER"));

    JsonNode result = BigqueryRecordConverter.convertRecordValue(data, fields, task);
    assertEquals(42, result.get("count").asLong());
  }

  @Test
  public void testConvertIntegerFieldNative() throws Exception {
    ObjectNode data = OBJECT_MAPPER.createObjectNode();
    data.put("count", 42);

    List<BigqueryFieldOption> fields = new ArrayList<>();
    fields.add(createFieldOption("count", "INTEGER"));

    JsonNode result = BigqueryRecordConverter.convertRecordValue(data, fields, task);
    assertEquals(42, result.get("count").asLong());
  }

  @Test
  public void testConvertFloatField() throws Exception {
    ObjectNode data = OBJECT_MAPPER.createObjectNode();
    data.put("value", "3.14");

    List<BigqueryFieldOption> fields = new ArrayList<>();
    fields.add(createFieldOption("value", "FLOAT"));

    JsonNode result = BigqueryRecordConverter.convertRecordValue(data, fields, task);
    assertEquals(3.14, result.get("value").asDouble(), 0.001);
  }

  @Test
  public void testConvertTimestampField() throws Exception {
    ObjectNode data = OBJECT_MAPPER.createObjectNode();
    data.put("created_at", "2020/05/01 12:00:00");

    List<BigqueryFieldOption> fields = new ArrayList<>();
    fields.add(createFieldOption("created_at", "TIMESTAMP", "%Y/%m/%d %H:%M:%S"));

    JsonNode result = BigqueryRecordConverter.convertRecordValue(data, fields, task);
    assertEquals("2020-05-01 12:00:00.000000 +00:00", result.get("created_at").asText());
  }

  @Test
  public void testConvertDatetimeField() throws Exception {
    ObjectNode data = OBJECT_MAPPER.createObjectNode();
    data.put("updated_at", "2020/05/01 12:00:00");

    List<BigqueryFieldOption> fields = new ArrayList<>();
    fields.add(createFieldOption("updated_at", "DATETIME", "%Y/%m/%d %H:%M:%S"));

    JsonNode result = BigqueryRecordConverter.convertRecordValue(data, fields, task);
    assertEquals("2020-05-01 12:00:00.000000", result.get("updated_at").asText());
  }

  @Test
  public void testConvertDateField() throws Exception {
    ObjectNode data = OBJECT_MAPPER.createObjectNode();
    data.put("birth_date", "2020/05/01");

    List<BigqueryFieldOption> fields = new ArrayList<>();
    fields.add(createFieldOption("birth_date", "DATE", "%Y/%m/%d"));

    JsonNode result = BigqueryRecordConverter.convertRecordValue(data, fields, task);
    assertEquals("2020-05-01", result.get("birth_date").asText());
  }

  @Test
  public void testConvertRepeatedMode() throws Exception {
    ObjectNode data = OBJECT_MAPPER.createObjectNode();
    ArrayNode array = OBJECT_MAPPER.createArrayNode();
    array.add("true");
    array.add("false");
    data.set("flags", array);

    List<BigqueryFieldOption> fields = new ArrayList<>();
    fields.add(createFieldOptionWithMode("flags", "BOOLEAN", "REPEATED"));

    JsonNode result = BigqueryRecordConverter.convertRecordValue(data, fields, task);
    assertTrue(result.get("flags").isArray());
    assertEquals(true, result.get("flags").get(0).asBoolean());
    assertEquals(false, result.get("flags").get(1).asBoolean());
  }

  @Test
  public void testConvertNestedRecord() throws Exception {
    ObjectNode inner = OBJECT_MAPPER.createObjectNode();
    inner.put("city", "Tokyo");
    inner.put("zip", "100");

    ObjectNode data = OBJECT_MAPPER.createObjectNode();
    data.set("address", inner);

    ConfigSource addressFieldConfig = CONFIG_MAPPER_FACTORY.newConfigSource();
    addressFieldConfig.set("name", "address");
    addressFieldConfig.set("type", "RECORD");
    List<ConfigSource> subFields = new ArrayList<>();
    ConfigSource cityField = CONFIG_MAPPER_FACTORY.newConfigSource();
    cityField.set("name", "city");
    cityField.set("type", "STRING");
    subFields.add(cityField);
    ConfigSource zipField = CONFIG_MAPPER_FACTORY.newConfigSource();
    zipField.set("name", "zip");
    zipField.set("type", "INTEGER");
    subFields.add(zipField);
    addressFieldConfig.set("fields", subFields);

    BigqueryFieldOption addressOption =
        CONFIG_MAPPER.map(addressFieldConfig, BigqueryFieldOption.class);

    List<BigqueryFieldOption> fields = new ArrayList<>();
    fields.add(addressOption);

    JsonNode result = BigqueryRecordConverter.convertRecordValue(data, fields, task);
    assertEquals("Tokyo", result.get("address").get("city").asText());
    assertEquals(100, result.get("address").get("zip").asLong());
  }

  @Test
  public void testConvertStringFieldWithJsonObjectValue() throws Exception {
    ObjectNode data = OBJECT_MAPPER.createObjectNode();
    ObjectNode jsonValue = OBJECT_MAPPER.createObjectNode();
    jsonValue.put("key", "value");
    ArrayNode nested = OBJECT_MAPPER.createArrayNode();
    nested.add(1);
    nested.add(2);
    nested.add(3);
    jsonValue.set("nested", nested);
    data.set("v", jsonValue);

    List<BigqueryFieldOption> fields = new ArrayList<>();
    fields.add(createFieldOption("v", "STRING"));

    JsonNode result = BigqueryRecordConverter.convertRecordValue(data, fields, task);
    assertTrue(result.get("v").isTextual());
    assertEquals("{\"key\":\"value\",\"nested\":[1,2,3]}", result.get("v").asText());
  }

  @Test
  public void testConvertStringFieldWithArrayValue() throws Exception {
    ObjectNode data = OBJECT_MAPPER.createObjectNode();
    ArrayNode arrayValue = OBJECT_MAPPER.createArrayNode();
    arrayValue.add("a");
    arrayValue.add("b");
    arrayValue.add("c");
    data.set("v", arrayValue);

    List<BigqueryFieldOption> fields = new ArrayList<>();
    fields.add(createFieldOption("v", "STRING"));

    JsonNode result = BigqueryRecordConverter.convertRecordValue(data, fields, task);
    assertTrue(result.get("v").isTextual());
    assertEquals("[\"a\",\"b\",\"c\"]", result.get("v").asText());
  }

  @Test
  public void testConvertJsonField() throws Exception {
    ObjectNode data = OBJECT_MAPPER.createObjectNode();
    ObjectNode jsonValue = OBJECT_MAPPER.createObjectNode();
    jsonValue.put("key", "value");
    jsonValue.put("num", 123);
    data.set("metadata", jsonValue);

    List<BigqueryFieldOption> fields = new ArrayList<>();
    fields.add(createFieldOption("metadata", "JSON"));

    JsonNode result = BigqueryRecordConverter.convertRecordValue(data, fields, task);
    assertEquals("value", result.get("metadata").get("key").asText());
    assertEquals(123, result.get("metadata").get("num").asInt());
  }

  @Test
  public void testConvertNumericField() throws Exception {
    ObjectNode data = OBJECT_MAPPER.createObjectNode();
    data.put("price", 99.99);

    List<BigqueryFieldOption> fields = new ArrayList<>();
    fields.add(createFieldOption("price", "NUMERIC"));

    JsonNode result = BigqueryRecordConverter.convertRecordValue(data, fields, task);
    assertEquals(99.99, result.get("price").asDouble(), 0.001);
  }

  @Test
  public void testNullInput() {
    List<BigqueryFieldOption> fields = new ArrayList<>();
    fields.add(createFieldOption("name", "STRING"));

    JsonNode result = BigqueryRecordConverter.convertRecordValue(null, fields, task);
    assertNull(result);
  }

  @Test
  public void testNullNodeInput() {
    List<BigqueryFieldOption> fields = new ArrayList<>();
    fields.add(createFieldOption("name", "STRING"));

    JsonNode nullNode = OBJECT_MAPPER.nullNode();
    JsonNode result = BigqueryRecordConverter.convertRecordValue(nullNode, fields, task);
    assertTrue(result.isNull());
  }

  @Test
  public void testMissingFieldInData() throws Exception {
    ObjectNode data = OBJECT_MAPPER.createObjectNode();
    data.put("other", "value");

    List<BigqueryFieldOption> fields = new ArrayList<>();
    fields.add(createFieldOption("name", "STRING"));

    JsonNode result = BigqueryRecordConverter.convertRecordValue(data, fields, task);
    assertEquals("value", result.get("other").asText());
    assertNull(result.get("name"));
  }

  @Test
  public void testNullFieldValue() throws Exception {
    ObjectNode data = OBJECT_MAPPER.createObjectNode();
    data.putNull("name");

    List<BigqueryFieldOption> fields = new ArrayList<>();
    fields.add(createFieldOption("name", "BOOLEAN"));

    JsonNode result = BigqueryRecordConverter.convertRecordValue(data, fields, task);
    assertTrue(result.get("name").isNull());
  }

  @Test
  public void testArrayInput() throws Exception {
    ObjectNode elem1 = OBJECT_MAPPER.createObjectNode();
    elem1.put("count", "1");
    ObjectNode elem2 = OBJECT_MAPPER.createObjectNode();
    elem2.put("count", "2");

    ArrayNode array = OBJECT_MAPPER.createArrayNode();
    array.add(elem1);
    array.add(elem2);

    List<BigqueryFieldOption> fields = new ArrayList<>();
    fields.add(createFieldOption("count", "INTEGER"));

    JsonNode result = BigqueryRecordConverter.convertRecordValue(array, fields, task);
    assertTrue(result.isArray());
    assertEquals(1, result.get(0).get("count").asLong());
    assertEquals(2, result.get(1).get("count").asLong());
  }
}
