package org.embulk.output.bigquery_java.visitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.embulk.config.ConfigSource;
import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.units.LocalFile;
import org.junit.Test;
import org.msgpack.value.Value;
import org.msgpack.value.impl.ImmutableArrayValueImpl;
import org.msgpack.value.impl.ImmutableDoubleValueImpl;
import org.msgpack.value.impl.ImmutableLongValueImpl;
import org.msgpack.value.impl.ImmutableMapValueImpl;
import org.msgpack.value.impl.ImmutableStringValueImpl;

public class TestJsonColumnVisitor {
  protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();

  protected static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

  @Test
  public void testBooleanColumn() throws JsonProcessingException {
    assertEquals(true, visitColumn(true, JsonColumnVisitor::booleanColumn));
    assertEquals(false, visitColumn(false, JsonColumnVisitor::booleanColumn));
    assertNull(visitColumn(null, JsonColumnVisitor::booleanColumn));

    assertEquals(
        "true", visitColumn(true, JsonColumnVisitor::booleanColumn, x -> x.set("type", "STRING")));
  }

  @Test
  public void testLongColumn() throws JsonProcessingException {
    assertEquals(100, visitColumn(100L, JsonColumnVisitor::longColumn));
    assertNull(visitColumn(null, JsonColumnVisitor::longColumn));

    assertEquals(
        "100", visitColumn(100L, JsonColumnVisitor::longColumn, x -> x.set("type", "STRING")));
  }

  @Test
  public void testDoubleColumn() throws JsonProcessingException {
    assertEquals(100.5, visitColumn(100.5, JsonColumnVisitor::doubleColumn));
    assertNull(visitColumn(null, JsonColumnVisitor::doubleColumn));

    assertEquals(
        "100.5", visitColumn(100.5, JsonColumnVisitor::doubleColumn, x -> x.set("type", "STRING")));
  }

  @Test
  public void testStringColumn() throws JsonProcessingException {
    assertEquals("test", visitColumn("test", JsonColumnVisitor::stringColumn));
    assertEquals("あ", visitColumn("あ", JsonColumnVisitor::stringColumn));
    assertEquals("😄", visitColumn("😄", JsonColumnVisitor::stringColumn));
    assertNull(visitColumn(null, JsonColumnVisitor::stringColumn));

    assertEquals(
        100, visitColumn("100", JsonColumnVisitor::stringColumn, x -> x.set("type", "INTEGER")));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testTimestampColumn() throws JsonProcessingException {
    assertEquals(
        "1970-01-01 00:00:00.000000 +00:00",
        visitColumn(
            org.embulk.spi.time.Timestamp.ofEpochMilli(0), JsonColumnVisitor::timestampColumn));
    assertNull(visitColumn(null, JsonColumnVisitor::timestampColumn));

    assertEquals(
        10000,
        visitColumn(
            org.embulk.spi.time.Timestamp.ofEpochMilli(10000),
            JsonColumnVisitor::timestampColumn,
            x -> x.set("type", "INTEGER")));
  }

  @Test
  public void testJsonColumn() throws JsonProcessingException {
    ImmutableMapValueImpl jsonObject =
        new ImmutableMapValueImpl(
            new Value[] {
              new ImmutableStringValueImpl("k0"),
              new ImmutableDoubleValueImpl(2.5),
              new ImmutableStringValueImpl("k1"),
              new ImmutableArrayValueImpl(
                  new Value[] {new ImmutableLongValueImpl(0), new ImmutableLongValueImpl(1)}),
              new ImmutableStringValueImpl("k2"),
              new ImmutableMapValueImpl(
                  new Value[] {
                    new ImmutableStringValueImpl("😀"), new ImmutableStringValueImpl("😀")
                  })
            });
    assertEquals(
        "{\"k0\":2.5,\"k1\":[0,1],\"k2\":{\"\\uD83D\\uDE00\":\"\\uD83D\\uDE00\"}}",
        visitColumn(jsonObject, JsonColumnVisitor::jsonColumn));
    assertNull(visitColumn(null, JsonColumnVisitor::jsonColumn));

    // columnOption is ignored
    assertEquals(
        "{\"k0\":2.5,\"k1\":[0,1],\"k2\":{\"\\uD83D\\uDE00\":\"\\uD83D\\uDE00\"}}",
        visitColumn(jsonObject, JsonColumnVisitor::jsonColumn, x -> x.set("type", "INTEGER")));
  }

  private Object visitColumn(Object value, BiConsumer<JsonColumnVisitor, Column> visit)
      throws JsonProcessingException {
    return visitColumn(value, visit, null);
  }

  @SuppressWarnings("unchecked")
  private Object visitColumn(
      Object value,
      BiConsumer<JsonColumnVisitor, Column> visit,
      Function<ConfigSource, ConfigSource> columnOption)
      throws JsonProcessingException {
    ConfigSource configSource = CONFIG_MAPPER_FACTORY.newConfigSource();
    configSource.set("mode", "replace");
    configSource.set("json_keyfile", LocalFile.ofContent(""));
    configSource.set("dataset", "test");
    configSource.set("table", "test");
    configSource.set("source_format", "NEWLINE_DELIMITED_JSON");
    PluginTask task = CONFIG_MAPPER.map(configSource, PluginTask.class);
    Column column = new Column(0, "k", Types.STRING);
    try (PageReader pageReader = new PageReaderForTest(value)) {
      List<BigqueryColumnOption> columnOptions = new ArrayList<>();
      if (columnOption != null) {
        ConfigSource c = CONFIG_MAPPER_FACTORY.newConfigSource().set("name", "k");
        BigqueryColumnOption bigqueryColumnOption =
            CONFIG_MAPPER.map(columnOption.apply(c), BigqueryColumnOption.class);
        columnOptions.add(bigqueryColumnOption);
      }
      JsonColumnVisitor jsonColumnVisitor = new JsonColumnVisitor(task, pageReader, columnOptions);
      visit.accept(jsonColumnVisitor, column);

      String json = new String(jsonColumnVisitor.getByteArray(), StandardCharsets.UTF_8);
      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> map = mapper.readValue(json, Map.class);
      return map.get("k");
    }
  }

  @SuppressWarnings("deprecation")
  private static class PageReaderForTest extends PageReader {
    private final Object value;

    public PageReaderForTest(Object value) {
      super(new Schema(new ArrayList<>()));
      this.value = value;
    }

    @Override
    public boolean isNull(Column column) {
      return value == null;
    }

    @Override
    public boolean getBoolean(Column column) {
      return (boolean) value;
    }

    @Override
    public long getLong(Column column) {
      return (long) value;
    }

    @Override
    public double getDouble(Column column) {
      return (double) value;
    }

    @Override
    public String getString(Column column) {
      return (String) value;
    }

    @Override
    public org.embulk.spi.time.Timestamp getTimestamp(Column column) {
      return (org.embulk.spi.time.Timestamp) value;
    }

    @Override
    public Value getJson(Column column) {
      return (Value) value;
    }
  }
}
