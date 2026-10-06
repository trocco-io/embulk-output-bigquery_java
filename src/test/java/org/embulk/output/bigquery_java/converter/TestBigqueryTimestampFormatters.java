package org.embulk.output.bigquery_java.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.util.Optional;
import org.embulk.config.ConfigSource;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.units.LocalFile;
import org.embulk.util.timestamp.TimestampFormatter;
import org.junit.Test;

public class TestBigqueryTimestampFormatters {
  private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();
  private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

  private static PluginTask createTask(String defaultTimezone, String defaultTimestampFormat) {
    ConfigSource configSource = CONFIG_MAPPER_FACTORY.newConfigSource();
    configSource.set("mode", "replace");
    configSource.set("json_keyfile", LocalFile.ofContent(""));
    configSource.set("dataset", "test");
    configSource.set("table", "test");
    configSource.set("source_format", "NEWLINE_DELIMITED_JSON");
    configSource.set("default_timezone", defaultTimezone);
    configSource.set("default_timestamp_format", defaultTimestampFormat);
    return CONFIG_MAPPER.map(configSource, PluginTask.class);
  }

  @Test
  public void testResolveTimezone_prefersColumnOverTask() {
    PluginTask task = createTask("Asia/Tokyo", "%Y-%m-%d");
    assertEquals(
        "America/New_York",
        BigqueryTimestampFormatters.resolveTimezone(Optional.of("America/New_York"), task));
  }

  @Test
  public void testResolveTimezone_fallsBackToTaskDefault() {
    PluginTask task = createTask("Asia/Tokyo", "%Y-%m-%d");
    assertEquals("Asia/Tokyo", BigqueryTimestampFormatters.resolveTimezone(Optional.empty(), task));
  }

  @Test
  public void testResolveTimezone_withoutTaskFallsBackToUtc() {
    assertEquals("UTC", BigqueryTimestampFormatters.resolveTimezone(Optional.empty(), null));
    assertEquals(
        "Asia/Tokyo", BigqueryTimestampFormatters.resolveTimezone(Optional.of("Asia/Tokyo"), null));
  }

  @Test
  public void testResolveTimestampFormat_prefersColumnOverTask() {
    PluginTask task = createTask("UTC", "%Y-%m-%d");
    assertEquals(
        "%H:%M", BigqueryTimestampFormatters.resolveTimestampFormat(Optional.of("%H:%M"), task));
  }

  @Test
  public void testResolveTimestampFormat_fallsBackToTaskDefault() {
    PluginTask task = createTask("UTC", "%Y-%m-%d");
    assertEquals(
        "%Y-%m-%d", BigqueryTimestampFormatters.resolveTimestampFormat(Optional.empty(), task));
  }

  @Test
  public void testResolveTimestampFormat_withoutTaskFallsBackToRubyDefault() {
    assertEquals(
        "%Y-%m-%d %H:%M:%S.%6N",
        BigqueryTimestampFormatters.resolveTimestampFormat(Optional.empty(), null));
  }

  @Test
  public void testGet_reusesFormatterForSamePatternAndTimezone() {
    TimestampFormatter first = BigqueryTimestampFormatters.get("%Y-%m-%d", "Asia/Tokyo");
    TimestampFormatter second = BigqueryTimestampFormatters.get("%Y-%m-%d", "Asia/Tokyo");
    assertSame(first, second);
  }

  @Test
  public void testGet_distinguishesPatternAndTimezone() {
    TimestampFormatter base = BigqueryTimestampFormatters.get("%Y-%m-%d", "Asia/Tokyo");
    assertNotSame(base, BigqueryTimestampFormatters.get("%Y-%m-%d", "UTC"));
    assertNotSame(base, BigqueryTimestampFormatters.get("%Y/%m/%d", "Asia/Tokyo"));
  }

  @Test
  public void testGet_appliesTimezoneToFormatting() {
    // Thu Apr 30 2020 20:00:00 UTC == Fri May 01 2020 05:00:00 JST
    java.time.Instant instant = java.time.Instant.ofEpochMilli(1588276800000L);
    assertEquals(
        "2020-05-01", BigqueryTimestampFormatters.get("%Y-%m-%d", "Asia/Tokyo").format(instant));
    assertEquals("2020-04-30", BigqueryTimestampFormatters.get("%Y-%m-%d", "UTC").format(instant));
  }
}
