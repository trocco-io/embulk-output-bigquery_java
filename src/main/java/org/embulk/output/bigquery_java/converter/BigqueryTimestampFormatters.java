package org.embulk.output.bigquery_java.converter;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.util.timestamp.TimestampFormatter;

/**
 * Resolves the effective timestamp format / timezone for a column or record field the way the ruby
 * plugin's ValueConverterFactory does (column option first, then the task-level default), and hands
 * out {@link TimestampFormatter} instances cached per (pattern, timezone) pair.
 *
 * <p>A formatter depends only on its pattern and zone, which are fixed per column for the whole
 * run, so building one per record is pure repeated work; the cache keeps that to one build per
 * distinct pair. {@link TimestampFormatter} instances are immutable, so sharing them across threads
 * is safe.
 */
public final class BigqueryTimestampFormatters {
  // Ruby plugin defaults (ValueConverterFactory::DEFAULT_TIMESTAMP_FORMAT / DEFAULT_TIMEZONE).
  // Used when no PluginTask is available so that every converter tolerates a null task the same
  // way instead of some branches throwing NullPointerException.
  static final String DEFAULT_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%6N";
  static final String DEFAULT_TIMEZONE = "UTC";

  private static final ConcurrentMap<String, TimestampFormatter> CACHE = new ConcurrentHashMap<>();

  private BigqueryTimestampFormatters() {}

  /** Column/field {@code timezone} if set, else the task's {@code default_timezone}. */
  public static String resolveTimezone(Optional<String> columnTimezone, PluginTask task) {
    if (columnTimezone.isPresent()) {
      return columnTimezone.get();
    }
    return task != null ? task.getDefaultTimezone() : DEFAULT_TIMEZONE;
  }

  /**
   * Column/field {@code timestamp_format} if set, else the task's {@code default_timestamp_format}.
   */
  public static String resolveTimestampFormat(Optional<String> columnFormat, PluginTask task) {
    if (columnFormat.isPresent()) {
      return columnFormat.get();
    }
    return task != null ? task.getDefaultTimestampFormat() : DEFAULT_TIMESTAMP_FORMAT;
  }

  /** A (cached) formatter for the given strftime-style pattern and default zone. */
  public static TimestampFormatter get(String pattern, String timezone) {
    return CACHE.computeIfAbsent(
        pattern + '\u0000' + timezone,
        key ->
            TimestampFormatter.builder(pattern, true).setDefaultZoneFromString(timezone).build());
  }
}
