package org.embulk.output.bigquery_java.config;

// Resolves effective config values from a PluginTask (fallbacks, deprecated aliases), shared by
// the validator and the client so they never disagree.
public final class BigqueryConfigResolver {
  public static final int DEFAULT_READ_TIMEOUT_SEC = 300;

  private BigqueryConfigResolver() {}

  // Mirrors ruby's google_client.rb: `read_timeout_sec || timeout_sec || 300`.
  public static int resolveReadTimeoutSec(PluginTask task) {
    return task.getReadTimeoutSec()
        .orElseGet(() -> task.getTimeoutSec().orElse(DEFAULT_READ_TIMEOUT_SEC));
  }

  // The read timeout actually applied to the HTTP transport. send_timeout_sec is folded in because
  // the transport has no separate timeout for the send phase; see README.md.
  public static long effectiveReadTimeoutSec(PluginTask task) {
    return (long) resolveReadTimeoutSec(task) + task.getSendTimeoutSec();
  }
}
