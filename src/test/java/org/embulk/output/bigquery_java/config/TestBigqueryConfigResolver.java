package org.embulk.output.bigquery_java.config;

import static org.embulk.output.bigquery_java.config.BigqueryConfigResolver.effectiveReadTimeoutSec;
import static org.embulk.output.bigquery_java.config.BigqueryConfigResolver.resolveReadTimeoutSec;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestBigqueryConfigResolver extends BigqueryConfigTestBase {
  @Test
  public void resolveReadTimeoutSec_prefersReadTimeoutSec() {
    assertEquals(
        34,
        resolveReadTimeoutSec(taskWith(c -> c.set("read_timeout_sec", 34).set("timeout_sec", 99))));
  }

  @Test
  public void resolveReadTimeoutSec_fallsBackToDeprecatedTimeoutSec() {
    assertEquals(99, resolveReadTimeoutSec(taskWith(c -> c.set("timeout_sec", 99))));
  }

  @Test
  public void resolveReadTimeoutSec_defaultsTo300WhenNeitherIsSet() {
    // 300 = BigqueryConfigResolver.DEFAULT_READ_TIMEOUT_SEC
    assertEquals(300, resolveReadTimeoutSec(taskWith(c -> c)));
  }

  @Test
  public void effectiveReadTimeoutSec_addsSendTimeoutSec() {
    assertEquals(
        34 + 56,
        effectiveReadTimeoutSec(
            taskWith(c -> c.set("read_timeout_sec", 34).set("send_timeout_sec", 56))));
  }

  @Test
  public void effectiveReadTimeoutSec_doesNotOverflowInt() {
    // Both values fit in int seconds on their own; the sum must be computed as long.
    assertEquals(
        (long) Integer.MAX_VALUE + 1,
        effectiveReadTimeoutSec(
            taskWith(
                c -> c.set("read_timeout_sec", Integer.MAX_VALUE).set("send_timeout_sec", 1))));
  }
}
