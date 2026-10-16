package org.embulk.output.bigquery_java.config;

import static org.junit.Assert.*;

import java.util.Arrays;
import org.junit.Test;

public class TestMode {
  @Test
  public void isSupported_knownAndImplementedModesOnly() {
    assertTrue(Mode.isSupported("delete_in_advance"));
    assertFalse(Mode.isSupported("replace_backup"));
    assertFalse(Mode.isSupported("foo"));
    // Case folding is validate()'s job, done once; the checks only know the canonical spelling.
    assertFalse(Mode.isSupported("Replace"));
  }

  @Test
  public void supportedConfigNames_excludesReplaceBackup() {
    assertEquals(
        Arrays.asList("replace", "append", "append_direct", "merge", "delete_in_advance"),
        Mode.supportedConfigNames());
  }

  @Test
  public void usesTempTable_matchesRubyList() {
    // Ruby: %w[replace replace_backup append merge].include?(task['mode'])
    for (Mode mode : Mode.values()) {
      boolean expected =
          Arrays.asList(Mode.REPLACE, Mode.REPLACE_BACKUP, Mode.APPEND, Mode.MERGE).contains(mode);
      assertEquals(mode.name(), expected, Mode.usesTempTable(mode.configName()));
    }
    assertFalse(Mode.usesTempTable("foo"));
  }

  @Test
  public void requiresAutoCreateTable_onlyAppendDirectDoesNot() {
    for (Mode mode : Mode.values()) {
      assertEquals(
          mode.name(), mode != Mode.APPEND_DIRECT, Mode.requiresAutoCreateTable(mode.configName()));
    }
    assertFalse(Mode.requiresAutoCreateTable("foo"));
  }
}
