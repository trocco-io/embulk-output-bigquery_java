package org.embulk.output.bigquery_java.config;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

// The `mode` values and what each one implies. The validators and setTempTable() read from here
// so the per-mode lists can't drift apart when a mode is added or enabled.
//
// Callers keep `mode` as the lower-case config string (BigqueryConfigValidator.validate() folds it
// once, as ruby does) and compare it with `equals("replace")` etc.; the enum only owns the lists.
public enum Mode {
  REPLACE(true, true, true),
  // Listed so the auto_create_table check and the temp-table routing already treat it as ruby
  // does, but not supported: validateMode() rejects it until the mode is implemented.
  REPLACE_BACKUP(false, true, true),
  APPEND(true, true, true),
  // Loads straight into the destination; the only mode that may run with auto_create_table: false.
  APPEND_DIRECT(true, false, false),
  MERGE(true, true, true),
  // Loads straight into the destination too, after deleting it.
  DELETE_IN_ADVANCE(true, true, false);

  private final boolean supported;
  private final boolean requiresAutoCreateTable;
  private final boolean usesTempTable;

  Mode(boolean supported, boolean requiresAutoCreateTable, boolean usesTempTable) {
    this.supported = supported;
    this.requiresAutoCreateTable = requiresAutoCreateTable;
    this.usesTempTable = usesTempTable;
  }

  // The value as written in the config: `replace`, `append_direct`, ...
  public String configName() {
    return name().toLowerCase(Locale.ROOT);
  }

  // Whether `mode` is a known, implemented mode. Unknown values (and unnormalized spellings such
  // as `Replace`) are not.
  public static boolean isSupported(String mode) {
    return is(mode, m -> m.supported);
  }

  public static boolean requiresAutoCreateTable(String mode) {
    return is(mode, m -> m.requiresAutoCreateTable);
  }

  // Modes that stage the load in a temp table and copy/merge it into the destination afterwards.
  public static boolean usesTempTable(String mode) {
    return is(mode, m -> m.usesTempTable);
  }

  public static List<String> supportedConfigNames() {
    return Arrays.stream(values())
        .filter(m -> m.supported)
        .map(Mode::configName)
        .collect(Collectors.toList());
  }

  // False for an unknown mode; validateMode() is where that gets reported.
  private static boolean is(String mode, Predicate<Mode> property) {
    return parse(mode).map(property::test).orElse(false);
  }

  private static Optional<Mode> parse(String mode) {
    return Arrays.stream(values()).filter(m -> m.configName().equals(mode)).findFirst();
  }
}
