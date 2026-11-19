package org.embulk.output.bigquery_java.config;

import java.util.List;
import java.util.Optional;

// What a column option and a nested field option have in common: a description of their own,
// and fields nested under them that may carry one. Lets one recursion read both
// (BigqueryClient#hasDescription()). The config interfaces re-declare these with their @Config.
public interface DescribedOption {
  Optional<String> getDescription();

  Optional<List<BigqueryFieldOption>> getFields();
}
