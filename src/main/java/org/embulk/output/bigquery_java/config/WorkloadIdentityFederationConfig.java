package org.embulk.output.bigquery_java.config;

import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;
import org.embulk.util.config.units.LocalFile;

public interface WorkloadIdentityFederationConfig extends Task {
  @Config("config")
  LocalFile getConfig();

  @Config("aws_access_key_id")
  String getAwsAccessKeyId();

  @Config("aws_secret_access_key")
  String getAwsSecretAccessKey();

  @Config("aws_region")
  @ConfigDefault("\"ap-northeast-1\"")
  String getAwsRegion();
}
