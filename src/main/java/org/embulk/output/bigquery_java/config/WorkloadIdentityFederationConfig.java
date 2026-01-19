package org.embulk.output.bigquery_java.config;

import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;
import org.embulk.util.config.units.LocalFile;

public interface WorkloadIdentityFederationConfig extends Task {
  @Config("config")
  LocalFile getConfig();

  @Config("aws_role_arn")
  String getAwsRoleArn();

  @Config("aws_role_session_name")
  @ConfigDefault("\"embulk-bigquery-output\"")
  String getAwsRoleSessionName();

  @Config("aws_region")
  @ConfigDefault("\"ap-northeast-1\"")
  String getAwsRegion();
}
