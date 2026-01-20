package org.embulk.output.bigquery_java;

import com.google.auth.oauth2.AwsSecurityCredentials;
import com.google.auth.oauth2.AwsSecurityCredentialsSupplier;
import com.google.auth.oauth2.ExternalAccountSupplierContext;
import java.io.IOException;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

/**
 * Supplies AWS security credentials by assuming an IAM role. This class handles the AssumeRole
 * operation and automatic credential refresh when credentials are about to expire.
 *
 * <p>This is designed for AWS Role Chaining scenarios where the base credentials (from IRSA, ECS
 * Task Role, or environment variables) need to assume a middle role for Workload Identity
 * Federation.
 */
public class AwsRoleCredentialsSupplier implements AwsSecurityCredentialsSupplier {
  private static final Logger logger = LoggerFactory.getLogger(AwsRoleCredentialsSupplier.class);

  /** Default session duration for AssumeRole (1 hour - maximum for role chaining) */
  private static final int SESSION_DURATION_SECONDS = 3600;

  /** Refresh credentials 5 minutes before expiration */
  private static final int REFRESH_THRESHOLD_SECONDS = 300;

  private final String roleArn;
  private final String sessionName;
  private final String region;
  private final StsClient stsClient;

  private Credentials currentCredentials;
  private Instant expirationTime;

  /**
   * Creates a new AwsRoleCredentialsSupplier.
   *
   * @param roleArn The ARN of the IAM role to assume
   * @param sessionName The session name for the assumed role session
   * @param region The AWS region for STS calls
   */
  public AwsRoleCredentialsSupplier(String roleArn, String sessionName, String region) {
    this.roleArn = roleArn;
    this.sessionName = sessionName;
    this.region = region;
    this.stsClient =
        StsClient.builder()
            .region(Region.of(region))
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build();
    logger.debug(
        "AwsRoleCredentialsSupplier created for role: {}, region: {}, session: {}",
        roleArn,
        region,
        sessionName);
  }

  /**
   * Constructor for testing - allows injecting a mock StsClient.
   *
   * @param roleArn The ARN of the IAM role to assume
   * @param sessionName The session name for the assumed role session
   * @param region The AWS region for STS calls
   * @param stsClient The STS client to use (for testing)
   */
  AwsRoleCredentialsSupplier(
      String roleArn, String sessionName, String region, StsClient stsClient) {
    this.roleArn = roleArn;
    this.sessionName = sessionName;
    this.region = region;
    this.stsClient = stsClient;
  }

  @Override
  public synchronized AwsSecurityCredentials getCredentials(ExternalAccountSupplierContext context)
      throws IOException {
    refreshIfNeeded();
    return new AwsSecurityCredentials(
        currentCredentials.accessKeyId(),
        currentCredentials.secretAccessKey(),
        currentCredentials.sessionToken());
  }

  @Override
  public String getRegion(ExternalAccountSupplierContext context) throws IOException {
    return region;
  }

  private boolean shouldRefresh() {
    if (currentCredentials == null || expirationTime == null) {
      return true;
    }
    // Refresh if we're within the threshold of expiration
    Instant refreshThreshold = Instant.now().plusSeconds(REFRESH_THRESHOLD_SECONDS);
    return refreshThreshold.isAfter(expirationTime);
  }

  private void refreshIfNeeded() throws IOException {
    if (!shouldRefresh()) {
      logger.debug("Using cached credentials, expires at: {}", expirationTime);
      return;
    }

    logger.info("Refreshing AWS credentials by assuming role: {}", roleArn);
    try {
      AssumeRoleResponse response =
          stsClient.assumeRole(
              AssumeRoleRequest.builder()
                  .roleArn(roleArn)
                  .roleSessionName(sessionName)
                  .durationSeconds(SESSION_DURATION_SECONDS)
                  .build());

      currentCredentials = response.credentials();
      expirationTime = currentCredentials.expiration();
      logger.info("AWS credentials refreshed, new expiration: {}", expirationTime);
    } catch (Exception e) {
      throw new IOException("Failed to assume role: " + roleArn, e);
    }
  }

  /** Closes the underlying STS client. */
  public void close() {
    if (stsClient != null) {
      stsClient.close();
    }
  }
}
