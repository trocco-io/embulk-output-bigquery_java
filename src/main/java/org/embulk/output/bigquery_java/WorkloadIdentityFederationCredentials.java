package org.embulk.output.bigquery_java;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.embulk.output.bigquery_java.config.WorkloadIdentityFederationConfig;
import org.embulk.util.config.units.LocalFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkloadIdentityFederationCredentials extends GoogleCredentials {
  private static final Logger logger =
      LoggerFactory.getLogger(WorkloadIdentityFederationCredentials.class);
  private static final Map<CacheKey, WorkloadIdentityFederationCredentials> cache =
      new ConcurrentHashMap<>();

  private final WorkloadIdentityFederationAuth auth;

  private static class CacheKey {
    private final String awsRoleArn;
    private final String awsRegion;
    private final String audience;
    private final Set<String> scopes;

    CacheKey(String awsRoleArn, String awsRegion, String audience, Set<String> scopes) {
      this.awsRoleArn = awsRoleArn;
      this.awsRegion = awsRegion;
      this.audience = audience;
      this.scopes = scopes;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CacheKey cacheKey = (CacheKey) o;
      return Objects.equals(awsRoleArn, cacheKey.awsRoleArn)
          && Objects.equals(awsRegion, cacheKey.awsRegion)
          && Objects.equals(audience, cacheKey.audience)
          && Objects.equals(scopes, cacheKey.scopes);
    }

    @Override
    public int hashCode() {
      return Objects.hash(awsRoleArn, awsRegion, audience, scopes);
    }

    @Override
    public String toString() {
      return String.format(
          "%s:%s:%s:%s", awsRoleArn, awsRegion, audience, String.join(",", scopes));
    }
  }

  public static WorkloadIdentityFederationCredentials getOrCreateByFetchingToken(
      WorkloadIdentityFederationConfig wifConfig, Set<String> scopes) throws IOException {
    JsonObject jsonConfig = parseConfig(wifConfig);
    String audience = jsonConfig.get("audience").getAsString();

    CacheKey cacheKey =
        new CacheKey(wifConfig.getAwsRoleArn(), wifConfig.getAwsRegion(), audience, scopes);

    WorkloadIdentityFederationCredentials cached = cache.get(cacheKey);
    if (cached != null) {
      logger.debug("cache hit for cacheKey: {}", cacheKey);
      return cached;
    }
    logger.debug("cache miss for cacheKey: {}", cacheKey);

    WorkloadIdentityFederationCredentials credentials =
        createByFetchingToken(
            wifConfig.getAwsRoleArn(),
            wifConfig.getAwsRoleSessionName(),
            wifConfig.getAwsRegion(),
            jsonConfig,
            scopes);

    cache.put(cacheKey, credentials);
    return credentials;
  }

  private static JsonObject parseConfig(WorkloadIdentityFederationConfig wifConfig) {
    LocalFile configFile = wifConfig.getConfig();
    String json = new String(configFile.getContent(), StandardCharsets.UTF_8);
    return JsonParser.parseString(json).getAsJsonObject();
  }

  private static WorkloadIdentityFederationCredentials createByFetchingToken(
      String awsRoleArn,
      String awsRoleSessionName,
      String awsRegion,
      JsonObject jsonConfig,
      Set<String> scopes)
      throws IOException {
    logger.info("creating credentials using AssumeRole with role: {}", awsRoleArn);
    String tokenUrl =
        jsonConfig.has("token_url") ? jsonConfig.get("token_url").getAsString() : null;
    String serviceAccountImpersonationUrl =
        jsonConfig.has("service_account_impersonation_url")
            ? jsonConfig.get("service_account_impersonation_url").getAsString()
            : null;

    // Create the AWS role credentials supplier that handles AssumeRole and auto-refresh
    AwsRoleCredentialsSupplier awsRoleCredentialsSupplier =
        new AwsRoleCredentialsSupplier(awsRoleArn, awsRoleSessionName, awsRegion);

    WorkloadIdentityFederationAuth auth =
        new WorkloadIdentityFederationAuth(
            awsRoleCredentialsSupplier,
            awsRegion,
            jsonConfig.get("audience").getAsString(),
            serviceAccountImpersonationUrl,
            tokenUrl,
            scopes);
    AccessToken token = auth.fetchAccessToken();
    return new WorkloadIdentityFederationCredentials(auth, token);
  }

  private WorkloadIdentityFederationCredentials(
      WorkloadIdentityFederationAuth auth, AccessToken accessToken) {
    super(GoogleCredentials.newBuilder().setAccessToken(accessToken));
    this.auth = auth;
  }

  @Override
  public AccessToken refreshAccessToken() throws IOException {
    logger.info("refreshing access token");
    return auth.fetchAccessToken();
  }

  /** Clear the cache. Useful for testing. */
  public static void clearCache() {
    cache.clear();
  }
}
