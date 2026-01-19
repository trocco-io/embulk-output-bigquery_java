package org.embulk.output.bigquery_java;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.AwsCredentials;
import com.google.auth.oauth2.AwsSecurityCredentials;
import com.google.auth.oauth2.AwsSecurityCredentialsSupplier;
import com.google.auth.oauth2.ExternalAccountSupplierContext;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import org.embulk.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkloadIdentityFederationAuth {
  private static final Logger logger =
      LoggerFactory.getLogger(WorkloadIdentityFederationAuth.class);
  private static final int TOKEN_LIFETIME_SECONDS = 3600;

  private final AwsSecurityCredentialsSupplier awsCredentialsSupplier;
  private final String awsRegion;
  private final String audience;
  private final String serviceAccountImpersonationUrl;
  private final String tokenUrl;
  private final Set<String> scopes;

  public WorkloadIdentityFederationAuth(
      AwsSecurityCredentialsSupplier awsCredentialsSupplier,
      String awsRegion,
      String audience,
      String serviceAccountImpersonationUrl,
      String tokenUrl,
      Set<String> scopes) {
    this.awsCredentialsSupplier = awsCredentialsSupplier;
    this.awsRegion = awsRegion;
    this.audience = audience;
    this.serviceAccountImpersonationUrl = serviceAccountImpersonationUrl;
    this.tokenUrl = tokenUrl != null ? tokenUrl : "https://sts.googleapis.com/v1/token";
    this.scopes = scopes;
  }

  public AccessToken fetchAccessToken() throws IOException {
    AccessToken federatedToken = fetchFederatedToken();
    if (serviceAccountImpersonationUrl != null && !serviceAccountImpersonationUrl.isEmpty()) {
      // Service account impersonation mode
      return impersonateServiceAccount(federatedToken);
    } else {
      // Direct access mode (no impersonation)
      return federatedToken;
    }
  }

  private String getServiceAccountEmail() {
    // Extract email from URL like:
    // https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/xxx@yyy.iam.gserviceaccount.com:generateAccessToken
    String[] parts = serviceAccountImpersonationUrl.split("serviceAccounts/");
    if (parts.length < 2) {
      throw new ConfigException(
          String.format(
              "Invalid service_account_impersonation_url: %s", serviceAccountImpersonationUrl));
    }
    return parts[1].replace(":generateAccessToken", "");
  }

  // https://docs.cloud.google.com/iam/docs/reference/sts/rest/v1/TopLevel/token
  // Protected for testing - allows subclasses to provide mock tokens
  protected AccessToken fetchFederatedToken() throws IOException {
    logger.debug("fetching federated token using AwsCredentials");

    // Create AWS security credentials supplier that uses our supplier and provides region
    final AwsSecurityCredentialsSupplier supplierWithRegion =
        new AwsSecurityCredentialsSupplier() {
          @Override
          public AwsSecurityCredentials getCredentials(ExternalAccountSupplierContext context)
              throws IOException {
            return awsCredentialsSupplier.getCredentials(context);
          }

          @Override
          public String getRegion(ExternalAccountSupplierContext context) throws IOException {
            return awsRegion;
          }
        };

    // Build AwsCredentials using the supplier
    AwsCredentials awsCredentials =
        AwsCredentials.newBuilder()
            .setAwsSecurityCredentialsSupplier(supplierWithRegion)
            .setAudience(audience)
            .setTokenUrl(tokenUrl)
            .setSubjectTokenType("urn:ietf:params:aws:token-type:aws4_request")
            .build();

    // Refresh to get the federated token
    awsCredentials.refresh();
    AccessToken accessToken = awsCredentials.getAccessToken();

    logger.debug("federated token obtained");

    return accessToken;
  }

  // https://cloud.google.com/iam/docs/reference/credentials/rest/v1/projects.serviceAccounts/generateAccessToken
  // Protected for testing - allows subclasses to provide mock impersonation
  protected AccessToken impersonateServiceAccount(AccessToken federatedToken) throws IOException {
    String serviceAccountEmail = getServiceAccountEmail();
    logger.debug("impersonating service account: {}", serviceAccountEmail);

    GoogleCredentials sourceCredentials = GoogleCredentials.create(federatedToken);
    ImpersonatedCredentials impersonatedCredentials =
        ImpersonatedCredentials.create(
            sourceCredentials,
            serviceAccountEmail,
            null,
            new ArrayList<>(scopes),
            TOKEN_LIFETIME_SECONDS);

    impersonatedCredentials.refresh();
    AccessToken accessToken = impersonatedCredentials.getAccessToken();
    logger.debug("service account impersonation succeeded");
    return accessToken;
  }
}
