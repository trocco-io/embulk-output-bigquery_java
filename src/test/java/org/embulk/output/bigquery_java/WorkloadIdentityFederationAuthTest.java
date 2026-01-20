package org.embulk.output.bigquery_java;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.AwsSecurityCredentials;
import com.google.auth.oauth2.AwsSecurityCredentialsSupplier;
import com.google.auth.oauth2.ExternalAccountSupplierContext;
import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class WorkloadIdentityFederationAuthTest {

  private static final String TEST_AWS_REGION = "us-east-1";
  private static final String TEST_AUDIENCE =
      "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider";
  private static final String TEST_SERVICE_ACCOUNT_IMPERSONATION_URL =
      "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test@project.iam.gserviceaccount.com:generateAccessToken";

  private Set<String> createTestScopes() {
    Set<String> scopes = new HashSet<>();
    scopes.add("https://www.googleapis.com/auth/cloud-platform");
    return scopes;
  }

  private AwsSecurityCredentialsSupplier createMockSupplier() {
    return new AwsSecurityCredentialsSupplier() {
      @Override
      public AwsSecurityCredentials getCredentials(ExternalAccountSupplierContext context)
          throws IOException {
        return new AwsSecurityCredentials(
            "assumed-access-key", "assumed-secret-key", "assumed-session-token");
      }

      @Override
      public String getRegion(ExternalAccountSupplierContext context) throws IOException {
        return TEST_AWS_REGION;
      }
    };
  }

  @Test
  public void testDirectAccessMode_returnsDirectFederatedToken() throws IOException {
    // When serviceAccountImpersonationUrl is null, direct access mode should be used
    AccessToken expectedToken = new AccessToken("federated-token", new Date());

    WorkloadIdentityFederationAuth auth =
        new TestableWorkloadIdentityFederationAuth(
            createMockSupplier(),
            TEST_AWS_REGION,
            TEST_AUDIENCE,
            null, // Direct access mode - no impersonation URL
            null,
            createTestScopes(),
            expectedToken,
            null);

    AccessToken result = auth.fetchAccessToken();

    assertNotNull(result);
    assertEquals("federated-token", result.getTokenValue());
  }

  @Test
  public void testDirectAccessMode_withEmptyUrl_returnsDirectFederatedToken() throws IOException {
    // When serviceAccountImpersonationUrl is empty string, direct access mode should be used
    AccessToken expectedToken = new AccessToken("federated-token-empty", new Date());

    WorkloadIdentityFederationAuth auth =
        new TestableWorkloadIdentityFederationAuth(
            createMockSupplier(),
            TEST_AWS_REGION,
            TEST_AUDIENCE,
            "", // Empty string - should also use direct access mode
            null,
            createTestScopes(),
            expectedToken,
            null);

    AccessToken result = auth.fetchAccessToken();

    assertNotNull(result);
    assertEquals("federated-token-empty", result.getTokenValue());
  }

  @Test
  public void testImpersonationMode_returnsImpersonatedToken() throws IOException {
    // When serviceAccountImpersonationUrl is provided, impersonation mode should be used
    AccessToken federatedToken = new AccessToken("federated-token", new Date());
    AccessToken impersonatedToken = new AccessToken("impersonated-token", new Date());

    WorkloadIdentityFederationAuth auth =
        new TestableWorkloadIdentityFederationAuth(
            createMockSupplier(),
            TEST_AWS_REGION,
            TEST_AUDIENCE,
            TEST_SERVICE_ACCOUNT_IMPERSONATION_URL,
            null,
            createTestScopes(),
            federatedToken,
            impersonatedToken);

    AccessToken result = auth.fetchAccessToken();

    assertNotNull(result);
    assertEquals("impersonated-token", result.getTokenValue());
  }

  /**
   * Test subclass that allows overriding the token fetching methods to return mock tokens without
   * making actual network calls.
   */
  private static class TestableWorkloadIdentityFederationAuth
      extends WorkloadIdentityFederationAuth {

    private final AccessToken mockFederatedToken;
    private final AccessToken mockImpersonatedToken;

    public TestableWorkloadIdentityFederationAuth(
        AwsSecurityCredentialsSupplier awsCredentialsSupplier,
        String awsRegion,
        String audience,
        String serviceAccountImpersonationUrl,
        String tokenUrl,
        Set<String> scopes,
        AccessToken mockFederatedToken,
        AccessToken mockImpersonatedToken) {
      super(
          awsCredentialsSupplier,
          awsRegion,
          audience,
          serviceAccountImpersonationUrl,
          tokenUrl,
          scopes);
      this.mockFederatedToken = mockFederatedToken;
      this.mockImpersonatedToken = mockImpersonatedToken;
    }

    @Override
    protected AccessToken fetchFederatedToken() throws IOException {
      return mockFederatedToken;
    }

    @Override
    protected AccessToken impersonateServiceAccount(AccessToken federatedToken) throws IOException {
      return mockImpersonatedToken;
    }
  }
}
