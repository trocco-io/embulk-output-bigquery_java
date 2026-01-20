package org.embulk.output.bigquery_java;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.AwsSecurityCredentials;
import java.io.IOException;
import java.time.Instant;
import org.junit.Test;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

public class AwsRoleCredentialsSupplierTest {

  private static final String TEST_ROLE_ARN = "arn:aws:iam::123456789012:role/test-role";
  private static final String TEST_SESSION_NAME = "test-session";
  private static final String TEST_REGION = "us-east-1";

  @Test
  public void testGetCredentials_refreshesWhenNeeded() throws IOException {
    // Setup mock STS client
    StsClient mockStsClient = mock(StsClient.class);
    Credentials mockCredentials =
        Credentials.builder()
            .accessKeyId("test-access-key")
            .secretAccessKey("test-secret-key")
            .sessionToken("test-session-token")
            .expiration(Instant.now().plusSeconds(3600))
            .build();
    AssumeRoleResponse mockResponse =
        AssumeRoleResponse.builder().credentials(mockCredentials).build();
    when(mockStsClient.assumeRole(any(AssumeRoleRequest.class))).thenReturn(mockResponse);

    // Create supplier with mock client
    AwsRoleCredentialsSupplier supplier =
        new AwsRoleCredentialsSupplier(
            TEST_ROLE_ARN, TEST_SESSION_NAME, TEST_REGION, mockStsClient);

    // First call should trigger refresh
    AwsSecurityCredentials credentials = supplier.getCredentials(null);
    assertNotNull(credentials);
    assertEquals("test-access-key", credentials.getAccessKeyId());
    assertEquals("test-secret-key", credentials.getSecretAccessKey());

    // Second call should use cached credentials (no refresh)
    AwsSecurityCredentials credentials2 = supplier.getCredentials(null);
    assertNotNull(credentials2);

    // Verify assumeRole was called only once (cached for second call)
    verify(mockStsClient, times(1)).assumeRole(any(AssumeRoleRequest.class));
  }

  @Test
  public void testGetRegion_returnsConfiguredRegion() throws IOException {
    StsClient mockStsClient = mock(StsClient.class);
    AwsRoleCredentialsSupplier supplier =
        new AwsRoleCredentialsSupplier(
            TEST_ROLE_ARN, TEST_SESSION_NAME, TEST_REGION, mockStsClient);

    String region = supplier.getRegion(null);
    assertEquals(TEST_REGION, region);
  }
}
