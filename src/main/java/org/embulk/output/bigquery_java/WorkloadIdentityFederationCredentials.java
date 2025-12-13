package org.embulk.output.bigquery_java;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;

public class WorkloadIdentityFederationCredentials extends GoogleCredentials {
  private final WorkloadIdentityFederationAuth auth;

  public WorkloadIdentityFederationCredentials(
      WorkloadIdentityFederationAuth auth, AccessToken accessToken) {
    super(accessToken);
    this.auth = auth;
  }

  @Override
  public AccessToken refreshAccessToken() throws IOException {
    return auth.getAccessToken();
  }
}
