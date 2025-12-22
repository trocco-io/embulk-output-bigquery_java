package org.embulk.output.bigquery_java;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.embulk.config.ConfigException;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.output.bigquery_java.config.WorkloadIdentityFederationConfig;
import org.embulk.util.config.units.LocalFile;

public class Auth {
  private final String authMethod;
  private final LocalFile jsonKeyFile;
  private final WorkloadIdentityFederationConfig workloadIdentityFederationConfig;

  public Auth(PluginTask task) {
    authMethod = task.getAuthMethod();
    jsonKeyFile = task.getJsonKeyfile().orElse(null);
    workloadIdentityFederationConfig = task.getWorkloadIdentityFederation().orElse(null);
  }

  public Credentials getCredentials(String... scopes) throws IOException {
    return getGoogleCredentials(scopes).createScoped(scopes);
  }

  private GoogleCredentials getGoogleCredentials(String... scopes) throws IOException {
    if ("authorized_user".equalsIgnoreCase(authMethod)) {
      return UserCredentials.fromStream(getCredentialsStream());
    } else if ("service_account".equalsIgnoreCase(authMethod)) {
      return ServiceAccountCredentials.fromStream(getCredentialsStream());
    } else if ("compute_engine".equalsIgnoreCase(authMethod)) {
      return ComputeEngineCredentials.create();
    } else if ("application_default".equalsIgnoreCase(authMethod)) {
      return GoogleCredentials.getApplicationDefault();
    } else if ("workload_identity_federation".equalsIgnoreCase(authMethod)) {
      return getWorkloadIdentityFederationCredentials(scopes);
    } else {
      throw new ConfigException("Unknown auth method: " + authMethod);
    }
  }

  private InputStream getCredentialsStream() {
    if (jsonKeyFile == null) {
      throw new ConfigException(
          "json_keyfile is required when auth_method is '" + authMethod + "'");
    }
    return new ByteArrayInputStream(jsonKeyFile.getContent());
  }

  private WorkloadIdentityFederationCredentials getWorkloadIdentityFederationCredentials(
      String... scopes) throws IOException {
    if (workloadIdentityFederationConfig == null) {
      throw new ConfigException(
          "workload_identity_federation config is required when auth_method is 'workload_identity_federation'");
    }
    Set<String> scopeSet = new HashSet<>(Arrays.asList(scopes));
    return WorkloadIdentityFederationCredentials.getOrCreateByFetchingToken(
        workloadIdentityFederationConfig, scopeSet);
  }
}
