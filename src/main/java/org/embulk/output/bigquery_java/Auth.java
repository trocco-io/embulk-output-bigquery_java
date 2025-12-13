package org.embulk.output.bigquery_java;

import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.embulk.config.ConfigException;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.util.config.units.LocalFile;

public class Auth {
  private final String authMethod;
  private final LocalFile jsonKeyFile;
  private final String accessToken;

  public Auth(PluginTask task) {
    authMethod = task.getAuthMethod();
    jsonKeyFile = task.getJsonKeyfile().orElse(null);
    accessToken = task.getAccessToken().orElse(null);
  }

  public Credentials getCredentials(String... scopes) throws IOException {
    return getGoogleCredentials().createScoped(scopes);
  }

  private GoogleCredentials getGoogleCredentials() throws IOException {
    if ("authorized_user".equalsIgnoreCase(authMethod)) {
      return UserCredentials.fromStream(getCredentialsStream());
    } else if ("service_account".equalsIgnoreCase(authMethod)) {
      return ServiceAccountCredentials.fromStream(getCredentialsStream());
    } else if ("compute_engine".equalsIgnoreCase(authMethod)) {
      return ComputeEngineCredentials.create();
    } else if ("application_default".equalsIgnoreCase(authMethod)) {
      return GoogleCredentials.getApplicationDefault();
    } else if ("access_token".equalsIgnoreCase(authMethod)) {
      return GoogleCredentials.create(new AccessToken(getAccessToken(), null));
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

  private String getAccessToken() {
    if (accessToken == null) {
      throw new ConfigException("access_token is required");
    }
    return accessToken;
  }
}
