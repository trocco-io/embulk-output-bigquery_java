package org.embulk.output.bigquery_java;

import com.google.auth.Credentials;
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

  public Auth(PluginTask task) {
    authMethod = task.getAuthMethod();
    jsonKeyFile = task.getJsonKeyfile();
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
    } else {
      throw new ConfigException("Unknown auth method: " + authMethod);
    }
  }

  private InputStream getCredentialsStream() {
    return new ByteArrayInputStream(jsonKeyFile.getContent());
  }
}
