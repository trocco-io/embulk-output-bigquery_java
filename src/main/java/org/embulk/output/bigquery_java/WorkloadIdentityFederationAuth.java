package org.embulk.output.bigquery_java;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.embulk.config.ConfigException;
import org.embulk.output.bigquery_java.config.WorkloadIdentityFederationConfig;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkloadIdentityFederationAuth {
  private static final Logger logger =
      LoggerFactory.getLogger(WorkloadIdentityFederationAuth.class);
  private static final int TOKEN_LIFETIME_SECONDS = 3600;

  private final String awsAccessKeyId;
  private final String awsSecretAccessKey;
  private final String awsRegion;
  private final String audience;
  private final String serviceAccountImpersonationUrl;
  private final String tokenUrl;

  public WorkloadIdentityFederationAuth(WorkloadIdentityFederationConfig config) {
    this.awsAccessKeyId = config.getAwsAccessKeyId();
    this.awsSecretAccessKey = config.getAwsSecretAccessKey();
    this.awsRegion = config.getAwsRegion();

    JSONObject jsonConfig =
        new JSONObject(
            new JSONTokener(new ByteArrayInputStream(config.getConfig().getContent())));
    this.audience = jsonConfig.getString("audience");
    this.serviceAccountImpersonationUrl = jsonConfig.getString("service_account_impersonation_url");
    this.tokenUrl = jsonConfig.optString("token_url", "https://sts.googleapis.com/v1/token");
  }

  public WorkloadIdentityFederationCredentials getCredentials() throws IOException {
    AccessToken token = getAccessToken();
    return new WorkloadIdentityFederationCredentials(this, token);
  }

  public AccessToken getAccessToken() throws IOException {
    Map<String, Object> awsRequest = createAwsSignedRequest();
    String federatedToken = exchangeTokenForGoogleAccessToken(awsRequest);
    String accessToken = impersonateServiceAccount(federatedToken);
    java.util.Date expirationTime =
        new java.util.Date(System.currentTimeMillis() + TOKEN_LIFETIME_SECONDS * 1000);
    return new AccessToken(accessToken, expirationTime);
  }

  private String getServiceAccountEmail() {
    // Extract email from URL like:
    // https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/xxx@yyy.iam.gserviceaccount.com:generateAccessToken
    String[] parts = serviceAccountImpersonationUrl.split("serviceAccounts/");
    if (parts.length < 2) {
      throw new ConfigException(
          "Invalid service_account_impersonation_url: " + serviceAccountImpersonationUrl);
    }
    return parts[1].replace(":generateAccessToken", "");
  }

  private Map<String, Object> createAwsSignedRequest() {
    String service = "sts";
    String host = "sts." + awsRegion + ".amazonaws.com";
    String method = "POST";

    ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    String amzDate = now.format(DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'"));
    String dateStamp = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

    String queryParams = "Action=GetCallerIdentity&Version=2011-06-15";
    String endpoint = "https://" + host + "/?" + queryParams;

    String payloadHash = sha256Hex("");

    Map<String, String> headers = new HashMap<>();
    headers.put("host", host);
    headers.put("x-amz-date", amzDate);
    headers.put("x-goog-cloud-target-resource", audience);

    List<String> signedHeadersList = new ArrayList<>(headers.keySet());
    Collections.sort(signedHeadersList);
    String signedHeaders = String.join(";", signedHeadersList);

    String canonicalHeaders =
        signedHeadersList.stream()
            .map(k -> k + ":" + headers.get(k) + "\n")
            .collect(Collectors.joining());

    String[] queryParts = queryParams.split("&");
    java.util.Arrays.sort(queryParts);
    String canonicalQuerystring = String.join("&", queryParts);

    String canonicalRequest =
        String.join(
            "\n", method, "/", canonicalQuerystring, canonicalHeaders, signedHeaders, payloadHash);

    String algorithm = "AWS4-HMAC-SHA256";
    String credentialScope = dateStamp + "/" + awsRegion + "/" + service + "/aws4_request";

    String stringToSign =
        String.join("\n", algorithm, amzDate, credentialScope, sha256Hex(canonicalRequest));

    byte[] signingKey = getSignatureKey(dateStamp, awsRegion, service);
    String signature = hmacSha256Hex(signingKey, stringToSign);

    String authorizationHeader =
        algorithm
            + " Credential="
            + awsAccessKeyId
            + "/"
            + credentialScope
            + ", SignedHeaders="
            + signedHeaders
            + ", Signature="
            + signature;

    List<Map<String, String>> requestHeaders = new ArrayList<>();
    for (String key : signedHeadersList) {
      Map<String, String> header = new HashMap<>();
      header.put("key", key);
      header.put("value", headers.get(key));
      requestHeaders.add(header);
    }
    Map<String, String> authHeader = new HashMap<>();
    authHeader.put("key", "Authorization");
    authHeader.put("value", authorizationHeader);
    requestHeaders.add(authHeader);

    Map<String, Object> result = new HashMap<>();
    result.put("url", endpoint);
    result.put("method", method);
    result.put("headers", requestHeaders);
    return result;
  }

  private byte[] getSignatureKey(String dateStamp, String region, String service) {
    byte[] kDate = hmacSha256(("AWS4" + awsSecretAccessKey).getBytes(StandardCharsets.UTF_8), dateStamp);
    byte[] kRegion = hmacSha256(kDate, region);
    byte[] kService = hmacSha256(kRegion, service);
    return hmacSha256(kService, "aws4_request");
  }

  private String exchangeTokenForGoogleAccessToken(Map<String, Object> awsRequest)
      throws IOException {
    JSONObject awsRequestJson = new JSONObject(awsRequest);
    String subjectToken = URLEncoder.encode(awsRequestJson.toString(), "UTF-8");

    String data =
        "grant_type="
            + URLEncoder.encode("urn:ietf:params:oauth:grant-type:token-exchange", "UTF-8")
            + "&audience="
            + URLEncoder.encode(audience, "UTF-8")
            + "&scope="
            + URLEncoder.encode("https://www.googleapis.com/auth/cloud-platform", "UTF-8")
            + "&requested_token_type="
            + URLEncoder.encode("urn:ietf:params:oauth:token-type:access_token", "UTF-8")
            + "&subject_token_type="
            + URLEncoder.encode("urn:ietf:params:aws:token-type:aws4_request", "UTF-8")
            + "&subject_token="
            + subjectToken;

    URL url = new URL(tokenUrl);
    logger.debug("Exchanging token at: {}", tokenUrl);

    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
    conn.setDoOutput(true);

    try (OutputStream os = conn.getOutputStream()) {
      os.write(data.getBytes(StandardCharsets.UTF_8));
    }

    int responseCode = conn.getResponseCode();
    logger.debug("Token exchange response code: {}", responseCode);

    if (responseCode != 200) {
      String errorBody;
      try (Scanner scanner = new Scanner(conn.getErrorStream(), "UTF-8")) {
        errorBody = scanner.useDelimiter("\\A").hasNext() ? scanner.next() : "";
      }
      logger.info("Token exchange failed: {} - {}", responseCode, errorBody);
      throw new IOException(
          "Google STS token exchange failed: " + responseCode + " - " + errorBody);
    }

    String responseBody;
    try (Scanner scanner = new Scanner(conn.getInputStream(), "UTF-8")) {
      responseBody = scanner.useDelimiter("\\A").hasNext() ? scanner.next() : "";
    }
    logger.info("Token exchange succeeded");

    JSONObject responseJson = new JSONObject(responseBody);
    if (responseJson.has("expires_in")) {
      logger.debug("Token expires_in: {}", responseJson.get("expires_in"));
    }
    if (responseJson.has("token_type")) {
      logger.debug("Token type: {}", responseJson.getString("token_type"));
    }
    if (responseJson.has("issued_token_type")) {
      logger.debug("Issued token type: {}", responseJson.getString("issued_token_type"));
    }
    return responseJson.getString("access_token");
  }

  private String impersonateServiceAccount(String federatedToken) throws IOException {
    String serviceAccountEmail = getServiceAccountEmail();
    String impersonationUrl =
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/"
            + serviceAccountEmail
            + ":generateAccessToken";
    logger.debug("Impersonating service account: {}", serviceAccountEmail);
    logger.debug("Impersonation URL: {}", impersonationUrl);

    URL url = new URL(impersonationUrl);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization", "Bearer " + federatedToken);
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setDoOutput(true);

    JSONObject requestBody = new JSONObject();
    requestBody.put("scope", new String[] {"https://www.googleapis.com/auth/bigquery"});
    requestBody.put("lifetime", TOKEN_LIFETIME_SECONDS + "s");

    try (OutputStream os = conn.getOutputStream()) {
      os.write(requestBody.toString().getBytes(StandardCharsets.UTF_8));
    }

    int responseCode = conn.getResponseCode();
    logger.debug("Impersonation response code: {}", responseCode);

    if (responseCode != 200) {
      String errorBody;
      try (Scanner scanner = new Scanner(conn.getErrorStream(), "UTF-8")) {
        errorBody = scanner.useDelimiter("\\A").hasNext() ? scanner.next() : "";
      }
      logger.info("Impersonation failed: {} - {}", responseCode, errorBody);
      throw new IOException(
          "Service account impersonation failed: " + responseCode + " - " + errorBody);
    }

    String responseBody;
    try (Scanner scanner = new Scanner(conn.getInputStream(), "UTF-8")) {
      responseBody = scanner.useDelimiter("\\A").hasNext() ? scanner.next() : "";
    }
    logger.info("Service account impersonation succeeded");

    JSONObject responseJson = new JSONObject(responseBody);
    if (responseJson.has("expireTime")) {
      logger.debug("Access token expire time: {}", responseJson.getString("expireTime"));
    }
    return responseJson.getString("accessToken");
  }

  private String sha256Hex(String data) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(data.getBytes(StandardCharsets.UTF_8));
      return bytesToHex(hash);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 algorithm not found", e);
    }
  }

  private byte[] hmacSha256(byte[] key, String data) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(key, "HmacSHA256"));
      return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      throw new RuntimeException("HMAC-SHA256 error", e);
    }
  }

  private String hmacSha256Hex(byte[] key, String data) {
    return bytesToHex(hmacSha256(key, data));
  }

  private String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }
}
