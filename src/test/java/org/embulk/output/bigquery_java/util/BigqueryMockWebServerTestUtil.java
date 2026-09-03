package org.embulk.output.bigquery_java.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.embulk.config.ConfigSource;
import org.embulk.test.TestingEmbulk;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.rules.TemporaryFolder;

// Shared request/response helpers for tests that point BigqueryClient at a MockWebServer standing
// in for the real BigQuery API (via test_host), whether that's exercised through the plugin's
// transaction/output flow or by calling BigqueryClient directly. All response builders assume
// project=project, dataset=dataset unless a table/job id says otherwise.
public final class BigqueryMockWebServerTestUtil {
  private static final String TEST_HOST_RESOURCE_PATH =
      "/java/org/embulk/output/bigquery_java/util/test_host.yml";

  private static final String E2E_INPUT_FILE_NAME = "embulk-output-bigquery_java-e2e-test.csv";

  private BigqueryMockWebServerTestUtil() {}

  // Loads test_host.yml, customized by `customizeConfig` (e.g. to set mode or flip a flag).
  // test_host itself is not set here: callers wire it to a specific MockWebServer separately,
  // since the server's URL isn't known until the server is started.
  public static ConfigSource loadTestHostConfig(
      TestingEmbulk embulk, Function<ConfigSource, ConfigSource> customizeConfig) {
    return customizeConfig.apply(embulk.loadYamlResource(TEST_HOST_RESOURCE_PATH));
  }

  // Starts a MockWebServer, enqueues `responses` on it, runs `action` against it, and returns the
  // requests it recorded, in order. `action` is expected to make exactly responses.length HTTP
  // calls against the server (e.g. via a BigqueryClient built with buildTestHostClient(), or by
  // running the plugin via runOutput()).
  public static List<RecordedRequest> runWithMockServer(
      MockServerAction action, MockResponse... responses) throws Exception {
    try (MockWebServer server = new MockWebServer()) {
      server.start();

      for (MockResponse response : responses) {
        server.enqueue(response);
      }

      action.run(server);

      assertEquals(responses.length, server.getRequestCount());
      List<RecordedRequest> requests = new ArrayList<>();
      for (int i = 0; i < responses.length; i++) {
        requests.add(server.takeRequest());
      }
      return requests;
    }
  }

  // Like runWithMockServer() above, but for an action expected to fail: `expectedFailureType` is
  // the exception type `action` should throw, and `errorMessagePattern` is a regex the thrown
  // exception's message must match.
  public static List<RecordedRequest> runWithMockServerExpectingFailure(
      Class<? extends Exception> expectedFailureType,
      String errorMessagePattern,
      MockServerAction action,
      MockResponse... responses)
      throws Exception {
    return runWithMockServer(
        server -> {
          Exception e = AssertUtil.assertThrows(expectedFailureType, () -> action.run(server));
          AssertUtil.assertMatches(e.getMessage(), errorMessagePattern);
        },
        responses);
  }

  // Runs the plugin with one input row against a MockWebServer standing in for the real BigQuery
  // API, and returns the requests it recorded, in order. outConfig is expected to come from
  // loadTestHostConfig(); test_host is set here once the server is up.
  public static List<RecordedRequest> runWithMockServer(
      TestingEmbulk embulk,
      TemporaryFolder testFolder,
      ConfigSource outConfig,
      List<String> inputLines,
      MockResponse... responses)
      throws Exception {
    return runWithMockServer(e2eRunAction(embulk, testFolder, outConfig, inputLines), responses);
  }

  // Like runWithMockServer() above, but for a run expected to fail: `expectedFailureType` is the
  // exception type the plugin run should throw (e.g. RuntimeException, since transaction() wraps
  // whatever it encounters, such as a job/API failure, in one), and `errorMessagePattern` is a
  // regex the thrown exception's message must match.
  public static List<RecordedRequest> runWithMockServerExpectingFailure(
      TestingEmbulk embulk,
      TemporaryFolder testFolder,
      ConfigSource outConfig,
      List<String> inputLines,
      Class<? extends Exception> expectedFailureType,
      String errorMessagePattern,
      MockResponse... responses)
      throws Exception {
    return runWithMockServerExpectingFailure(
        expectedFailureType,
        errorMessagePattern,
        e2eRunAction(embulk, testFolder, outConfig, inputLines),
        responses);
  }

  private static MockServerAction e2eRunAction(
      TestingEmbulk embulk,
      TemporaryFolder testFolder,
      ConfigSource outConfig,
      List<String> inputLines) {
    return server -> {
      outConfig.set("test_host", server.url("/").toString());
      File in = testFolder.newFile(E2E_INPUT_FILE_NAME);
      Files.write(in.toPath(), inputLines);
      embulk.runOutput(outConfig, in.toPath());
    };
  }

  // An action run against a MockWebServer by runWithMockServer(), after its responses are
  // enqueued and before its recorded requests are collected.
  @FunctionalInterface
  public interface MockServerAction {
    void run(MockWebServer server) throws Exception;
  }

  // The BigQuery client library gzip-compresses request bodies by default.
  private static String readRequestBodyAsString(RecordedRequest request) throws IOException {
    byte[] rawBytes = request.getBody().readByteArray();
    if (!"gzip".equals(request.getHeader("Content-Encoding"))) {
      return new String(rawBytes, StandardCharsets.UTF_8);
    }
    try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(rawBytes))) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buffer = new byte[1024];
      int n;
      while ((n = gzip.read(buffer)) != -1) {
        out.write(buffer, 0, n);
      }
      return out.toString("UTF-8");
    }
  }

  public static JSONObject requestBodyJson(RecordedRequest request) throws IOException {
    return new JSONObject(readRequestBodyAsString(request));
  }

  // The first field of a tables.insert/tables.patch request body's schema (these tests always
  // use a single-column schema, c0).
  public static JSONObject firstSchemaField(JSONObject requestBody) {
    return requestBody.getJSONObject("schema").getJSONArray("fields").getJSONObject(0);
  }

  // Asserts a patched schema field's description/policyTags match expectedDescription/
  // expectedPolicyTag, where null means the field should have neither present.
  public static void assertFieldDescriptionAndPolicyTag(
      JSONObject field, String expectedDescription, String expectedPolicyTag) {
    if (expectedDescription != null) {
      assertEquals(expectedDescription, field.getString("description"));
    } else {
      assertFalse(field.has("description"));
    }
    if (expectedPolicyTag != null) {
      assertEquals(
          expectedPolicyTag, field.getJSONObject("policyTags").getJSONArray("names").getString(0));
    } else {
      assertFalse(field.has("policyTags"));
    }
  }

  // Job configuration bodies for jobs.insert/jobs.get responses, matching the load/copy/query
  // calls these tests exercise (project=project, dataset=dataset, table=table).
  public static final String LOAD_CONFIG_BODY =
      "{\"destinationTable\":{\"projectId\":\"project\",\"datasetId\":\"dataset\","
          + "\"tableId\":\"table\"}}";

  public static final String COPY_CONFIG_BODY =
      "{\"sourceTables\":[{\"projectId\":\"project\",\"datasetId\":\"dataset\","
          + "\"tableId\":\"temp\"}],\"destinationTable\":{\"projectId\":\"project\","
          + "\"datasetId\":\"dataset\",\"tableId\":\"table\"}}";

  public static final String QUERY_CONFIG_BODY = "{\"query\":\"SELECT 1\"}";

  // All request-path assertions below assume project=project, dataset=dataset (see class
  // comment), so those segments are centralized here rather than repeated per method.
  private static final String PROJECT_PATH_PREFIX = "/bigquery/v2/projects/project";
  private static final String TABLE_PATH_PREFIX = PROJECT_PATH_PREFIX + "/datasets/dataset/tables/";

  private static void assertRequest(RecordedRequest request, String method, String path) {
    assertEquals(method, request.getMethod());
    assertEquals(path, request.getPath());
  }

  public static void assertGetDataset(RecordedRequest request) {
    assertRequest(request, "GET", PROJECT_PATH_PREFIX + "/datasets/dataset?prettyPrint=false");
  }

  public static void assertPostTables(RecordedRequest request) {
    assertRequest(
        request, "POST", PROJECT_PATH_PREFIX + "/datasets/dataset/tables?prettyPrint=false");
  }

  public static void assertPostJobs(RecordedRequest request) {
    assertRequest(request, "POST", PROJECT_PATH_PREFIX + "/jobs?prettyPrint=false");
  }

  public static void assertGetJobStatus(RecordedRequest request, String jobId) {
    assertRequest(
        request, "GET", PROJECT_PATH_PREFIX + "/jobs/" + jobId + "?location=US&prettyPrint=false");
  }

  public static void assertGetTable(RecordedRequest request, String tableId) {
    assertRequest(request, "GET", TABLE_PATH_PREFIX + tableId + "?prettyPrint=false");
  }

  public static void assertDeleteTable(RecordedRequest request, String tableId) {
    assertRequest(request, "DELETE", TABLE_PATH_PREFIX + tableId);
  }

  // The BigQuery client library sends table updates as POST with a method-override header rather
  // than a literal HTTP PATCH.
  public static void assertPatchTable(RecordedRequest request, String tableId) {
    assertRequest(request, "POST", TABLE_PATH_PREFIX + tableId + "?prettyPrint=false");
    assertEquals("PATCH", request.getHeader("X-HTTP-Method-Override"));
  }

  public static String tableIdOf(JSONObject json, String key) {
    return json.getJSONObject(key).getString("tableId");
  }

  // Returns the configuration.<configKey> object of a jobs.insert request body (e.g. "load",
  // "copy", "query"), which holds fields such as destinationTable or writeDisposition.
  public static JSONObject jobConfig(RecordedRequest request, String configKey) throws IOException {
    return requestBodyJson(request).getJSONObject("configuration").getJSONObject(configKey);
  }

  // BigqueryClient#copy() always copies from exactly one source table.
  public static String firstSourceTableId(JSONObject copyConfig) {
    JSONArray sourceTables = copyConfig.getJSONArray("sourceTables");
    assertEquals(1, sourceTables.length());
    return sourceTables.getJSONObject(0).getString("tableId");
  }

  public static MockResponse datasetResponse() {
    return new MockResponse()
        .setResponseCode(200)
        .setBody(
            "{\"kind\":\"bigquery#dataset\",\"datasetReference\":"
                + "{\"projectId\":\"project\",\"datasetId\":\"dataset\"}}");
  }

  public static MockResponse tableResponse() {
    return new MockResponse()
        .setResponseCode(200)
        .setBody(
            "{\"kind\":\"bigquery#table\",\"type\":\"TABLE\",\"tableReference\":"
                + "{\"projectId\":\"project\",\"datasetId\":\"dataset\",\"tableId\":\"x\"}}");
  }

  public static MockResponse tableResponseWithSchemaFields(String fieldsJson) {
    return new MockResponse()
        .setResponseCode(200)
        .setBody(
            "{\"kind\":\"bigquery#table\",\"type\":\"TABLE\",\"tableReference\":"
                + "{\"projectId\":\"project\",\"datasetId\":\"dataset\",\"tableId\":\"table\"},"
                + "\"schema\":{\"fields\":"
                + fieldsJson
                + "}}");
  }

  // A c0 schema field with a description and a policy tag set, used as the "old" schema that
  // retain_column_descriptions/retain_column_policy_tags should restore after a replace.
  public static MockResponse tableResponseWithOldDescriptionAndPolicyTag() {
    return tableResponseWithSchemaFields(
        "[{\"name\":\"c0\",\"type\":\"STRING\",\"mode\":\"NULLABLE\","
            + "\"description\":\"old-description\","
            + "\"policyTags\":{\"names\":[\"old-policy-tag\"]}}]");
  }

  // A bare c0 schema field with neither a description nor a policy tag, e.g. the post-replace
  // destination schema before updateTableIfNeed() restores either.
  public static MockResponse tableResponseWithNullableC0() {
    return tableResponseWithSchemaFields(
        "[{\"name\":\"c0\",\"type\":\"STRING\",\"mode\":\"NULLABLE\"}]");
  }

  public static MockResponse deleteResponse() {
    return new MockResponse().setResponseCode(204);
  }

  // A standard BigQuery API error response, e.g. a 409 "Already Exists" or a 400 validation
  // error.
  public static MockResponse errorResponse(int code, String message, String reason) {
    return new MockResponse()
        .setResponseCode(code)
        .setBody(
            "{\"error\":{\"code\":"
                + code
                + ",\"message\":\""
                + message
                + "\",\"errors\":[{\"message\":\""
                + message
                + "\",\"reason\":\""
                + reason
                + "\"}]}}");
  }

  // A job resource as returned by both jobs.insert (POST) and jobs.get (GET, polled by
  // BigqueryJobWaiter). state/errorReason control which branch of BigqueryJobWaiter#waitFor is
  // exercised: state=RUNNING is only meaningful for the POST response (BigqueryJobWaiter always
  // issues at least one GET before checking state); a GET response with state=DONE and a non-null
  // errorReason makes waitFor throw the exception matching that reason.
  public static MockResponse jobResponse(
      String jobId, String configKey, String configBody, String state, String errorReason) {
    StringBuilder status = new StringBuilder("{\"state\":\"").append(state).append("\"");
    if (errorReason != null) {
      status
          .append(",\"errorResult\":{\"reason\":\"")
          .append(errorReason)
          .append("\",\"message\":\"boom\"}")
          .append(",\"errors\":[{\"reason\":\"")
          .append(errorReason)
          .append("\",\"message\":\"boom\"}]");
    }
    status.append("}");
    return new MockResponse()
        .setResponseCode(200)
        .setBody(
            "{\"kind\":\"bigquery#job\",\"jobReference\":"
                + "{\"projectId\":\"project\",\"jobId\":\""
                + jobId
                + "\",\"location\":\"US\"},"
                + "\"configuration\":{\""
                + configKey
                + "\":"
                + configBody
                + "},"
                + "\"status\":"
                + status
                + ",\"statistics\":{\"creationTime\":\"0\",\"startTime\":\"0\",\"endTime\":\"0\"}}");
  }

  // A completed job, as returned both by the jobs.insert response (test_host branch of
  // BigqueryClient#load, or the plain jobs.insert path used by copy/merge) and by the subsequent
  // jobs.get poll in BigqueryJobWaiter.
  private static MockResponse jobDoneResponse(String jobId, String configKey, String configBody) {
    return jobResponse(jobId, configKey, configBody, "DONE", null);
  }

  private static MockResponse loadJobDoneResponse(String jobId) {
    return jobDoneResponse(
        jobId,
        "load",
        "{\"destinationTable\":"
            + "{\"projectId\":\"project\",\"datasetId\":\"dataset\",\"tableId\":\"table\"}}");
  }

  // The response to jobs.insert when creating the load job.
  public static MockResponse createLoadJobResponse(String jobId) {
    return loadJobDoneResponse(jobId);
  }

  // The response to the jobs.get poll (BigqueryJobWaiter) that observes the load job as DONE.
  public static MockResponse waitForLoadJobResponse(String jobId) {
    return loadJobDoneResponse(jobId);
  }

  private static MockResponse copyJobDoneResponse(String jobId) {
    return jobDoneResponse(
        jobId,
        "copy",
        "{\"sourceTables\":[{\"projectId\":\"project\",\"datasetId\":\"dataset\",\"tableId\":\"temp\"}],"
            + "\"destinationTable\":"
            + "{\"projectId\":\"project\",\"datasetId\":\"dataset\",\"tableId\":\"table\"}}");
  }

  // The response to jobs.insert when creating the copy job.
  public static MockResponse createCopyJobResponse(String jobId) {
    return copyJobDoneResponse(jobId);
  }

  // The response to the jobs.get poll (BigqueryJobWaiter) that observes the copy job as DONE.
  public static MockResponse waitForCopyJobResponse(String jobId) {
    return copyJobDoneResponse(jobId);
  }

  private static MockResponse queryJobDoneResponse(String jobId) {
    return jobDoneResponse(jobId, "query", "{\"query\":\"MERGE\"}");
  }

  // The response to jobs.insert when creating the query job.
  public static MockResponse createQueryJobResponse(String jobId) {
    return queryJobDoneResponse(jobId);
  }

  // The response to the jobs.get poll (BigqueryJobWaiter) that observes the query job as DONE.
  public static MockResponse waitForQueryJobResponse(String jobId) {
    return queryJobDoneResponse(jobId);
  }
}
