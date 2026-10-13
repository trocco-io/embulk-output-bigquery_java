package org.embulk.output.bigquery_java.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.QueueDispatcher;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
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

  // Stands in for the MockWebServer's scheme://host:port in responses built before the server is
  // started; runWithMockServer() substitutes the real one when enqueueing.
  private static final String TEST_HOST_PLACEHOLDER = "{{test_host}}";

  private static final String RESUMABLE_UPLOAD_PATH =
      "/upload/bigquery/v2/projects/project/jobs?uploadType=resumable";
  private static final String RESUMABLE_UPLOAD_ID = "test-upload";

  // BaseWriteChannel.DEFAULT_CHUNK_SIZE (google-cloud-core 2.8.0): a file up to this size is
  // uploaded as one PUT; a larger one as 15 MB chunks (answered with 308) plus a final chunk.
  public static final long UPLOAD_CHUNK_SIZE = 15L * 1024 * 1024;

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
      // A request with no enqueued response is a test bug: answer it right away instead of
      // leaving the client waiting for its read timeout.
      QueueDispatcher dispatcher = new QueueDispatcher();
      dispatcher.setFailFast(
          new MockResponse().setResponseCode(599).setBody("no response enqueued for this request"));
      server.setDispatcher(dispatcher);
      server.start();

      for (MockResponse response : responses) {
        // A resumable upload session's Location must be an absolute URL, and the server's URL is
        // only known now; see startResumableUploadResponse().
        String location = response.getHeaders().get("Location");
        if (location != null && location.contains(TEST_HOST_PLACEHOLDER)) {
          String testHost = "http://" + server.getHostName() + ":" + server.getPort();
          response.setHeader("Location", location.replace(TEST_HOST_PLACEHOLDER, testHost));
        }
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
    return gunzip(rawBytes);
  }

  private static String gunzip(byte[] rawBytes) throws IOException {
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

  // The POST that opens the resumable upload session. Its body is the load job (jobReference +
  // configuration.load), so jobConfig(request, "load") applies to it.
  public static void assertStartResumableUpload(RecordedRequest request) {
    assertRequest(request, "POST", RESUMABLE_UPLOAD_PATH);
  }

  // The PUT of the whole file as the single, final chunk of the resumable upload session opened
  // by startResumableUploadResponse(). A file larger than the library's chunk size would be sent
  // as several PUTs (the intermediate ones answered with 308), which these helpers don't model.
  public static void assertUploadLoadFile(RecordedRequest request) {
    assertRequest(request, "PUT", RESUMABLE_UPLOAD_PATH + "&upload_id=" + RESUMABLE_UPLOAD_ID);
    long size = request.getBodySize();
    assertTrue(
        "uploaded " + size + " bytes, more than one chunk; see assertUploadLoadFile()",
        size <= UPLOAD_CHUNK_SIZE);
    assertEquals("bytes 0-" + (size - 1) + "/" + size, request.getHeader("Content-Range"));
  }

  // One chunk PUT of a multi-chunk upload (a file over UPLOAD_CHUNK_SIZE): Content-Range is
  // "bytes from-to/*" for an intermediate chunk (total < 0) and "bytes from-to/total" for the last.
  public static void assertUploadChunk(RecordedRequest request, long from, long to, long total) {
    assertRequest(request, "PUT", RESUMABLE_UPLOAD_PATH + "&upload_id=" + RESUMABLE_UPLOAD_ID);
    String totalPart = total < 0 ? "*" : String.valueOf(total);
    assertEquals("bytes " + from + "-" + to + "/" + totalPart, request.getHeader("Content-Range"));
  }

  // The 3 requests matching loadAttemptResponses(), starting at requests.get(offset).
  public static void assertLoadAttempt(List<RecordedRequest> requests, int offset, String jobId) {
    assertStartResumableUpload(requests.get(offset));
    assertUploadLoadFile(requests.get(offset + 1));
    assertGetJobStatus(requests.get(offset + 2), jobId);
  }

  // The lines of the intermediate file as uploaded by assertUploadLoadFile()'s request. The body
  // is the file itself; `gzipped` states what the test's compression setting demands of it
  // (true for compression: GZIP, test_host.yml's default), so a plain body fails to gunzip.
  public static List<String> uploadedLines(RecordedRequest request, boolean gzipped)
      throws IOException {
    byte[] body = request.getBody().readByteArray();
    String content = gzipped ? gunzip(body) : new String(body, StandardCharsets.UTF_8);
    return Arrays.asList(content.split("\n"));
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

  // The job id BigqueryClient#load() chose, as sent in the POST that opens the resumable upload
  // session (assertStartResumableUpload()): compare it across attempts to tell a re-upload under
  // the same job from a new job.
  public static String jobIdOf(RecordedRequest openUpload) throws IOException {
    return requestBodyJson(openUpload).getJSONObject("jobReference").getString("jobId");
  }

  // Returns the configuration.<configKey> object of a job request body (e.g. "load", "copy",
  // "query"), which holds fields such as destinationTable or writeDisposition. The request is
  // jobs.insert for copy/query, or the POST that opens the resumable upload session for load.
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

  // A table with numRows set, e.g. the temp table row count
  // BigqueryJavaOutputPlugin#getTransactionReport() reads when is_skip_job_result_check is false.
  public static MockResponse tableResponseWithNumRows(long numRows) {
    return new MockResponse()
        .setResponseCode(200)
        .setBody(
            "{\"kind\":\"bigquery#table\",\"type\":\"TABLE\",\"tableReference\":"
                + "{\"projectId\":\"project\",\"datasetId\":\"dataset\",\"tableId\":\"x\"},"
                + "\"numRows\":\""
                + numRows
                + "\"}");
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

  // A job resource as returned by the request that creates the job (jobs.insert POST, or the
  // final PUT of a load's resumable upload) and by jobs.get (GET, polled by
  // BigqueryJobWaiter). state/errorReason control which branch of BigqueryJobWaiter#waitFor is
  // exercised: state=RUNNING is only meaningful for the POST response (BigqueryJobWaiter always
  // issues at least one GET before checking state); a GET response with state=DONE and a non-null
  // errorReason makes waitFor throw the exception matching that reason.
  public static MockResponse jobResponse(
      String jobId, String configKey, String configBody, String state, String errorReason) {
    return jobResponse(jobId, configKey, configBody, state, errorReason, null);
  }

  // Like the 5-arg jobResponse() above, but statisticsFields, if non-null, is merged into the
  // job's statistics object (e.g. a load job's outputRows, read by
  // BigqueryJavaOutputPlugin#getTransactionReport() when is_skip_job_result_check is false).
  private static MockResponse jobResponse(
      String jobId,
      String configKey,
      String configBody,
      String state,
      String errorReason,
      String statisticsFields) {
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
    String statistics =
        "{\"creationTime\":\"0\",\"startTime\":\"0\",\"endTime\":\"0\""
            + (statisticsFields != null ? "," + statisticsFields : "")
            + "}";
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
                + ",\"statistics\":"
                + statistics
                + "}");
  }

  // A completed job, as returned both by the request that creates it (the final chunk of the
  // resumable upload for load, or jobs.insert for copy/merge) and by the subsequent jobs.get poll
  // in BigqueryJobWaiter.
  private static MockResponse jobDoneResponse(String jobId, String configKey, String configBody) {
    return jobResponse(jobId, configKey, configBody, "DONE", null);
  }

  private static MockResponse loadJobDoneResponse(String jobId) {
    return jobResponse(
        jobId,
        "load",
        "{\"destinationTable\":"
            + "{\"projectId\":\"project\",\"datasetId\":\"dataset\",\"tableId\":\"table\"}}",
        "DONE",
        null,
        "\"load\":{\"outputRows\":\"1\"}");
  }

  // The response that opens the resumable upload session (BigqueryClient#load's
  // bigquery.writer()): the Location is where the file's chunks get PUT.
  public static MockResponse startResumableUploadResponse() {
    return new MockResponse()
        .setResponseCode(200)
        .setHeader(
            "Location",
            TEST_HOST_PLACEHOLDER + RESUMABLE_UPLOAD_PATH + "&upload_id=" + RESUMABLE_UPLOAD_ID);
  }

  // The response to an intermediate chunk PUT (a file over UPLOAD_CHUNK_SIZE): "resume
  // incomplete", so the channel sends the next chunk.
  public static MockResponse uploadChunkAcceptedResponse() {
    return new MockResponse().setResponseCode(308);
  }

  // Simulates a connection reset while a chunk is being PUT. The client sees an IOException,
  // which the library treats as retryable: it re-sends the same chunk on the same session up to
  // `retries` more times (the next enqueued responses answer those PUTs). Other policies don't
  // behave like a network failure: DISCONNECT_AT_START and DISCONNECT_DURING_RESPONSE_BODY
  // surface as an unparseable (empty) response that is not retried, and NO_RESPONSE blocks until
  // the read timeout (read_timeout_sec: 1 in test_host.yml).
  public static MockResponse disconnectDuringUploadResponse() {
    return new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_DURING_REQUEST_BODY);
  }

  // Never answers the request: the client gives up after its read timeout (read_timeout_sec: 1
  // in test_host.yml), which the library treats as retryable. Each use costs that second.
  public static MockResponse unansweredResponse() {
    return new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE);
  }

  // Writes JSONL rows to `file` until it holds at least `minBytes` (plain, not gzipped), e.g. to
  // get an upload that spans more than one chunk; returns the file's size.
  public static long writeLoadFile(Path file, long minBytes) throws IOException {
    byte[] line =
        "{\"c0\":\"0123456789abcdef0123456789abcdef0123456789abcdef\"}\n"
            .getBytes(StandardCharsets.UTF_8);
    try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(file))) {
      for (long written = 0; written < minBytes; written += line.length) {
        out.write(line);
      }
    }
    return Files.size(file);
  }

  // One attempt of BigqueryClient#load() as the server sees it, 3 responses: the POST that opens
  // the resumable upload session, the PUT of the file (which creates the job, still running),
  // and the jobs.get poll (BigqueryJobWaiter) that reports the job DONE: succeeded with 1 output
  // row when errorReason is null, failed with that reason otherwise. assertLoadAttempt() checks
  // the matching requests.
  public static MockResponse[] loadAttemptResponses(String jobId, String errorReason) {
    return new MockResponse[] {
      startResumableUploadResponse(),
      uploadLoadFileResponse(jobId),
      waitForLoadJobResponse(jobId, errorReason)
    };
  }

  // The response to the final chunk PUT: the load job it created, still running.
  public static MockResponse uploadLoadFileResponse(String jobId) {
    return jobResponse(jobId, "load", LOAD_CONFIG_BODY, "RUNNING", null);
  }

  // The response to the jobs.get poll (BigqueryJobWaiter): the load job DONE, succeeded with 1
  // output row when errorReason is null, failed with that reason otherwise.
  public static MockResponse waitForLoadJobResponse(String jobId, String errorReason) {
    return errorReason == null
        ? loadJobDoneResponse(jobId)
        : jobResponse(jobId, "load", LOAD_CONFIG_BODY, "DONE", errorReason);
  }

  // Builds the response list for runWithMockServer() when it includes whole load attempts
  // (loadAttemptResponses()) among single responses, e.g.
  //   MockResponses.of(datasetResponse()).add(loadAttemptResponses("job", null)).toArray()
  public static final class MockResponses {
    private final List<MockResponse> list = new ArrayList<>();

    private MockResponses() {}

    public static MockResponses of(MockResponse... responses) {
      return new MockResponses().add(responses);
    }

    public MockResponses add(MockResponse... responses) {
      list.addAll(Arrays.asList(responses));
      return this;
    }

    public MockResponse[] toArray() {
      return list.toArray(new MockResponse[0]);
    }
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
