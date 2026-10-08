package org.embulk.output.bigquery_java;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.NoCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.PolicyTags;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import com.google.cloud.bigquery.spi.v2.BigQueryRpc;
import com.google.cloud.spi.ServiceRpcFactory;
import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.embulk.config.ConfigSource;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.output.bigquery_java.exception.BigqueryBackendException;
import org.embulk.output.bigquery_java.exception.BigqueryException;
import org.embulk.output.bigquery_java.exception.BigqueryUploadException;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.test.TestingEmbulk;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;
import org.mockito.verification.VerificationMode;
import org.slf4j.Logger;

public class TestBigqueryClient {
  protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();

  protected static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

  private static final String BASIC_RESOURCE_PATH =
      "/java/org/embulk/output/bigquery_java/bigquery_client/";

  private static ConfigSource loadYamlResource(TestingEmbulk embulk, String fileName) {
    return embulk.loadYamlResource(BASIC_RESOURCE_PATH + fileName);
  }

  @Rule
  public TestingEmbulk embulk =
      TestingEmbulk.builder()
          .registerPlugin(OutputPlugin.class, "bigquery_java", BigqueryJavaOutputPlugin.class)
          .registerPlugin(FileInputPlugin.class, "file", LocalFileInputPlugin.class)
          .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
          .build();

  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  private static StandardSQLTypeName toBQType(BigqueryColumnOption column) {
    Map<String, StandardSQLTypeName> bqTypeMap = new HashMap<>();
    bqTypeMap.put("INTEGER", StandardSQLTypeName.INT64);
    bqTypeMap.put("STRING", StandardSQLTypeName.STRING);
    return bqTypeMap.getOrDefault(column.getType().orElse("STRING"), StandardSQLTypeName.STRING);
  }

  @Test
  public void TestIsNeedUpdateTable() {
    ConfigSource baseConfig = loadYamlResource(embulk, "takeover.yml");

    // Helper function to create config and test isNeedUpdateTable
    Function<String, Function<Boolean, Function<Boolean, Boolean>>> testIsNeedUpdate =
        mode ->
            retainPolicyTags ->
                retainDescriptions ->
                    BigqueryClient.isNeedUpdateTable(
                        CONFIG_MAPPER.map(
                            baseConfig
                                .set("mode", mode)
                                .set("retain_column_policy_tags", retainPolicyTags)
                                .set("retain_column_descriptions", retainDescriptions),
                            PluginTask.class));

    // Test cases
    assertTrue(testIsNeedUpdate.apply("replace").apply(false).apply(true));
    assertTrue(testIsNeedUpdate.apply("replace").apply(true).apply(false));
    assertFalse(testIsNeedUpdate.apply("replace").apply(false).apply(false));
    assertFalse(testIsNeedUpdate.apply("insert").apply(true).apply(true));
  }

  private Schema invokeTakeoverBuildSchema(
      Function<ConfigSource, ConfigSource> setupConfig,
      Function<Field.Builder, Field> setupField0,
      Function<Field.Builder, Field> setupField1) {
    ConfigSource config = loadYamlResource(embulk, "takeover.yml");
    PluginTask baseTask = CONFIG_MAPPER.map(config, PluginTask.class);
    PluginTask task = CONFIG_MAPPER.map(setupConfig.apply(config), PluginTask.class);

    List<Field> currentFields =
        baseTask.getColumnOptions().orElse(java.util.Collections.emptyList()).stream()
            .map(column -> Field.newBuilder(column.getName(), toBQType(column)).build())
            .collect(Collectors.toList());

    HashMap<String, Function<Field.Builder, Field>> setups = new HashMap<>();
    setups.put("c0", setupField0);
    setups.put("c1", setupField1);

    List<Field> fieldList =
        baseTask.getColumnOptions().orElse(java.util.Collections.emptyList()).stream()
            .map(c -> setups.get(c.getName()).apply(Field.newBuilder(c.getName(), toBQType(c))))
            .collect(Collectors.toList());

    return BigqueryClient.buildPatchSchema(
        task, FieldList.of(currentFields), FieldList.of(fieldList));
  }

  private Schema invokeRetainDescriptionBuildSchema(
      String mode, Boolean retainColumnDescriptions, String d0, String d1) {
    return invokeRetainDescriptionBuildSchema(
        configSource -> configSource, mode, retainColumnDescriptions, d0, d1);
  }

  private Schema invokeRetainDescriptionBuildSchema(
      Function<ConfigSource, ConfigSource> setupConfig,
      String mode,
      Boolean retainColumnDescriptions,
      String d0,
      String d1) {
    return invokeTakeoverBuildSchema(
        configSource ->
            setupConfig.apply(
                configSource
                    .set("mode", mode)
                    .set("retain_column_policy_tags", true)
                    .set("retain_column_descriptions", retainColumnDescriptions)),
        builder -> builder.setDescription(d0).build(),
        builder -> builder.setDescription(d1).build());
  }

  @Test
  public void testRetainDescriptionTrue() {
    Schema schema = invokeRetainDescriptionBuildSchema("replace", true, "prev_c0", "prev_c1");
    assertEquals("d0", schema.getFields().get(0).getDescription());
    assertEquals("prev_c1", schema.getFields().get(1).getDescription());
  }

  @Test
  public void testRetainDescriptionFalse() {
    Schema schema = invokeRetainDescriptionBuildSchema("replace", false, "prev_c0", "prev_c1");
    assertEquals("d0", schema.getFields().get(0).getDescription());
    assertNull(schema.getFields().get(1).getDescription());
  }

  @Test
  public void testRetainDescriptionTrueWithColumnOptionNull() {
    Schema schema =
        invokeRetainDescriptionBuildSchema(
            c -> c.set("column_options", null), "replace", true, "prev_c0", "prev_c1");
    assertEquals("prev_c0", schema.getFields().get(0).getDescription());
    assertEquals("prev_c1", schema.getFields().get(1).getDescription());
  }

  @Test
  public void testRetainDescriptionFalseWithColumnOptionNull() {
    Schema schema =
        invokeRetainDescriptionBuildSchema(
            c -> c.set("column_options", null), "replace", false, "prev_c0", "prev_c1");
    assertNull(schema.getFields().get(0).getDescription());
    assertNull(schema.getFields().get(1).getDescription());
  }

  @Test
  public void testRetainDescriptionTrueButNotModeReplace() {
    // Ruby applies `column_options[].description` (here, c0's "d0" from takeover.yml) on every
    // run regardless of mode. Java only reflects it when isNeedUpdateTable() is true (mode:
    // replace with a retain flag on), so for any other mode buildPatchSchema() returns null and
    // the configured description is never applied, even though it's set here.
    // TODO: apply `column_options[].description` regardless of mode, like ruby does.
    Schema schema = invokeRetainDescriptionBuildSchema("insert", true, "prev_c0", "prev_c1");
    assertNull(schema);
  }

  private Schema invokeRetainPolicyTagsBuildSchema(
      Function<ConfigSource, ConfigSource> setupConfig,
      String mode,
      Boolean retainPolicyTags,
      String[] tags0,
      String[] tags1) {
    List<String> n0 = Arrays.stream(tags0).collect(Collectors.toList());
    List<String> n1 = Arrays.stream(tags1).collect(Collectors.toList());
    PolicyTags p0 = PolicyTags.newBuilder().setNames(n0).build();
    PolicyTags p1 = PolicyTags.newBuilder().setNames(n1).build();
    return invokeTakeoverBuildSchema(
        configSource ->
            setupConfig.apply(
                configSource
                    .set("mode", mode)
                    .set("retain_column_policy_tags", retainPolicyTags)
                    .set("retain_column_descriptions", true)),
        builder -> builder.setPolicyTags(p0).build(),
        builder -> builder.setPolicyTags(p1).build());
  }

  private Schema invokeRetainPolicyTagsBuildSchema(
      String mode, Boolean retainPolicyTags, String[] tags0, String[] tags1) {
    return invokeRetainPolicyTagsBuildSchema(c -> c, mode, retainPolicyTags, tags0, tags1);
  }

  @Test
  public void testRetainColumnPolicyTagsTrue() {
    Schema schema =
        invokeRetainPolicyTagsBuildSchema(
            "replace", true, new String[] {"c0"}, new String[] {"c10", "c11"});
    assertArrayEquals(
        new String[] {"c0"},
        schema.getFields().get(0).getPolicyTags().getNames().toArray(new String[0]));
    assertArrayEquals(
        new String[] {"c10", "c11"},
        schema.getFields().get(1).getPolicyTags().getNames().toArray(new String[0]));
  }

  @Test
  public void testRetainColumnPolicyTagsFalse() {
    Schema schema =
        invokeRetainPolicyTagsBuildSchema(
            "replace", false, new String[] {"c0"}, new String[] {"c10", "c11"});
    assertNull(schema.getFields().get(0).getPolicyTags());
    assertNull(schema.getFields().get(1).getPolicyTags());
  }

  @Test
  public void testRetainColumnPolicyTagsTrueButNotModeReplace() {
    Schema schema =
        invokeRetainPolicyTagsBuildSchema(
            "insert", true, new String[] {"c0"}, new String[] {"c10", "c11"});
    assertNull(schema);
  }

  @Test
  public void testStoreCachedSrcFieldsIfNeed() throws NoSuchFieldException, IllegalAccessException {
    ConfigSource config = loadYamlResource(embulk, "takeover.yml");

    // Test case 1: Mode is "replace" with retainColumnDescriptions = true
    PluginTask replaceTask =
        CONFIG_MAPPER.map(
            config
                .set("mode", "replace")
                .set("retain_column_descriptions", true)
                .set("retain_column_policy_tags", false),
            PluginTask.class);

    BigqueryClient client = Mockito.mock(BigqueryClient.class);
    Table mockTable = Mockito.mock(Table.class);
    TableDefinition mockTableDef = Mockito.mock(TableDefinition.class);

    Schema mockSchema = Schema.of(Field.newBuilder("field1", StandardSQLTypeName.STRING).build());

    Mockito.when(client.getTable(Mockito.anyString())).thenReturn(mockTable);
    Mockito.when(mockTable.getDefinition()).thenReturn(mockTableDef);
    Mockito.when(mockTableDef.getSchema()).thenReturn(mockSchema);
    Mockito.when(client.storeCachedSrcFieldsIfNeed()).thenCallRealMethod();

    // Set task field
    java.lang.reflect.Field taskField = BigqueryClient.class.getDeclaredField("task");
    taskField.setAccessible(true);
    taskField.set(client, replaceTask);

    assertEquals(client.storeCachedSrcFieldsIfNeed(), mockSchema.getFields());

    // Test case 2: Mode is "insert" - should return null
    PluginTask insertTask =
        CONFIG_MAPPER.map(
            config
                .set("mode", "insert")
                .set("retain_column_descriptions", true)
                .set("retain_column_policy_tags", true),
            PluginTask.class);

    taskField.set(client, insertTask);

    assertNull(client.storeCachedSrcFieldsIfNeed());

    // Test case 3: Table doesn't exist - should return null
    Mockito.when(client.getTable(Mockito.anyString())).thenReturn(null);

    taskField.set(client, replaceTask);

    assertNull(client.storeCachedSrcFieldsIfNeed());

    // Test case 4: Schema is null - should return null
    Table nullSchemaTable = Mockito.mock(Table.class);
    TableDefinition nullSchemaTableDef = Mockito.mock(TableDefinition.class);

    Mockito.when(client.getTable(Mockito.anyString())).thenReturn(nullSchemaTable);
    Mockito.when(nullSchemaTable.getDefinition()).thenReturn(nullSchemaTableDef);
    Mockito.when(nullSchemaTableDef.getSchema()).thenReturn(null);

    taskField.set(client, replaceTask);

    assertNull(client.storeCachedSrcFieldsIfNeed());
  }

  // The upload path can't be exercised through MockWebServer: TableDataWriteChannel's resumable
  // upload hardcodes the real BigQuery host (see BigqueryTestHostSupport). Instead, BigqueryClient
  // is a Mockito mock whose load() runs for real, with its collaborators injected by reflection
  // and its network steps (writeToStream / waitForLoad) left to the mock.
  private static BigqueryClient newLoadClient(PluginTask task, BigQuery bigquery, Logger logger)
      throws ReflectiveOperationException {
    BigqueryClient client = Mockito.mock(BigqueryClient.class);
    Mockito.when(
            client.load(
                Mockito.any(Path.class),
                Mockito.anyString(),
                Mockito.any(JobInfo.WriteDisposition.class)))
        .thenCallRealMethod();
    setClientField(client, "task", task);
    setClientField(client, "bigquery", bigquery);
    setClientField(client, "schema", new org.embulk.spi.Schema(Collections.emptyList()));
    setClientField(client, "columnOptions", Collections.emptyList());
    setClientField(client, "destinationProject", "project");
    setClientField(client, "destinationDataset", "dataset");
    setClientField(client, "logger", logger);
    return client;
  }

  private static void setClientField(BigqueryClient client, String name, Object value)
      throws ReflectiveOperationException {
    java.lang.reflect.Field field = BigqueryClient.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(client, value);
  }

  // BigQuery and the write channel are mocks: tests here only look at what load() does around
  // them (attempt count, job ids, logging).
  private static class LoadFixture {
    final BigQuery bigquery = Mockito.mock(BigQuery.class);
    final TableDataWriteChannel writer = Mockito.mock(TableDataWriteChannel.class);
    final Logger logger = Mockito.mock(Logger.class);
    final BigqueryClient client;

    LoadFixture(PluginTask task) throws ReflectiveOperationException {
      Mockito.when(
              bigquery.writer(
                  Mockito.any(JobId.class), Mockito.any(WriteChannelConfiguration.class)))
          .thenReturn(writer);
      client = newLoadClient(task, bigquery, logger);
    }

    List<String> capturedJobIds() {
      ArgumentCaptor<JobId> captor = ArgumentCaptor.forClass(JobId.class);
      Mockito.verify(bigquery, Mockito.atLeastOnce())
          .writer(captor.capture(), Mockito.any(WriteChannelConfiguration.class));
      return captor.getAllValues().stream().map(JobId::getJob).collect(Collectors.toList());
    }

    void verifyUploadAttempts(int times) throws IOException {
      Mockito.verify(bigquery, Mockito.times(times))
          .writer(Mockito.any(JobId.class), Mockito.any(WriteChannelConfiguration.class));
      Mockito.verify(client, Mockito.times(times))
          .writeToStream(Mockito.any(Path.class), Mockito.any(OutputStream.class));
    }

    void verifyUploadErrorsLogged(int times) {
      Mockito.verify(logger, Mockito.times(times)).error(Mockito.contains("failed to upload"));
    }
  }

  private PluginTask loadTask(int retries) {
    ConfigSource config = loadYamlResource(embulk, "takeover.yml").set("retries", retries);
    return CONFIG_MAPPER.map(config, PluginTask.class);
  }

  private LoadFixture loadFixture(int retries) throws ReflectiveOperationException {
    return new LoadFixture(loadTask(retries));
  }

  // Here BigQuery and TableDataWriteChannel are the real library classes on top of a mocked
  // BigQueryRpc, so the channel's own behavior is exercised: how it reports a failed upload and
  // what closing it does. A mocked channel can't reproduce either, because write() and close()
  // are final in BaseWriteChannel. Each upload session gets its own id ("upload-1", "upload-2",
  // ...)
  // so tests can tell the attempts apart in rpc.write() calls.
  private static class RpcLoadFixture {
    final BigQueryRpc rpc = Mockito.mock(BigQueryRpc.class);
    final Logger logger = Mockito.mock(Logger.class);
    final BigqueryClient client;

    @SuppressWarnings("unchecked")
    RpcLoadFixture(PluginTask task) throws ReflectiveOperationException {
      ServiceRpcFactory<BigQueryOptions> rpcFactory = Mockito.mock(ServiceRpcFactory.class);
      Mockito.when(rpcFactory.create(Mockito.any(BigQueryOptions.class))).thenReturn(rpc);
      BigQuery bigquery =
          BigQueryOptions.newBuilder()
              .setProjectId("project")
              .setCredentials(NoCredentials.getInstance())
              .setServiceRpcFactory(rpcFactory)
              // Leave retrying to BigqueryClient#load() so the library doesn't retry underneath.
              .setRetrySettings(ServiceOptions.getNoRetrySettings())
              .build()
              .getService();
      Mockito.when(rpc.open(Mockito.any(com.google.api.services.bigquery.model.Job.class)))
          .thenReturn("upload-1", "upload-2", "upload-3");
      stubWrite().thenReturn(new com.google.api.services.bigquery.model.Job());
      client = newLoadClient(task, bigquery, logger);
    }

    OngoingStubbing<com.google.api.services.bigquery.model.Job> stubWrite() {
      return Mockito.when(
          rpc.write(
              Mockito.anyString(),
              Mockito.any(byte[].class),
              Mockito.anyInt(),
              Mockito.anyLong(),
              Mockito.anyInt(),
              Mockito.anyBoolean()));
    }

    // The last chunk (lastChunk = true) finalizes the resumable session, which is what makes
    // BigQuery start the load job for the uploaded bytes.
    void verifyFinalized(String uploadId, VerificationMode mode) {
      Mockito.verify(rpc, mode)
          .write(
              Mockito.eq(uploadId),
              Mockito.any(byte[].class),
              Mockito.anyInt(),
              Mockito.anyLong(),
              Mockito.anyInt(),
              Mockito.eq(true));
    }
  }

  private RpcLoadFixture rpcLoadFixture(int retries) throws ReflectiveOperationException {
    return new RpcLoadFixture(loadTask(retries));
  }

  @Test
  public void testLoadRetriesUploadIOExceptionThenGivesUp()
      throws IOException, ReflectiveOperationException {
    LoadFixture f = loadFixture(1);
    Mockito.doCallRealMethod()
        .when(f.client)
        .writeToStream(Mockito.any(Path.class), Mockito.any(OutputStream.class));
    Path notAFile = testFolder.newFolder().toPath();

    BigqueryUploadException thrown =
        assertThrows(
            BigqueryUploadException.class,
            () -> f.client.load(notAFile, "table", JobInfo.WriteDisposition.WRITE_APPEND));

    assertTrue(thrown.getMessage().contains("failed to upload"));
    assertTrue(thrown.getCause() instanceof IOException);
    f.verifyUploadAttempts(2);
    f.verifyUploadErrorsLogged(2);
    Mockito.verify(f.logger).error("embulk-output-bigquery: Give up retrying for Load job");
    Mockito.verify(f.writer, Mockito.never()).getJob();
  }

  @Test
  public void testLoadRetriesUploadIOExceptionThenSucceeds()
      throws IOException, ReflectiveOperationException {
    LoadFixture f = loadFixture(1);
    Mockito.doThrow(new IOException("Connection reset"))
        .doNothing()
        .when(f.client)
        .writeToStream(Mockito.any(Path.class), Mockito.any(OutputStream.class));
    Path loadFile = testFolder.newFile().toPath();

    f.client.load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND);

    f.verifyUploadAttempts(2);
    f.verifyUploadErrorsLogged(1);
    Mockito.verify(f.logger, Mockito.never())
        .error("embulk-output-bigquery: Give up retrying for Load job");
    Mockito.verify(f.writer).getJob();
  }

  // TableDataWriteChannel reports a failed upload as BigQueryException (a RuntimeException), not
  // IOException: BigQueryRpc#write translates the IOException, and flushBuffer() rethrows it via
  // BigQueryException.translateAndThrow(). So a connection reset during the upload has to be
  // retried through that type.
  @Test
  public void testLoadRetriesUploadBigQueryExceptionThenSucceeds()
      throws IOException, ReflectiveOperationException {
    RpcLoadFixture f = rpcLoadFixture(1);
    f.stubWrite()
        .thenThrow(new BigQueryException(new SocketException("Connection reset")))
        .thenReturn(new com.google.api.services.bigquery.model.Job());
    Path loadFile = testFolder.newFile().toPath();

    f.client.load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND);

    Mockito.verify(f.rpc, Mockito.times(2))
        .open(Mockito.any(com.google.api.services.bigquery.model.Job.class));
    Mockito.verify(f.logger, Mockito.never())
        .error("embulk-output-bigquery: Give up retrying for Load job");
  }

  // A BigQueryException the library itself wouldn't retry (4xx) fails the load right away, with
  // the plugin's context message but without being retried.
  @Test
  public void testLoadDoesNotRetryNonRetryableBigQueryException()
      throws IOException, ReflectiveOperationException {
    RpcLoadFixture f = rpcLoadFixture(1);
    f.stubWrite().thenThrow(new BigQueryException(400, "Invalid schema"));
    Path loadFile = testFolder.newFile().toPath();

    BigqueryException thrown =
        assertThrows(
            BigqueryException.class,
            () -> f.client.load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND));

    assertFalse(thrown instanceof BigqueryUploadException);
    assertTrue(thrown.getMessage().contains("failed to upload"));
    assertTrue(thrown.getMessage().contains("Invalid schema"));
    Mockito.verify(f.rpc, Mockito.times(1))
        .open(Mockito.any(com.google.api.services.bigquery.model.Job.class));
  }

  // Once the upload has failed, the resumable session must not be finalized: closing the channel
  // sends the bytes written so far as the last chunk, and BigQuery starts a load job for that
  // partial file in addition to the one issued by the retry.
  @Test
  public void testLoadDoesNotFinalizeUploadAfterFailure()
      throws IOException, ReflectiveOperationException {
    RpcLoadFixture f = rpcLoadFixture(1);
    Mockito.doAnswer(
            invocation -> {
              OutputStream stream = (OutputStream) invocation.getArguments()[1];
              stream.write("{}\n".getBytes(StandardCharsets.UTF_8));
              throw new IOException("read error in the middle of the file");
            })
        .doNothing()
        .when(f.client)
        .writeToStream(Mockito.any(Path.class), Mockito.any(OutputStream.class));
    Path loadFile = testFolder.newFile().toPath();

    f.client.load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND);

    f.verifyFinalized("upload-1", Mockito.never());
    f.verifyFinalized("upload-2", Mockito.times(1));
  }

  // ruby generates the job id once per load() and reuses it across network retries
  // (with_network_retry), so a retried upload can't turn into a second load job.
  @Test
  public void testLoadReusesJobIdAcrossUploadRetries()
      throws IOException, ReflectiveOperationException {
    LoadFixture f = loadFixture(1);
    Mockito.doThrow(new IOException("Connection reset"))
        .doNothing()
        .when(f.client)
        .writeToStream(Mockito.any(Path.class), Mockito.any(OutputStream.class));
    Path loadFile = testFolder.newFile().toPath();

    f.client.load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND);

    List<String> jobIds = f.capturedJobIds();
    assertEquals(2, jobIds.size());
    assertEquals(jobIds.get(0), jobIds.get(1));
  }

  // Guard for the other half of the ruby behavior: with_job_retry wraps the job id generation, so
  // a job that ran and failed (BackendError etc.) is retried as a new job. A finished job id can't
  // be reused anyway. This passes today and must keep passing once upload retries reuse the id.
  @Test
  public void testLoadUsesNewJobIdWhenJobFails() throws IOException, ReflectiveOperationException {
    LoadFixture f = loadFixture(1);
    Mockito.doThrow(new BigqueryBackendException("backendError"))
        .doReturn(null)
        .when(f.client)
        .waitForLoad(Mockito.any(Job.class));
    Path loadFile = testFolder.newFile().toPath();

    f.client.load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND);

    List<String> jobIds = f.capturedJobIds();
    assertEquals(2, jobIds.size());
    assertNotEquals(jobIds.get(0), jobIds.get(1));
  }

  // The stack trace is already logged by onRetry() and by Embulk when the exception propagates;
  // the per-attempt ERROR inside the retry loop should carry the message only.
  @Test
  public void testLoadDoesNotLogUploadStackTraceInsideRetryLoop()
      throws IOException, ReflectiveOperationException {
    LoadFixture f = loadFixture(1);
    Mockito.doCallRealMethod()
        .when(f.client)
        .writeToStream(Mockito.any(Path.class), Mockito.any(OutputStream.class));
    Path notAFile = testFolder.newFolder().toPath();

    assertThrows(
        BigqueryUploadException.class,
        () -> f.client.load(notAFile, "table", JobInfo.WriteDisposition.WRITE_APPEND));

    Mockito.verify(f.logger, Mockito.never())
        .error(Mockito.contains("failed to upload"), Mockito.any(Throwable.class));
  }
}
