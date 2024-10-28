package org.embulk.output.bigquery_java;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.TableResult;
import org.checkerframework.checker.units.qual.A;
import org.embulk.config.ConfigSource;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.output.bigquery_java.config.BigqueryTimePartitioning;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestBigqueryJavaOutputPlugin {
    protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
            ConfigMapperFactory.builder().addDefaultModules().build();

    protected static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

    private ConfigSource config;
    private static final String BASIC_RESOURCE_PATH = "/java/org/embulk/output/bigquery_java/";

    private static ConfigSource loadYamlResource(TestingEmbulk embulk, String fileName) {
        return embulk.loadYamlResource(BASIC_RESOURCE_PATH + fileName);
    }

    @Rule
    public TestingEmbulk embulk = TestingEmbulk.builder()
            .registerPlugin(OutputPlugin.class, "bigquery_java", BigqueryJavaOutputPlugin.class)
            .registerPlugin(FileInputPlugin.class, "file", LocalFileInputPlugin.class)
            .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
            .build();

    @Rule public TemporaryFolder testFolder = new TemporaryFolder();

    @Test
    public void testDefaultConfigValues() {
        config = loadYamlResource(embulk, "base.yml");
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        assertEquals("replace", task.getMode());
        assertEquals(5, task.getRetries());
        assertEquals(0, task.getMaxBadRecords());
        assertEquals("dataset", task.getDataset());
        assertEquals("table", task.getTable());
        assertEquals("service_account", task.getAuthMethod());
        assertEquals("UTC", task.getDefaultTimezone());
        assertEquals("UTF-8", task.getEncoding());
        assertTrue(task.getDeleteFromLocalWhenJobEnd());
        assertFalse(task.getAutoCreateDataset());
        assertTrue(task.getAutoCreateTable());
    }

    @Test
    public void testWithTimePartitioning() {
        config = loadYamlResource(embulk, "time_partitioning.yml");
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        BigqueryTimePartitioning bigqueryTimePartitioning;

        assertTrue(task.getTimePartitioning().isPresent());
        bigqueryTimePartitioning = task.getTimePartitioning().get();
        assertEquals("DAY", bigqueryTimePartitioning.getType());
        assertTrue(bigqueryTimePartitioning.getField().isPresent());
        assertEquals("date", bigqueryTimePartitioning.getField().get());
    }

    public interface TestTask extends Task {
        @Config("json_keyfile")
        String getJsonKeyfile();

        @Config("dataset")
        String getDataset();

        @Config("table")
        String getTable();
    }

    @Test
    public void testRun() throws IOException {
        ConfigSource testConfig = EmbulkTests.config("EMBULK_OUTPUT_BIGQUERY_TEST_CONFIG");
        TestTask testTask = CONFIG_MAPPER.map(testConfig, TestTask.class);
        ConfigSource outConfig = CONFIG_MAPPER_FACTORY.newConfigSource();
        outConfig.set("type", "bigquery_java");
        outConfig.set("mode", "replace");
        outConfig.set("json_keyfile", testTask.getJsonKeyfile());
        outConfig.set("dataset", testTask.getDataset());
        outConfig.set("table", testTask.getTable());
        outConfig.set("source_format", "NEWLINE_DELIMITED_JSON");

        File in = testFolder.newFile("embulk-output-bigquery_java-test.csv");
        Files.write(in.toPath(), Arrays.asList("c0:string,c1:boolean,index:double", "test0,true,0", "test1,false,1"));
        TestingEmbulk.RunResult runResult = embulk.runOutput(outConfig, in.toPath());

        BigqueryClient bigqueryClient = new BigqueryClient(CONFIG_MAPPER.map(outConfig, PluginTask.class), runResult.getOutputSchema());
        TableResult tableResult = bigqueryClient.runQuery(String.format("SELECT * from `%s`.`%s`", testTask.getDataset(), testTask.getTable()));
        List<FieldValueList> results = new ArrayList<>();
        tableResult.iterateAll().forEach(results::add);
        assertEquals(2, results.size());
        assertEquals("test0", results.get(0).get("c0").getStringValue());
        assertTrue(results.get(0).get("c1").getBooleanValue());
        assertEquals("test1", results.get(1).get("c0").getStringValue());
        assertFalse(results.get(1).get("c1").getBooleanValue());
    }
}
