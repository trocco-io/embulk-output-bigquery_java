package org.embulk.output.bigquery_java;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.output.bigquery_java.config.BigqueryTimePartitioning;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.spi.OutputPlugin;
import org.embulk.test.TestingEmbulk;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestBigqueryJavaOutputPlugin {
    private ConfigSource config;
    private static final String BASIC_RESOURCE_PATH = "java/org/embulk/output/bigquery_java/";

    private static ConfigSource loadYamlResource(TestingEmbulk embulk, String fileName) {
        return embulk.loadYamlResource(BASIC_RESOURCE_PATH + fileName);
    }

    @Rule
    public TestingEmbulk embulk = TestingEmbulk.builder()
            .registerPlugin(OutputPlugin.class, "bigquery_java", BigqueryJavaOutputPlugin.class)
            .build();

    @Test
    public void testDefaultConfigValues() {
        config = loadYamlResource(embulk, "base.yml");
        PluginTask task = config.loadConfig(PluginTask.class);

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
        PluginTask task = config.loadConfig(PluginTask.class);
        BigqueryTimePartitioning bigqueryTimePartitioning;

        assertTrue(task.getTimePartitioning().isPresent());
        bigqueryTimePartitioning = task.getTimePartitioning().get();
        assertEquals("DAY", bigqueryTimePartitioning.getType());
        assertTrue(bigqueryTimePartitioning.getField().isPresent());
        assertEquals("date", bigqueryTimePartitioning.getField().get());
    }
}
