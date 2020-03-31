package org.embulk.output.bigquery_java.config;

import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.output.bigquery_java.BigqueryJavaOutputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.test.TestingEmbulk;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestBigqueryConfigValidator {
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
    public void validateMode() {
        config = loadYamlResource(embulk, "base.yml");
        PluginTask task = config.loadConfig(PluginTask.class);
        BigqueryConfigValidator.validateMode(task);

        assertEquals("replace", task.getMode());
    }

    @Test(expected = ConfigException.class)
    public void validateMode_invalid_configException() {
        config = loadYamlResource(embulk, "base.yml");
        PluginTask task = config.loadConfig(PluginTask.class);
        task.setMode("foo");
        BigqueryConfigValidator.validateMode(task);
    }

    @Test
    public void validateModeAndAutoCreteTable(){
        config = loadYamlResource(embulk, "base.yml");
        PluginTask task = config.loadConfig(PluginTask.class);
        BigqueryConfigValidator.validateModeAndAutoCreteTable(task);

        assertEquals("replace", task.getMode());
        assertTrue(task.getAutoCreateTable());
    }

    @Test(expected = ConfigException.class)
    public void validateModeAndAutoCreteTable_autoCreateTable_False_configException(){
        config = loadYamlResource(embulk, "base.yml");
        PluginTask task = config.loadConfig(PluginTask.class);
        task.setAutoCreateTable(false);
        BigqueryConfigValidator.validateModeAndAutoCreteTable(task);
    }
}
