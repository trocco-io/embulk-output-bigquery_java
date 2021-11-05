package org.embulk.output.bigquery_java.config;

import org.embulk.config.Config;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.output.bigquery_java.BigqueryJavaOutputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.test.TestingEmbulk;
import org.junit.Rule;
import org.junit.Test;

import java.util.Optional;

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
    public void validateModeAndAutoCreteTable() {
        config = loadYamlResource(embulk, "base.yml");
        PluginTask task = config.loadConfig(PluginTask.class);
        BigqueryConfigValidator.validateModeAndAutoCreteTable(task);

        assertEquals("replace", task.getMode());
        assertTrue(task.getAutoCreateTable());
    }

    @Test(expected = ConfigException.class)
    public void validateModeAndAutoCreteTable_autoCreateTable_False_configException() {
        config = loadYamlResource(embulk, "base.yml");
        PluginTask task = config.loadConfig(PluginTask.class);
        task.setAutoCreateTable(false);
        BigqueryConfigValidator.validateModeAndAutoCreteTable(task);
    }

    @Test
    public void validateProject() {
        config = loadYamlResource(embulk, "base.yml");
        PluginTask task = config.loadConfig(PluginTask.class);
        task.setProject(Optional.of("project_id"));
        BigqueryConfigValidator.validateProject(task);

        assertEquals("project_id", task.getProject().get());
    }

    @Test(expected = ConfigException.class)
    public void validateProject_project_null_configException() {
        config = loadYamlResource(embulk, "base.yml");
        PluginTask task = config.loadConfig(PluginTask.class);
        BigqueryConfigValidator.validateProject(task);
    }

    @Test(expected = ConfigException.class)
    public void validateProject_project_empty_string_configException() {
        config = loadYamlResource(embulk, "base.yml");
        PluginTask task = config.loadConfig(PluginTask.class);
        task.setProject(Optional.empty());
        BigqueryConfigValidator.validateProject(task);
    }

}
