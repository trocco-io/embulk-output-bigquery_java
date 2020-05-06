package org.embulk.output.bigquery_java.converter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import org.embulk.config.ConfigSource;
import org.embulk.output.bigquery_java.BigqueryJavaOutputPlugin;
import org.embulk.output.bigquery_java.BigqueryUtil;
import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
import org.embulk.output.bigquery_java.config.BigqueryColumnOptionType;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.spi.OutputPlugin;
import org.embulk.test.TestingEmbulk;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestBigqueryLongConverter {
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
    public void testConvertLongToString() {
        ObjectNode node = BigqueryUtil.getObjectMapper().createObjectNode();
        config = loadYamlResource(embulk, "base.yml");
        ImmutableList.Builder<ConfigSource> builder = ImmutableList.builder();
        ConfigSource configSource = embulk.newConfig();
        configSource.set("type", "STRING");
        configSource.set("name", "key");
        builder.add(configSource);
        config.set("column_options",builder.build());
        BigqueryColumnOption columnOption = configSource.loadConfig(BigqueryColumnOption.class);
        PluginTask task = config.loadConfig(PluginTask.class);

        BigqueryLongConverter.convertAndSet(node, "key", 1L, BigqueryColumnOptionType.STRING);

        assertEquals(node.get("key").asText(),"1");
    }

    @Test
    public void testConvertLongToInteger() {
        ObjectNode node = BigqueryUtil.getObjectMapper().createObjectNode();
        config = loadYamlResource(embulk, "base.yml");
        ImmutableList.Builder<ConfigSource> builder = ImmutableList.builder();
        ConfigSource configSource = embulk.newConfig();
        configSource.set("type", "INTEGER");
        configSource.set("name", "key");
        builder.add(configSource);
        config.set("column_options",builder.build());
        BigqueryColumnOption columnOption = configSource.loadConfig(BigqueryColumnOption.class);
        PluginTask task = config.loadConfig(PluginTask.class);

        BigqueryLongConverter.convertAndSet(node, "key", 1L, BigqueryColumnOptionType.INTEGER);

        assertEquals(node.get("key").asLong(),1L);
    }

    @Test
    public void testConvertLongToFloat() {
        ObjectNode node = BigqueryUtil.getObjectMapper().createObjectNode();
        config = loadYamlResource(embulk, "base.yml");
        ImmutableList.Builder<ConfigSource> builder = ImmutableList.builder();
        ConfigSource configSource = embulk.newConfig();
        configSource.set("type", "FLOAT");
        configSource.set("name", "key");
        builder.add(configSource);
        config.set("column_options",builder.build());
        BigqueryColumnOption columnOption = configSource.loadConfig(BigqueryColumnOption.class);
        PluginTask task = config.loadConfig(PluginTask.class);

        BigqueryLongConverter.convertAndSet(node, "key", 1L, BigqueryColumnOptionType.FLOAT);

        assertEquals(node.get("key").asDouble(),1.0, 0);
    }

    @Test
    public void testConvertLongToTimestamp() {
        ObjectNode node = BigqueryUtil.getObjectMapper().createObjectNode();
        config = loadYamlResource(embulk, "base.yml");
        ImmutableList.Builder<ConfigSource> builder = ImmutableList.builder();
        ConfigSource configSource = embulk.newConfig();
        configSource.set("type", "TIMESTAMP");
        configSource.set("name", "key");
        builder.add(configSource);
        config.set("column_options",builder.build());
        BigqueryColumnOption columnOption = configSource.loadConfig(BigqueryColumnOption.class);
        PluginTask task = config.loadConfig(PluginTask.class);

        BigqueryLongConverter.convertAndSet(node, "key", 1L, BigqueryColumnOptionType.TIMESTAMP);

        assertEquals(node.get("key").asLong(),1L);
    }

    @Test
    public void testConvertLongToBoolean() {
        ObjectNode node = BigqueryUtil.getObjectMapper().createObjectNode();
        config = loadYamlResource(embulk, "base.yml");
        ImmutableList.Builder<ConfigSource> builder = ImmutableList.builder();
        ConfigSource configSource = embulk.newConfig();
        configSource.set("type", "BOOLEAN");
        configSource.set("name", "key");
        builder.add(configSource);
        config.set("column_options",builder.build());
        BigqueryColumnOption columnOption = configSource.loadConfig(BigqueryColumnOption.class);
        PluginTask task = config.loadConfig(PluginTask.class);

        BigqueryLongConverter.convertAndSet(node, "key", 1L, BigqueryColumnOptionType.BOOLEAN);
        assertTrue(node.get("key").asBoolean());

        BigqueryLongConverter.convertAndSet(node, "key", 0L, BigqueryColumnOptionType.BOOLEAN);
        assertFalse(node.get("key").asBoolean());
    }
}
