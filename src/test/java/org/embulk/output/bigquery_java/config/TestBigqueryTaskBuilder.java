package org.embulk.output.bigquery_java.config;

import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.output.bigquery_java.BigqueryJavaOutputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.test.TestingEmbulk;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestBigqueryTaskBuilder {

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
    public void setAbortOnError_DefaultMaxBadRecord_True() {
        config = loadYamlResource(embulk, "base.yml");
        PluginTask task = config.loadConfig(PluginTask.class);
        BigqueryTaskBuilder.setAbortOnError(task);

        assertEquals(0, task.getMaxBadRecords());
        assertEquals(true, task.getAbortOnError().get());
    }

    // TODO jsonl without compression, csv with/out compression
    @Test
    public void setFileExt_JSONL_GZIP_JSONL_GZ() {
        config = loadYamlResource(embulk, "base.yml");
        PluginTask task = config.loadConfig(PluginTask.class);

        BigqueryTaskBuilder.setFileExt(task);
        assertEquals("NEWLINE_DELIMITED_JSON", task.getSourceFormat());
        assertEquals("GZIP", task.getCompression());
        assertEquals(".jsonl.gz", task.getFileExt().get());
    }

    @Test
    public void setProject() {
        config = loadYamlResource(embulk, "base.yml");
        config.set("json_keyfile", BASIC_RESOURCE_PATH + "json_key.json");
        PluginTask task = config.loadConfig(PluginTask.class);
        BigqueryTaskBuilder.setProject(task);

        assertEquals("project_id", task.getProject().get());
    }

    @Test(expected = ConfigException.class)
    public void setProject_config_exception() {
        config = loadYamlResource(embulk, "base.yml");
        PluginTask task = config.loadConfig(PluginTask.class);
        BigqueryTaskBuilder.setProject(task);
    }
}
