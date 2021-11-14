package org.embulk.output.bigquery_java.config;

import com.google.common.io.Resources;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.output.bigquery_java.BigqueryJavaOutputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.test.TestingEmbulk;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

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
        config = embulk.configLoader().fromYamlString(
                String.join("\n",
                        "type: bigquery_java",
                        "mode: replace",
                        "auth_method: service_account",
                        "json_keyfile: json_key.json",
                        "dataset: dataset",
                        "table: table",
                        "source_format: NEWLINE_DELIMITED_JSON",
                        "compression: GZIP",
                        "auto_create_dataset: false",
                        "auto_create_table: true",
                        "path_prefix: /tmp/bq_compress/bq_",
                        ""
                )
        );
        PluginTask task = config.loadConfig(PluginTask.class);
        BigqueryTaskBuilder.setAbortOnError(task);

        assertEquals(0, task.getMaxBadRecords());
        assertEquals(true, task.getAbortOnError().get());
    }

    // TODO jsonl without compression, csv with/out compression
    @Test
    public void setFileExt_JSONL_GZIP_JSONL_GZ() {
        config = embulk.configLoader().fromYamlString(
                String.join("\n",
                        "type: bigquery_java",
                        "mode: replace",
                        "auth_method: service_account",
                        "json_keyfile: json_key.json",
                        "dataset: dataset",
                        "table: table",
                        "source_format: NEWLINE_DELIMITED_JSON",
                        "compression: GZIP",
                        "auto_create_dataset: false",
                        "auto_create_table: true",
                        "path_prefix: /tmp/bq_compress/bq_",
                        ""
                        )
        );
        PluginTask task = config.loadConfig(PluginTask.class);

        BigqueryTaskBuilder.setFileExt(task);
        assertEquals("NEWLINE_DELIMITED_JSON", task.getSourceFormat());
        assertEquals("GZIP", task.getCompression());
        assertEquals(".jsonl.gz", task.getFileExt().get());
    }

    @Test
    public void setProject() {
        config = embulk.configLoader().fromYamlString(
                String.join("\n",
                        "type: bigquery_java",
                        "mode: replace",
                        "auth_method: service_account",
                        "json_keyfile: json_key.json",
                        "dataset: dataset",
                        "table: table",
                        "source_format: NEWLINE_DELIMITED_JSON",
                        "compression: GZIP",
                        "auto_create_dataset: false",
                        "auto_create_table: true",
                        "path_prefix: /tmp/bq_compress/bq_",
                        ""
                )
        );
        config.set("json_keyfile", Resources.getResource(BASIC_RESOURCE_PATH+"json_key.json").getPath());
        PluginTask task = config.loadConfig(PluginTask.class);
        BigqueryTaskBuilder.setProject(task);

        assertEquals("project_id", task.getProject().get());
    }

    @Test(expected = ConfigException.class)
    public void setProject_config_exception() {
        config = embulk.configLoader().fromYamlString(
                String.join("\n",
                        "type: bigquery_java",
                        "mode: replace",
                        "auth_method: service_account",
                        "json_keyfile: json_key.json",
                        "dataset: dataset",
                        "table: table",
                        "source_format: NEWLINE_DELIMITED_JSON",
                        "compression: GZIP",
                        "auto_create_dataset: false",
                        "auto_create_table: true",
                        "path_prefix: /tmp/bq_compress/bq_",
                        ""
                )
        );
        PluginTask task = config.loadConfig(PluginTask.class);
        BigqueryTaskBuilder.setProject(task);
    }


    @Test
    public void clustering() {
        config = embulk.configLoader().fromYamlString(
                String.join("\n",
                        "type: bigquery_java",
                        "mode: replace",
                        "auth_method: service_account",
                        "json_keyfile: json_key.json",
                        "dataset: dataset",
                        "table: table",
                        "source_format: NEWLINE_DELIMITED_JSON",
                        "compression: GZIP",
                        "auto_create_dataset: false",
                        "auto_create_table: true",
                        "path_prefix: /tmp/bq_compress/bq_",
                        "clustering:",
                        "  fields:",
                        "    - foo",
                        "    - bar",
                        "    - baz"
                        )
        );
        PluginTask task = config.loadConfig(PluginTask.class);
        BigqueryTaskBuilder.setAbortOnError(task);
        System.out.println(task.getClustering().get().getFields());
        List<String> expectedOutput = Arrays.asList("foo", "bar", "baz");

        assertEquals(expectedOutput, task.getClustering().get().getFields().get());
    }
}
