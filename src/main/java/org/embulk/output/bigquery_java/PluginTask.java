package org.embulk.output.bigquery_java;

;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

import java.util.List;
import java.util.Optional;

public interface PluginTask
        extends Task {

    // TODO: default should be append
    @Config("mode")
    @ConfigDefault("\"replace\"")
    public String getMode();

    // TODO: default should be application default
    @Config("auth_method")
    @ConfigDefault("\"service_account\"")
    public String getAuthMethod();

    @Config("json_keyfile")
    public String getJsonKeyfile();

    @Config("dataset")
    public String getDataset();

    @Config("table")
    public String getTable();

    @Config("location")
    @ConfigDefault("null")
    public String getLocation();

    @Config("column_options")
    @ConfigDefault("null")
    public Optional<List<BigqueryColumnOption>> getColumnOptions();

    @Config("compression")
    @ConfigDefault("NONE")
    public String getCompression();

    @Config("source_format")
    public String getSourceFormat();

    @Config("path_prefix")
    @ConfigDefault("null")
    public String getPathPrefix();

    public void setPathPrefix(String pathPrefix);

    @Config("default_timezone")
    @ConfigDefault("\"UTC\"")
    public String getDefaultTimezone();

    @Config("default_timestamp_format")
    @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%6N\"")
    public String getDefaultTimestampFormat();

    //TODO: make this optional
    @Config("file_ext")
    @ConfigDefault("null")
    public String getFileExt();

    public void setFileExt(String fileExt);

    @Config("encoding")
    @ConfigDefault("\"UTF-8\"")
    public String getEncoding();

    @Config("auto_create_dataset")
    @ConfigDefault("false")
    public boolean getAutoCreateDataset();

    @Config("auto_create_table")
    @ConfigDefault("true")
    public boolean getAutoCreateTable();

    @Config("max_bad_records")
    @ConfigDefault("0")
    public int getMaxBadRecords();

    @Config("ignore_unknown_values")
    @ConfigDefault("false")
    public boolean getIgnoreUnknownValues();

    @Config("allow_quoted_newlines")
    @ConfigDefault("false")
    public boolean getAllowQuotedNewlines();

    @Config("template_table")
    @ConfigDefault("null")
    public String getTemplateTable();

    @Config("job_status_polling_interval")
    @ConfigDefault("10")
    public long getJobStatusPollingInterval();

    @Config("job_status_max_polling_time")
    @ConfigDefault("3600")
    public long getJobStatusMaxPollingTime();

}
