package org.embulk.output.bigquery_java.config;

import java.util.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

import java.util.List;

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
    public Optional<String> getLocation();

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
    public Optional<String> getPathPrefix();

    public void setPathPrefix(Optional<String> pathPrefix);

    @Config("default_timezone")
    @ConfigDefault("\"UTC\"")
    public String getDefaultTimezone();

    @Config("default_timestamp_format")
    @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%6N\"")
    public String getDefaultTimestampFormat();

    //TODO: make this optional
    @Config("file_ext")
    @ConfigDefault("null")
    public Optional<String> getFileExt();

    public void setFileExt(Optional<String> fileExt);

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
    public Optional<String> getTemplateTable();

    @Config("job_status_polling_interval")
    @ConfigDefault("10")
    public long getJobStatusPollingInterval();

    @Config("job_status_max_polling_time")
    @ConfigDefault("3600")
    public long getJobStatusMaxPollingTime();

    @Config("temp_table")
    @ConfigDefault("null")
    public Optional<String> getTempTable();

    public void setTempTable(Optional<String> tempTable);

    @Config("delete_from_local_when_job_end")
    @ConfigDefault("true")
    public boolean getDeleteFromLocalWhenJobEnd();

}
