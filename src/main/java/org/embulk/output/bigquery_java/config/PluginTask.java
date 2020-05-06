package org.embulk.output.bigquery_java.config;

import java.util.List;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;


public interface PluginTask
        extends Task {

    @Config("mode")
    @ConfigDefault("\"append\"")
    String getMode();

    @VisibleForTesting
    void setMode(String mode);

    // TODO: default should be application default
    @Config("auth_method")
    @ConfigDefault("\"service_account\"")
    String getAuthMethod();

    @Config("json_keyfile")
    String getJsonKeyfile();

    @Config("dataset")
    String getDataset();

    @Config("table")
    String getTable();

    @Config("location")
    @ConfigDefault("null")
    Optional<String> getLocation();

    @Config("column_options")
    @ConfigDefault("null")
    Optional<List<BigqueryColumnOption>> getColumnOptions();

    @Config("compression")
    @ConfigDefault("\"NONE\"")
    String getCompression();

    @Config("source_format")
    String getSourceFormat();

    @Config("path_prefix")
    @ConfigDefault("null")
    Optional<String> getPathPrefix();

    void setPathPrefix(Optional<String> pathPrefix);

    @Config("default_timezone")
    @ConfigDefault("\"UTC\"")
    String getDefaultTimezone();

    @Config("default_timestamp_format")
    @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%6N %:z\"")
    String getDefaultTimestampFormat();

    //TODO: make this optional
    @Config("file_ext")
    @ConfigDefault("null")
    Optional<String> getFileExt();

    void setFileExt(Optional<String> fileExt);

    @Config("encoding")
    @ConfigDefault("\"UTF-8\"")
    String getEncoding();

    @Config("auto_create_dataset")
    @ConfigDefault("false")
    boolean getAutoCreateDataset();

    @Config("auto_create_table")
    @ConfigDefault("true")
    boolean getAutoCreateTable();

    @VisibleForTesting
    void setAutoCreateTable(boolean autoCreateTable);

    @Config("max_bad_records")
    @ConfigDefault("0")
    int getMaxBadRecords();

    @Config("ignore_unknown_values")
    @ConfigDefault("false")
    boolean getIgnoreUnknownValues();

    @Config("allow_quoted_newlines")
    @ConfigDefault("false")
    boolean getAllowQuotedNewlines();

    @Config("template_table")
    @ConfigDefault("null")
    Optional<String> getTemplateTable();

    @Config("job_status_polling_interval")
    @ConfigDefault("10")
    long getJobStatusPollingInterval();

    @Config("job_status_max_polling_time")
    @ConfigDefault("3600")
    long getJobStatusMaxPollingTime();

    @Config("temp_table")
    @ConfigDefault("null")
    Optional<String> getTempTable();

    void setTempTable(Optional<String> tempTable);

    @Config("delete_from_local_when_job_end")
    @ConfigDefault("true")
    boolean getDeleteFromLocalWhenJobEnd();

    @Config("is_skip_job_result_check")
    @ConfigDefault("false")
    boolean getIsSkipJobResultCheck();

    @Config("abort_on_error")
    @ConfigDefault("null")
    Optional<Boolean> getAbortOnError();

    void setAbortOnError(Optional<Boolean> abortOnError);

    // TODO: this is not corresponding to before_load SQL syntax
    @Config("enable_standard_sql")
    @ConfigDefault("false")
    boolean getEnableStandardSQL();

    @Config("retries")
    @ConfigDefault("5")
    int getRetries();

    @Config("before_load")
    @ConfigDefault("null")
    Optional<String> getBeforeLoad();

    @Config("time_partitioning")
    @ConfigDefault("null")
    Optional<BigqueryTimePartitioning> getTimePartitioning();

    void setTimePartitioning(Optional<BigqueryTimePartitioning> bigqueryTimePartitioning);
}
