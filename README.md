# embulk-output-bigquery_java

[Embulk](https://github.com/embulk/embulk/) output plugin to load/insert data into [Google BigQuery](https://cloud.google.com/bigquery/) using [direct insert](https://cloud.google.com/bigquery/loading-data-into-bigquery#loaddatapostrequest)

## Overview

This plugin is targeting Java version of [embulk-output-bigquery](https://github.com/embulk/embulk-output-bigquery) and some additional functions. Most of features are not implemented right row. You should use jruby version for stable transfer.

load data into Google BigQuery as batch jobs for big amount of data
https://developers.google.com/bigquery/loading-data-into-bigquery

* **Plugin type**: output
* **Resume supported**: no
* **Cleanup supported**: no
* **Dynamic table creating**: yes

### NOT IMPLEMENTED
* insert data over streaming inserts
  * for continuous real-time insertions
  * Please use other product, like [fluent-plugin-bigquery](https://github.com/kaizenplatform/fluent-plugin-bigquery)
  * https://developers.google.com/bigquery/streaming-data-into-bigquery#usecases

Current version of this plugin supports Google API with Service Account Authentication, but does not support
OAuth flow for installed applications.

## Difference to [embulk-output-bigquery](https://github.com/embulk/embulk-output-bigquery)
- before_load
  SQL query is executed before load to the table


## Configuration

Under construction

#### Original options

| name     (x) is unsupported                            | type        | required?  | default                  | description            |
|:-------------------------------------|:------------|:-----------|:-------------------------|:-----------------------|
|  mode (replace, append is supported)                                | string      | optional   | "append"                 | See [Mode](#mode)      |
|  auth_method (service_account is supported)                        | string      | optional   | "application\_default"   | See [Authentication](#authentication) |
|  json_keyfile                        | string      | optional   |                          | keyfile path or `content` |
|  project  (x)                           | string      | required unless service\_account's `json_keyfile` is given. | | project\_id |
|  dataset                             | string      | required   |                          | dataset |
|  location                            | string      | optional   | nil                      | geographic location of dataset. See [Location](#location) |
|  table                               | string      | required   |                          | table name, or table name with a partition decorator such as `table_name$20160929`|
|  auto_create_dataset                 | boolean     | optional   | false                    | automatically create dataset |
|  auto_create_table                   | boolean     | optional   | true                     | `false` is available only for `append_direct` mode. Other modes require `true`. See [Dynamic Table Creating](#dynamic-table-creating) and [Time Partitioning](#time-partitioning) |
|  schema_file   (x)                      | string      | optional   |                          | /path/to/schema.json |
|  template_table                      | string      | optional   |                          | template table name. See [Dynamic Table Creating](#dynamic-table-creating) |
|  job_status_max_polling_time         | int         | optional   | 3600 sec                 | Max job status polling time |
|  job_status_polling_interval         | int         | optional   | 10 sec                   | Job status polling interval |
|  is_skip_job_result_check            | boolean     | optional   | false                    | Skip waiting Load job finishes. Available for append, or delete_in_advance mode |
|  with_rehearsal  (x)                    | boolean     | optional   | false                    | Load `rehearsal_counts` records as a rehearsal. Rehearsal loads into REHEARSAL temporary table, and delete finally. You may use this option to investigate data errors as early stage as possible |
|  rehearsal_counts  (x)                  | integer     | optional   | 1000                     | Specify number of records to load in a rehearsal |
|  abort_on_error                      | boolean     | optional   | true if max_bad_records is 0, otherwise false | Raise an error if number of input rows and number of output rows does not match |
|  column_options  (not fully supported)                    | hash        | optional   |                          | See [Column Options](#column-options) |
|  default_timezone                    | string      | optional   | UTC                      | |
|  default_timestamp_format            | string      | optional   | %Y-%m-%d %H:%M:%S.%6N    | |
|  payload_column  (x)                    | string      | optional   | nil                      | See [Formatter Performance Issue](#formatter-performance-issue) |
|  payload_column_index  (x)               | integer     | optional   | nil                      | See [Formatter Performance Issue](#formatter-performance-issue) |
|  gcs_bucket   (x)                       | string      | optional   | nil                      | See [GCS Bucket](#gcs-bucket) |
|  auto_create_gcs_bucket (x)              | boolean     | optional   | false                    | See [GCS Bucket](#gcs-bucket) |
|  progress_log_interval  (x)            | float       | optional   | nil (Disabled)           | Progress log interval. The progress log is disabled by nil (default). NOTE: This option may be removed in a future because a filter plugin can achieve the same goal |
|  before_load          | string       | optional   | nil            |  if set, this SQL will be executed before loading all records in append mode. In replace mode, SQL is not executed. |


Client or request options

| name                                 | type        | required?  | default                  | description            |
|:-------------------------------------|:------------|:-----------|:-------------------------|:-----------------------|
|  open_timeout_sec  (x)                  | integer     | optional   | 300                      | Seconds to wait for the connection to open |
|  timeout_sec       (x)                  | integer     | optional   | 300                      | Seconds to wait for one block to be read (google-api-ruby-client < v0.11.0) |
|  send_timeout_sec   (x)                 | integer     | optional   | 300                      | Seconds to wait to send a request (google-api-ruby-client >= v0.11.0) |
|  read_timeout_sec   (x)                 | integer     | optional   | 300                      | Seconds to wait to read a response (google-api-ruby-client >= v0.11.0) |
|  retries                            | integer     | optional   | 5                        | Number of retries |
|  application_name   (x)                  | string      | optional   | "Embulk BigQuery plugin" | User-Agent |
|  sdk_log_level      (x)                 | string      | optional   | nil (WARN)               | Log level of google api client library |


Options for intermediate local files

| name                                 | type        | required?  | default                  | description            |
|:-------------------------------------|:------------|:-----------|:-------------------------|:-----------------------|
|  path_prefix                         | string      | optional   |                          | Path prefix of local files such as "/tmp/prefix_". Default randomly generates with [tempfile](http://ruby-doc.org/stdlib-2.2.3/libdoc/tempfile/rdoc/Tempfile.html) |
|  sequence_format   (x)                  | string      | optional   | .%d.%d                   | Sequence format for pid, thread id |
|  file_ext                            | string      | optional   |                          | The file extension of local files such as ".csv.gz" ".json.gz". Default automatically generates from `source_format` and `compression`|
|  skip_file_generation (x)                | boolean     | optional   |                          | Load already generated local files into BigQuery if available. Specify correct path_prefix and file_ext. |
|  delete_from_local_when_job_end      | boolean     | optional   | true                     | If set to true, delete generate local files when job is end |
|  compression                         | string      | optional   | "NONE"                   | Compression of local files (`GZIP` or `NONE`) |

`source_format` is also used to determine formatter (csv or jsonl).

#### Same options of bq command-line tools or BigQuery job's property

Following options are same as [bq command-line tools](https://cloud.google.com/bigquery/bq-command-line-tool#creatingtablefromfile) or BigQuery [job's property](https://cloud.google.com/bigquery/docs/reference/v2/jobs#resource).

| name                              | type     | required? | default | description            |
|:----------------------------------|:---------|:----------|:--------|:-----------------------|
|  source_format   (jsonl is available)                 | string   | required  | "CSV"   |   File type (`NEWLINE_DELIMITED_JSON` or `CSV`) |
|  max_bad_records                  | int      | optional  | 0       | |
|  field_delimiter  (x)                | char     | optional  | ","     | |
|  encoding         (x)                | string   | optional  | "UTF-8" | `UTF-8` or `ISO-8859-1` |
|  ignore_unknown_values (x)            | boolean  | optional  | false   | |
|  allow_quoted_newlines  (x)          | boolean  | optional  | false   | Set true, if data contains newline characters. It may cause slow procsssing |
|  time_partitioning       (x)         | hash     | optional  | `{"type":"DAY"}` if `table` parameter has a partition decorator, otherwise nil | See [Time Partitioning](#time-partitioning) |
|  time_partitioning.type   (x)        | string   | required  | nil     | The only type supported is DAY, which will generate one partition per day based on data loading time. |
|  time_partitioning.expiration_ms (x)  | int      | optional  | nil     | Number of milliseconds for which to keep the storage for a partition. |
|  time_partitioning.field   (x)       | string   | optional  | nil     | `DATE` or `TIMESTAMP` column used for partitioning |
|  clustering     (x)                   | hash     | optional  | nil     | Currently, clustering is supported for partitioned tables, so must be used with `time_partitioning` option. See [clustered tables](https://cloud.google.com/bigquery/docs/clustered-tables) |
|  clustering.fields  (x)               | array    | required  | nil     | One or more fields on which data should be clustered. The order of the specified columns determines the sort order of the data. |
|  schema_update_options  (x)           | array    | optional  | nil     | (Experimental) List of `ALLOW_FIELD_ADDITION` or `ALLOW_FIELD_RELAXATION` or both. See [jobs#configuration.load.schemaUpdateOptions](https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.schemaUpdateOptions). NOTE for the current status: `schema_update_options` does not work for `copy` job, that is, is not effective for most of modes such as `append`, `replace` and `replace_backup`. `delete_in_advance` deletes origin table so does not need to update schema. Only `append_direct` can utilize schema update. |



## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
