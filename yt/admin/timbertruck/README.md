### Timbertruck

Utility to send YT log files to YTQueue.
It tracks log files of other processes, and reliably sends them to YTQueue (or logbroker).


#### Configuration

Example of configuration:
```yaml
# Configuration is YAML file.

# REQUIRED.
# Working directory.
# Directory where timbertruck keeps its working files i.e. its state (what files are to be send), and hard links to yet unsent files.
work_dir: /yt/disk2/hume-data/master-logs/timbertruck

# OPTIONAL.
# Log file. If not specified timbertruck writes logs to stderr.
# Timbertruck reopens logfile when receives SIGHUP.
log_file: /yt/disk2/hume-data/master-logs/timbertruck.debug.log

# OPTIONAL.
# Log file for errors. If not specified timbertruck does not duplicate error logs to a separate file.
# Timbertruck reopens logfile when receives SIGHUP.
error_log_file: /yt/disk2/hume-data/master-logs/timbertruck.error.log

# OPTIONAL.
# LoggerBufferSize is the buffer size in bytes for the timbertruck logger.
#
# Default value is 32768 (32 KiB).
logger_buffer_size: 16384

# OPTIONAL.
# LogrotatingTimeout defines the interval before reopening the log file, e.g., "5s" for 5 seconds, "10m" for 10 minutes.
reopen_log_file_interval: 16m

# OPTIONAL.
# TVM Auth info. Used to Logbroker and YT authentication.
# If not specified Timbertruck searches env variable LB_TOKEN,
# and env variable YT_TOKEN or file ~/.yt/token for corresponding authentication tokens.
tvm_id: 100500
tvm_secret_file: /path/to/secret/file

# OPTIONAL.
# Hostname. If not specified timbertruck detects it automatically.
# Used to generate session ids.
#
hostname: m001-hume.man-pre.yp-c.yandex.net

# OPTIONAL.
# Timbertruck writes its pid to this file. If not specified it writes it to the {work_dir}/timbertruck.pid
pid_file: /yt/disk2/hume-data/master-logs/timbertruck.pid

# OPTIONAL.
# Configuration of internal http server that provides metrics.
admin_panel:
  # REQUIRED.
  # Port to be listened.
  port: 8080

  # OPTIONAL.
  # Default metric tags.
  monitoring_tags: {cluster: hume}

  # OPTIONAL.
  # Metrics stream format.
  # Possible values: 'spack', 'json'. 
  # Default value is 'spack'.
  metrics_format: spack

# List of json log files to send (i.e. logs where each line of file is JSON).
json_logs:
-
  # REQUIRED
  # name of the stream (must be unique for all logs tracked by timberuck)
  name: access
  # REQUIRED
  # path to the log file to track
  log_file: /yt/disk2/freud-data/master-logs/master-sas5-9603.access.json.log

  # OPTIONAL
  # Maximum number of unsent files in log.
  # If a new log file appears but number of active tasks already reached max_active_task_count
  # new log file is skipped.
  #
  # If max_active_task_count is not specified, default value is 100.
  max_active_task_count: 50

  # OPTIONAL
  # Buffer size at which a flush to the output is triggered.
  # It must be greater than or equal to text_file_line_limit.
  # 
  # Default value is 16777216 (16 MiB).
  queue_batch_size: 1048576

  # OPTIONAL
  # Maximum allowed length of a line in the text file.
  # Lines longer than this value will be truncated.
  #
  # Default value is 16777216 (16 MiB).
  text_file_line_limit: 1048576

  # REQUIRED
  # Description of YTQueue to send logs to.
  yt_queue:
    - cluster: hahn
      queue_path: //path/to/queue_table
      producer_path: //path/to/producer_table
      # rpc_proxy_role: default

# List of text YT logs to send.
text_logs:
- name: master_text
  # cluster and tskv_format are put to tskv value and used by Logfeller to parse records.
  cluster: hahn
  tskv_format: yt-raw-master-log

  # Described above
  queue_batch_size: 1048576
  text_file_line_limit: 1048576

  # Described above
  log_file: /yt/disk2/freud-data/master-logs/master-sas5-9603.debug.log
  logbroker_topic: /yt/dev/test_topic
  max_active_task_count: 100
  yt_queue:
    - cluster: hahn
      queue_path: //path/to/queue_table
      producer_path: //path/to/producer_table
      rpc_proxy_role: default

# OPTIONAL
# Configuration for verification file uploader component.
verification_file_uploader:
  # REQUIRED
  # YT cluster to upload log files to.
  yt_cluster: "hahn"
  
  # REQUIRED
  # Directory path in YT where log files will be stored.
  yt_log_files_dir: "//home/logs/uploaded"
  
  # OPTIONAL
  # Minimum time since last modification a log file must have before it can be uploaded.
  # Files modified more recently than this will be skipped.
  # Default: 5h
  min_log_file_age: 5h
  
  # OPTIONAL
  # Maximum time since last modification a log file can have to be eligible for upload.
  # Files modified longer ago than this will be skipped.
  # Default: 10h
  max_log_file_age: 10h
  
  # OPTIONAL
  # How often the uploader scans for new log files to upload.
  # Default: 6h
  scan_log_files_interval: 6h
  
  # REQUIRED
  # List of log file stream configurations to monitor and upload.
  streams:
    - file_pattern: "/yt/disk2/freud-data/master-logs/master-sas5-9603.debug.log*"
      log_format: "text"
    - file_pattern: "/yt/disk2/freud-data/master-logs/master-sas5-9603.access.json*"
      log_format: "json"
```


#### Metrics

`tt.application.restart_count`
: number of timbertruck daemon restarts

`tt.application.error_log_count`
: number of errors in logs

`tt.stream.active_tasks`
: number of the tasks (files) that are not fully processed yet
