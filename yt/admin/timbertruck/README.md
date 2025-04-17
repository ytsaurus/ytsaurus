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
  log_file: /yt/disk2/freud-data/master-logs/master-sas5-9603.debug.log
  logbroker_topic: /yt/dev/test_topic
  max_active_task_count: 100
  yt_queue:
    - cluster: hahn
      queue_path: //path/to/queue_table
      producer_path: //path/to/producer_table
      rpc_proxy_role: default
```


#### Metrics

`tt.application.restart_count`
: number of timbertruck daemon restarts

`tt.application.error_log_count`
: number of errors in logs

`tt.stream.active_tasks`
: number of the tasks (files) that are not fully processed yet
