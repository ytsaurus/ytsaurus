## Logslice

Logslice is an utility which efficiently extracts log messages for specific time frame from compressed log file. It could additionaly invoke `grep` utility to perofrm filtration. It is provided with `logslice.py` script to invoke logslice on remote machine and pass `grep` arguments safely to avoid shell command injection, which makes in possible to use `logslice.py` in agents.

## Usage

Local `logslice` utility:
```
Usage: logslice [OPTIONS] log_file [-- GREP_ARGS...]
  -t|--start TIME    start of the time window (default: beginning of file)
  -e|--end TIME      end of the time window (default: end of file)
  --info             print first and last line timestamps
  --extend SECONDS   widen the window by this many seconds on both sides
                     (default: 0)
  --codec CODEC      compression codec: auto, zstd, gzip or plain
                     (default: "auto")
  {-g|--grep} ARGS   grep arguments as a single string, split into tokens
                     (quotes group multi-word patterns)
  GREP_ARGS          grep arguments as plain multiple arguments, for local use
```

Remote `logslice.py` script:
```
Usage: logslice.py host [--type type] [-l logslice] [-t start_time] [-e end_time] -- grep_args...
  host                       remote machine name
  --type {debug,error,info}  log type: debug, error or info (default: debug)
  -l LOGSLICE                path to a logslice binary
  -t START                   time window start (passed to logslice)
  -e END                     time window end (passed to logslice)
```

## Description

Logslice uses binary search to find which compression blocks correspond to start and end of a time window. `logslice.py` additionaly investigates `logs` directory on the remote machine to find YT-specific log files and identifies which of them correspond to time window.

## Example

```
$ time python3 logslice.py sas5-5383-tab-node-ada.sas.yp-c.yandex.net -t "2026-06-19 09:55:55,0" -e "2026-06-19 09:55:55,01" -- "Fair throttler tick details"
Connecting to sas5-5383-tab-node-ada.sas.yp-c.yandex.net (you may need to touch your security key)...
Found 1025 debug log file(s) for component 'node-sas5-5383'.
Selected 1 file(s): node-sas5-5383.debug.log.4.zst .. node-sas5-5383.debug.log.4.zst
2026-06-19 09:55:55,002980	D	ClusterNode	Fair throttler tick details (BucketIncome: {default: 56250000, static_store_preload_in: 56250000, user_backend_in: 56250000, replication_in: 56250000, store_compaction_and_partitioning_in: 56250000}, BucketUsage: {default: 0, static_store_preload_in: 0, user_backend_in: 0, replication_in: 0, store_compaction_and_partitioning_in: 0}, BucketDemands: {default: 0, static_store_preload_in: 0, user_backend_in: 0, replication_in: 0, store_compaction_and_partitioning_in: 0}, BucketQuota: {default: 281250000, static_store_preload_in: 281250000, user_backend_in: 281250000, replication_in: 281250000, store_compaction_and_partitioning_in: 281250000}, Direction: In)	DelayedExecutor	fffee60f5aa7a078	
2026-06-19 09:55:55,002999	D	ClusterNode	Fair throttler tick details (BucketIncome: {store_compaction_and_partitioning_out: 35156250, default: 35156250, dynamic_store_read_out: 35156250, snapshot_out: 35156250, store_flush_out: 35156250, user_backend_out: 35156250, changelog_out: 35156250, replication_out: 35156250}, BucketUsage: {store_compaction_and_partitioning_out: 0, default: 0, dynamic_store_read_out: 0, snapshot_out: 0, store_flush_out: 0, user_backend_out: 0, changelog_out: 0, replication_out: 0}, BucketDemands: {store_compaction_and_partitioning_out: 0, default: 0, dynamic_store_read_out: 0, snapshot_out: 0, store_flush_out: 0, user_backend_out: 0, changelog_out: 0, replication_out: 0}, BucketQuota: {store_compaction_and_partitioning_out: 175781250, default: 175781250, dynamic_store_read_out: 175781250, snapshot_out: 175781250, store_flush_out: 175781250, user_backend_out: 175781250, changelog_out: 175781250, replication_out: 175781250}, Direction: Out)	DelayedExecutor	fffee60f5aa25226	

real	0m20.290s
user	0m0.102s
sys	0m0.098s
```