## Python SDK


Availabe as a package in [PyPI](https://pypi.org/project/ytsaurus-client/).




**Releases:**

{% cut "**0.13.46**" %}

**Release date:** 2026-01-18


#### Fixes
- Fixed `yt execute` for commands without input data
- Removed display of authorization headers in logs

{% endcut %}


{% cut "**0.13.45**" %}

**Release date:** 2025-12-29


#### Features
* Pass compression_level to parquet writer
* Add queue_tag and consumer_tag for queue and consumer metrics


{% endcut %}


{% cut "**0.13.44**" %}

**Release date:** 2025-12-12


#### Features
* Introduced `list-job-traces`
* Introduced `check-operation-permission`

#### Fixes
* Make `trace_id` for `get-job-trace` optional
* Fixed `transform` errors when specifying `data size_per_job` or `data_size` in the user spec


{% endcut %}


{% cut "**0.13.43**" %}

**Release date:** 2025-11-22


#### Features
  * Add annotations option in `start_query` command in CLI.

#### Fixes
  * Fix `push_queue_producer` retries.
  * Fix layer detection on unknown OS.

{% endcut %}


{% cut "**0.13.42**" %}

**Release date:** 2025-11-14


#### Features
* Enable retries for RPC calls
* More type-hints
* Introduced `get-job-trace` to CLI
* Added `--stderr-type` to `get-job-stderr`
* Added warnings about using `multithreading`

#### Fixes
* `transform` command preserves attributes (`compression_codec`, `erasure_codec`, `optimize_for`) from the destination table if they are not explicitly overridden
* Fixed `--config` for `yt-fuse`

{% endcut %}


{% cut "**0.13.41**" %}

**Release date:** 2025-10-24


#### Features
  * Added `--with-env-patch` option to `show-default-config` CLI command to dump default config with environment variables applied

#### Fixes
  * Fixed parsing of `YPath` when specifying a cluster and ranges
  * Fixed `spec_builder` when passing `client=None`

{% endcut %}


{% cut "**0.13.40**" %}

**Release date:** 2025-10-13


#### Features
  * YT-26355: Infer Null type from Arrow schema
  * YT-26389: Support omit_inaccessible_rows
  * Added `log_once` function

{% endcut %}


{% cut "**0.13.39**" %}

**Release date:** 2025-10-10


#### Features
* Process YQL quieries in separated processes (Commit: 3c09bed1d8d4ed07c1b4fe9393c39bb420c7dbc0)
* Added `clip_timestamp` option (Commit: 4e6889f0cd0615cee3d5d5ae0d85602233e2412f)
* Added task count in parallel read (Commit: 506e97dd397e29eeaa4e7b88f48467aa4419c48c)


#### Fixes
* Fix passing abort messages (Commit: b2b49815a043b78e8a3160f05400864a0fef678c)
* Fix Handle some environment variables with types (`YT_CHUNK_SIZE`) (Commit: d2109522d473a3126eb5f9258089d41689549621)
* Fix dirtable reader (Commit: 9ea085c12bdff6c8a67b5ad1ea6236db4da32771)
* Add warning about retry (Commit: 507d120389cb8963d25efe21102e2c35428a9d2f)


{% endcut %}


{% cut "**0.13.36**" %}

**Release date:** 2025-08-29


#### Features

  * Supported [blob](https://ytsaurus.tech/docs/en/user-guide/storage/formats#BLOB) table format

#### Fixes

  * Fixed heavy proxy selection logic for heavy requests
  * Fixed `get_table_schema` for replicated tables
  * Fixed `yt execute` for commands with input data

{% endcut %}


{% cut "**0.13.35**" %}

**Release date:** 2025-08-12


#### Features
  * Add compression codec option for parquet.
  * Add methods for distributed write API.
  * Add typing for config and for spec builders.

#### Fixes
  * Add details to import error of pickling encryption.
  * Fix wording of write_table description.
  * Fix YSON convert for bytes object.
  * Remove old *_ratio and *_share terms from YT CLI.

{% endcut %}


{% cut "**0.13.34**" %}

**Release date:** 2025-07-27


#### Features
  * Added support for `YT_LOG_PATH` for RPC requests
  * Added `--attribute` argument for `list_operations` CLI command
  * Rework local RPC connection configuration

{% endcut %}


{% cut "**0.13.33**" %}

**Release date:** 2025-07-14


#### Features
  * Support tz types in python
  * Add type hints for YtClient config
  * Support custom auth class in Python SDK config

#### Fixes
  * Fix hiding tokens in case of YtProxyUnavailable exception

{% endcut %}


{% cut "**0.13.31**" %}

**Release date:** 2025-06-20


#### Features
 * Minor improvements
 * Bump py-dependencies 2f5dc26abd27401d7c775b4e7406b4c85c1c4105

{% endcut %}


{% cut "**0.13.30**" %}

**Release date:** 2025-06-16


#### Features
  * Introduce `list_operation_events` command


{% endcut %}


{% cut "**0.13.29**" %}

**Release date:** 2025-06-02


#### Features
  * Add `annotate_with_types` to `yson_to_json` function
  * Improve proxy banned warning message

#### Fixes
  * Remove `YtSequoiaRetriableError`
  * Fix handling errors in `write_table` with enabled framing


{% endcut %}


{% cut "**0.13.28**" %}

**Release date:** 2025-04-30


#### Features

- Turn on `redirect_stdout_to_stderr` by default
- Add password strength validation in `set_user_password` request

{% endcut %}


{% cut "**0.13.27**" %}

**Release date:** 2025-04-18


#### Features
* Active users of dynamic tables APIs are encouraged to use RPC proxies
* Support using /api/v4/discover_proxies handler instead of /hosts

#### Fixes
* Error on getting `impersonation_user` setting from configuration

{% endcut %}


{% cut "**0.13.26**" %}

**Release date:** 2025-03-25


#### Features
* Add support of `expression` and `aggregate` properties in TableSchema.
* Add impersonation support.
* Do not strip docker host in spec builder.
* Add logging of bad requests.
* Bump ytsaurus-client dependencies.

{% endcut %}


{% cut "**0.13.25**" %}

**Release date:** 2025-03-12


#### Features
* Introduce `yt whoami` command

#### Fixes
* Fix issue-token output format


{% endcut %}


{% cut "**0.13.24**" %}

**Release date:** 2025-03-02


#### Features
* Replace `include_scheduler` option with `include_runtime` option in `get_operation` command (it is backward incompatible change)
* Request `type` attributes instead of `operation_type` attribute in `get_operation` command
* Add `redirect_stdout_to_stderr` support
* Support `require_sync_replica` in `push_queue_producer`
* Add `is_prerequisite_check_fail` method to error, add `YtAuthenticationError`
* Support operation suspend reason

#### Fixes
* Drop python2 related code in `_py_runner.py`
* Add `python_requires=">=3.8"` to package setup
* Do not request all attributes in operation exists check
* Fix handling request timeout of `start_operation` command 

{% endcut %}


{% cut "**0.13.23**" %}

**Release date:** 2025-02-04


#### Features
  * Add `min_batch_row_count` option to dump parquet
  * Add `patch_operation_spec` method
  * Add queue producer methods in YT cli
  * Add `trimmed_row_counts` parameter
  * Add `versioned_read_options` parameter
  * Add `ignore_type_mismatch` parameter
  * Do not write command line into started_by (by default)
  * Show native libraries version in CLI
  * Make pickling->dynamic_libraries->enable_auto_collection remote patchable
  * Apply destination path attributes to temporary objects within parallel upload

#### Fixes
  * YSON: unescape invalid seqs as in bingings implementation.
  * Fix `generate_traceparent`
  * Remove `typing_extensions` module imports for newer python versions
  * Fix native driver configuration

{% endcut %}


{% cut "**0.13.22**" %}

**Release date:** 2025-01-10


#### Fixes:
* Fix import checks for `orc` related functions

{% endcut %}


{% cut "**0.13.21**" %}

**Release date:** 2024-12-26


Features:
* Introduce YAML format support
* Introduce the higher level primitives for tracking queries
* Add `network_project` option setter for `UserJobSpecBuilder`
* Add parallel mode for ORC format
* Support `omit_inaccessible_columns` for read commands
* Support `preserve_acl` option in copy/move commands
* Rework authentication commands in CLI over getpass
* Dirtable upload improvements
* Add queue producer commands
* Improve SpecBuilder: add use_columnar_statistics, ordered, data_size_per_reduce_job

Fixes:
* Fix retries for parquet/orc upload commands

Cosmetics:
* Remove legacy constant from operation_commands.py
* Beautify imports: drop Python 2 support
* Wrap doc links into constants for `--help` command

Many thanks to @zlobober for significant contribution!

{% endcut %}


{% cut "**0.13.19**" %}

**Release date:** 2024-10-15


Features:
* Add possibility to upload and dump tables in ORC format using CLI commands: `upload-orc` and `dump-orc`
* Support parallel mode for `dump-parquet` command
* Support nullable fields during parsing YT schema from parquet schema
* Support parallel mode for `read_table_structured` command
* Add cli params to docker respawn decorator (PR: #849). Thanks @thenno for the PR!

Fixes:
* Do not retry `LineTooLong` error
* Fix `read_query_result` always returning raw results (PR: #800). Thanks @zlobober for the PR!
* Fix cyclic references that were causing memory leaks
* Reduce default value of `write_parallel/concatenate_size` from 100 to 20
* Fix retries in `upload-parquet` command

{% endcut %}


{% cut "**0.13.18**" %}

**Release date:** 2024-07-26


Features:
* Use expanduser for `config["token_path"]`
* Support custom dill params
* Support Nullable patchable config element
* Add max_replication_factor in config
* Use strawberry ctl address from cypress client_config

Fixes:
* Fixes of E721: do not compare types, for exact checks use `is` / `is not`, for instance checks use `isinstance()`
* Fix bug in YT python wrapper: stop transaction pinger before exiting transaction

Thanks to multiple outside contributors for the active participation in Python SDK development.

{% endcut %}


{% cut "**0.13.17**" %}

**Release date:** 2024-06-26


Features: 
  - Support profiles in configuration file
  - Add versioned select
  - Add enum.StrEnum and enum.IntEnum support for yt_dataclasses

Fixes:
  - Fix test_operation_stderr_output in py.test environment

Thanks to @thenno for considerable contribution!



{% endcut %}


{% cut "**0.13.16**" %}

**Release date:** 2024-06-19


Features:
- Allow to specify prerequisite transaction ids in client.Transaction context manager (PR: #638). Thanks @chegoryu for the PR!
- Add client and chunk_count parameters to dirtable_commands
- Add alter_query command for Query Tracker
- Add dump_job_proxy_log command (PR: #594). Thanks @tagirhamitov  for the PR!

Fixes:
- Fix return result of lock command in case of batch client
- Fix jupyter notebooks for operations in separate cells (PR: #654). Thanks @dmi-feo for the PR!

{% endcut %}


{% cut "**0.13.14**" %}

**Release date:** 2024-03-09


Features:
- Added an option for skipping rows merge in select
- Support composite types in QL
- Add `preserve_account` option to table backup commands
- Expand the list of dynamic table retriable errors
- Enhance table creation with specified append attribute
- Various improvements of maintenance API
- Support `upload_parquet` command

Fixes:
- Support SortColumn serialization
- Fix file descriptors leak in config parsing
- Fix output stream validation for TypedJobs


{% endcut %}


{% cut "**0.13.12**" %}

**Release date:** 2023-12-14


Features:
* Support `double` and `float` type in `@yt_dataclass`.
* Added `get_query_result` command.

Fixes:
* Fixed setting config from environment variables.
* Eligeable error message if node type is not equal to table in operation spec.

{% endcut %}

