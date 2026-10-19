## Python SDK


Available as a package in [PyPI](https://pypi.org/project/ytsaurus-client/).




**Releases:**

{% cut "**0.13.55**" %}

**Release date:** 2026-10-02


### Features
- Add the `job_binary_local_path` and `job_binary_cypress_path` methods to `UserJobSpecBuilder` to run Python function and C++ jobs with the specified executable instead of the current one [7e2aa2780fffded19b274df1db787b5576c75fd3]

### Fixes
- Retry `yt flow` commands when the flow controller has not built its flow view yet (error code `3302`) [c235cd45b50a94ef04adff99e0a4e3dd44ccc0a1]

{% endcut %}


{% cut "**0.13.54**" %}

**Release date:** 2026-08-30


### Features
- Add the `pickling/encryption_engine` config option and `YT_ENCRYPTION_ENGINE` environment variable to select the pickle file encryption engine: `cryptography_fernet` (default) or `native_chacha`, a ChaCha20 implementation that uses OpenSSL through `ctypes` with a pure Python fallback and does not require the `cryptography` package
[4089571ffc63e4d51efd4d3d307ca4e6ca1ab697, 900a7412007038c7f8f8218241044b73d7252d7e]
- Add experimental `yt admin bundle-controller` commands (`init`, `create-bundle`, `drop-allocations`, `set-bundle-resource-limits`) for initializing the Bundle controller configuration and managing bundles [0978827940a977bb6fa4ac0367cfe8e1894e7900]
- Add `--max-points-per-series` to `yt admin metrics dump`; by default, a time range and `--step` combination that needs more than 11000 points per series is now rejected before any query is sent [1ff11ecf162b7c571eb92dd72995a7b138b7a01c]
- Add `-q`/`--quiet` to `yt check-permission` to suppress output and report the result through the exit code: `0` if the permission is granted, `1` otherwise [f3b2a1cdbd46c8326c1b4e45893ab9be7e5611c4]
- Add the `cumulative_data_weights` parameter to `reshard_table` and `--cumulative-data-weights` to `yt reshard-table` to set initial cumulative data weights for the new tablets of an ordered dynamic table [13d9311430e32d5a9fe93ab5212454dc6e29a8f9]
- Add the `YT_PREFER_HTTPS` environment variable for the `proxy/prefer_https` config option [cd416bd9ba31dbbda519ece6760b252213892de1]
- Add the `driver_enable_fork_detection` config option and `YT_DRIVER_ENABLE_FORK_DETECTION` environment variable to make the driver raise an error when it is used in a forked process [6ccfe4cff5fed6ddbdc8d8619da6042711400fb7]

### Fixes
- Add `tornado` to the `recommended` extra: `yt run-job-shell` requires it, and the PyPI package does not bundle it [bf184950e2548cb940518f4830bdb449c8bfcb0c]
- Improve the error message for an authentication token with leading or trailing whitespace [6eb002957aeeca372c94a3d67380d6c946705498]
- Fix `yt flow` commands to retry transient errors using the standard client retry policy [2ef4e45a92a078f3f07691db0a7544bb30d8b6e5]
- Update vendored `urllib3` to 1.26.16 to eliminate `ResourceWarning: unclosed socket` warnings [d0d51f13800cfcc6e806d8d27504d1a6176bfdf2]
- Fix `TypeError` in `YtError.find_matching_error` when an inner error is not in the YTsaurus error format, for example in a `502` response from an intermediate gateway [83fb173150aea2e3e9ed253f417d0bb2a29cd63b]
- Constrain `argcomplete` to versions below 3.7.1 for Python older than 3.10 [806a90e36e448eb42a6ec99592eb9bb962840e88]
- Fix `yt admin metrics replay` for archives without dashboards, and fix `yt admin metrics dump` handling of escape sequences in label values, selectors with duplicate label matchers or empty-matching regular expressions, and an empty `defaults` section in the spec
[15c6b5d4357a09422710b4db5ebf3957b71b65e1, e8a422599344201cc86617edbb1fb2c3a34e3b8f, 7e8307d8849770a5ad323d9ba40b2ece59c38921, c659950d81fe219c09352c090fa91aff5f153476]


{% endcut %}


{% cut "**0.13.53**" %}

**Release date:** 2026-07-24


### Features
- Make `yt admin logs k8s` use the active kubeconfig or in-cluster namespace by default, falling back to `default` [77e6e2c7f83aa240c9bddf2500577c6c3087776b]
- Add `yt dq` CLI commands for managing DQ cliques through Strawberry [c1c4106e9dfa2c32f7565506d17bb8167b245454]
- Add the `annotate_objects` config option and `YT_ANNOTATE_OBJECTS` environment variable to automatically attach underscore-prefixed attributes to newly created tables, files, and map nodes
[6c162f1874bf54cd5ce5eaea94eb845dda3bba8a, 734943a5e2e14b3590013e924a61e90c18db4481]
- Add `--status-only` to `yt flow describe-pipeline` to request only the pipeline status and related messages instead of its full description [bc0f6eb4c49e71cc7fed6e345153a59620894232]

### Fixes
- Fix batch request retries to recognize retriable errors nested in individual batch responses [4d0033adc1f595d57a8e203f22ee08568209d791]
- Fix wrapper retries after failed master transaction commits when using the native driver through an RPC proxy [ec101cfdbd3089d85d576367d7719150f5db633a]
- Improve diagnostics when heavy-proxy discovery finds no proxy matching `http_proxy_role`, including explicit errors when light-proxy fallback is disabled [44df8fae938e8de235accde85690d155106f9845]
- Fix crashes when reading zero-row results in Arrow format [a7e02e14416e58384ba71c862013012ef1acc351]
- Fix `yt whoami` with invalid credentials to report the underlying authentication error instead of raising `AttributeError` [17b88749ad8696aabca8586237f32bf54c9602ff]

{% endcut %}


{% cut "**0.13.52**" %}

**Release date:** 2026-07-08


### Features
- Warn when a short cluster alias is used without `proxy/default_suffix` configured, and raise an informative `YtError` on DNS resolution failure instead of an opaque connection error [5677a1bceebbf064b5e0dc234d411ac8f0a0c82e]
- Add experimental (interface may change) `yt devtools image` CLI commands (`get-cluster-env`, `prepare`, `run`, `list`, `install`) to build and run Docker images matching the target cluster's OS/Python environment [7320a23fe95a982ac0bbb6442cca957a968066bc]

### Fixes
- Redact auth headers (including `request_headers`) in HTTP error responses and logs [98fdfad3f8561fc4cb9a932cc0e645189895891f, 6f9fde83692386df25e285223ff0d625c1a0e040]
- Fix `yt` CLI path autocompletion when a Cypress path `prefix` is configured [1ca058a74d91dc21991d6d125f629f6c5609f60f]
- Fix `YPath.join()`/`ypath_join()` silently dropping attributes when joining paths; add a `with_attributes` parameter to control merging [82b104d3f3e4e70aba4c995e221f56d31411af40]


{% endcut %}


{% cut "**0.13.51**" %}

**Release date:** 2026-06-26


### Features
- Add `yt-admin` CLI commands `get-user-banned`, `set-user-banned`, and `list-banned-users` for managing user bans [4a19859b389eb1668fb4f14396aab7d96801cfb6]
- Add `yt admin remove-master-unrecognized-options` command to remove unrecognized options from the master dynamic config after update [7beacb9aed028a9cf961a9e18cf43f28d95c453a]
- Add `check_cluster_liveness` method to `YtClient` (and module-level `yt.check_cluster_liveness`) with `check_cypress_root`, `check_secondary_master_cells`, and `check_tablet_cell_bundle` options [9286c63af6bee36ccd40d17f3d856da4cd0cfa11]

### Fixes
- Add `--timestamp` flag to the `lookup-rows` CLI command [973bea764d9c8038dc8727ea75c94f0bb43ea3da]
- Fix `YT_DRIVER_CONFIG` environment variable crashing with `TypeError` on Python 3 [9118cf7dc5a0f66cd042cc07f60ffd5e0108dad3]

### Breaking changes
- Removed the `TzDate`, `TzDatetime`, `TzTimestamp`, `TzDate32`, `TzDatetime64`, `TzTimestamp64` typed-API classes from `yt.wrapper.schema` [e12b3891d56424113226d891995706fb09da93f7]


{% endcut %}


{% cut "**0.13.50**" %}

**Release date:** 2026-05-29


#### Features
- Increase default `data_size_per_thread` for parallel file reading [99a0a95dee050b88a3415233d29214b0902ce3a9]
- Support operation alias in YT CLI and Python API [12fc8945f15a20c645c87667fad287c606ea47ec]
- Add `freeze-hydra-peer`, `truncate-changelog`, and `schedule-restart` commands to `yt-admin` [ae0cb902cca1e0caef31e7ed96983c0e05fd3126]
- Add experimental `yt admin metrics` commands for Prometheus metrics dump and local replay via Docker [d6d286ce7d084eab62be0c545fc967aa2e190a64]
- Support `omit_inaccessible_columns` and `omit_inaccessible_rows` in `read-table` [20d3bead559f0df0fa05d86a64b700e26c25fc14]

#### Fixes
- Use heavy proxy for `get_table_columnar_statistics` method [d8890491c7a333b0902f41e964fedcbe677a0781]


{% endcut %}


{% cut "**0.13.49**" %}

**Release date:** 2026-04-30


#### Features
- Experimental! Add `yt admin describe` and `yt admin logs k8s` commands for cluster inspection and log fetching via Kubernetes API [ff53dc4ccd39e574364e1b7846fa3f47b7badab3, d2d8f8bf40eba0e96ffb6a615b642931ffad92d5]
- Add `build_master_snapshots` and `master_exit_read_only` commands [fc953aa8229231760b219500d122ac87a9d16175, 986be02ca04da7b0b9c13827cbc94fd55cf4f3e0]
- Add `backoff_config` parameter to `run_with_retries` to allow customizing the retry backoff policy [56ed6b2223cc14f1fefb547d1ffd1ce68e494283]

#### Fixes
- Fix logger compatibility with Python 3.14 [93d89672ca44353644e68bed805cc9c9613d3eae]
- Hide `secure_vault` contents from request logs in RPC drivers [983f55e3f1d86a59f2be5f058c8e3ebacd48fa8c]
- CLI `yt execute` now raises a clear `YtError` when invoked with a command not supported by the cluster, instead of crashing with `KeyError` [d768e16d3069d0faf569eabe05aad042c9328b1f]


{% endcut %}


{% cut "**0.13.48**" %}

**Release date:** 2026-03-27


#### Features
- Add `read_from` parameter to `lookup_rows` and `select_rows` [2f0b0b0ae0aa23390785c7913866202ae03dbcf8]
- Add `--no-enable-slicing` option to `reshard-table` CLI command [62c8c5ab4ce0c6efd8b4ac190279ca55d9c69b2e]
- Add type hints to `lock` command [a3ac56216b9c9ddc860df1ff9942d532051e42b0]

#### Fixes
- Fix native driver to use address resolver config in server format [2816f6fe94f7feffc04d3ff547333cc4ffc1b8e8]
- Fix `make_read_request` to raise original error instead of possible abort transaction error [c223a8f738c04524828fb7268fd757beb4dd93cc]
- Fix retry logic when `retry_count` is `None` [db1ab343327831b0ea499e6e5c7a47db77fa08df]

{% endcut %}


{% cut "**0.13.47**" %}

**Release date:** 2026-02-16


### Features
- Add `run-job-shell-command` to CLI [394c049deb1460f767be591036f5d55b7d5d58db]
- Add `lock` attribute support for `ColumnSchema` [87a9d8809a144c64d72fc767999c8c9d25616911]
- Add support for distributed reads in `read_parallel` mode [01912a6703b7fea296efc3eb5fbaebd69ea2d046]

### Fixes
- Fix Docker image preparation using CLI [2788466412f56e941044e833dbfc201d1937807f]

{% endcut %}


{% cut "**0.13.46**" %}

**Release date:** 2026-01-18


### Fixes
- Fixed `yt execute` for commands without input data
- Removed display of authorization headers in logs

{% endcut %}


{% cut "**0.13.45**" %}

**Release date:** 2025-12-29


### Features
* Pass compression_level to parquet writer
* Add queue_tag and consumer_tag for queue and consumer metrics


{% endcut %}


{% cut "**0.13.44**" %}

**Release date:** 2025-12-12


### Features
* Introduced `list-job-traces`
* Introduced `check-operation-permission`

### Fixes
* Make `trace_id` for `get-job-trace` optional
* Fixed `transform` errors when specifying `data size_per_job` or `data_size` in the user spec


{% endcut %}


{% cut "**0.13.43**" %}

**Release date:** 2025-11-22


### Features
  * Add annotations option in `start_query` command in CLI.

### Fixes
  * Fix `push_queue_producer` retries.
  * Fix layer detection on unknown OS.

{% endcut %}


{% cut "**0.13.42**" %}

**Release date:** 2025-11-14


### Features
* Enable retries for RPC calls
* More type-hints
* Introduced `get-job-trace` to CLI
* Added `--stderr-type` to `get-job-stderr`
* Added warnings about using `multithreading`

### Fixes
* `transform` command preserves attributes (`compression_codec`, `erasure_codec`, `optimize_for`) from the destination table if they are not explicitly overridden
* Fixed `--config` for `yt-fuse`

{% endcut %}


{% cut "**0.13.41**" %}

**Release date:** 2025-10-24


### Features
  * Added `--with-env-patch` option to `show-default-config` CLI command to dump default config with environment variables applied

### Fixes
  * Fixed parsing of `YPath` when specifying a cluster and ranges
  * Fixed `spec_builder` when passing `client=None`

{% endcut %}


{% cut "**0.13.40**" %}

**Release date:** 2025-10-13


### Features
  * YT-26355: Infer Null type from Arrow schema
  * YT-26389: Support omit_inaccessible_rows
  * Added `log_once` function

{% endcut %}


{% cut "**0.13.39**" %}

**Release date:** 2025-10-10


### Features
* Process YQL quieries in separated processes (Commit: 3c09bed1d8d4ed07c1b4fe9393c39bb420c7dbc0)
* Added `clip_timestamp` option (Commit: 4e6889f0cd0615cee3d5d5ae0d85602233e2412f)
* Added task count in parallel read (Commit: 506e97dd397e29eeaa4e7b88f48467aa4419c48c)


### Fixes
* Fix passing abort messages (Commit: b2b49815a043b78e8a3160f05400864a0fef678c)
* Fix Handle some environment variables with types (`YT_CHUNK_SIZE`) (Commit: d2109522d473a3126eb5f9258089d41689549621)
* Fix dirtable reader (Commit: 9ea085c12bdff6c8a67b5ad1ea6236db4da32771)
* Add warning about retry (Commit: 507d120389cb8963d25efe21102e2c35428a9d2f)


{% endcut %}


{% cut "**0.13.36**" %}

**Release date:** 2025-08-29


### Features

  * Supported [blob](https://ytsaurus.tech/docs/en/user-guide/storage/formats#BLOB) table format

### Fixes

  * Fixed heavy proxy selection logic for heavy requests
  * Fixed `get_table_schema` for replicated tables
  * Fixed `yt execute` for commands with input data

{% endcut %}


{% cut "**0.13.35**" %}

**Release date:** 2025-08-12


### Features
  * Add compression codec option for parquet.
  * Add methods for distributed write API.
  * Add typing for config and for spec builders.

### Fixes
  * Add details to import error of pickling encryption.
  * Fix wording of write_table description.
  * Fix YSON convert for bytes object.
  * Remove old *_ratio and *_share terms from YT CLI.

{% endcut %}


{% cut "**0.13.34**" %}

**Release date:** 2025-07-27


### Features
  * Added support for `YT_LOG_PATH` for RPC requests
  * Added `--attribute` argument for `list_operations` CLI command
  * Rework local RPC connection configuration

{% endcut %}


{% cut "**0.13.33**" %}

**Release date:** 2025-07-14


### Features
  * Support tz types in python
  * Add type hints for YtClient config
  * Support custom auth class in Python SDK config

### Fixes
  * Fix hiding tokens in case of YtProxyUnavailable exception

{% endcut %}


{% cut "**0.13.31**" %}

**Release date:** 2025-06-20


## Features
 * Minor improvements
 * Bump py-dependencies 2f5dc26abd27401d7c775b4e7406b4c85c1c4105

{% endcut %}


{% cut "**0.13.30**" %}

**Release date:** 2025-06-16


## Features
  * Introduce `list_operation_events` command


{% endcut %}


{% cut "**0.13.29**" %}

**Release date:** 2025-06-02


## Features
  * Add `annotate_with_types` to `yson_to_json` function
  * Improve proxy banned warning message

## Fixes
  * Remove `YtSequoiaRetriableError`
  * Fix handling errors in `write_table` with enabled framing


{% endcut %}


{% cut "**0.13.28**" %}

**Release date:** 2025-04-30


## Features

- Turn on `redirect_stdout_to_stderr` by default
- Add password strength validation in `set_user_password` request

{% endcut %}


{% cut "**0.13.27**" %}

**Release date:** 2025-04-18


### Features
* Active users of dynamic tables APIs are encouraged to use RPC proxies
* Support using /api/v4/discover_proxies handler instead of /hosts

### Fixes
* Error on getting `impersonation_user` setting from configuration

{% endcut %}


{% cut "**0.13.26**" %}

**Release date:** 2025-03-25


### Features
* Add support of `expression` and `aggregate` properties in TableSchema.
* Add impersonation support.
* Do not strip docker host in spec builder.
* Add logging of bad requests.
* Bump ytsaurus-client dependencies.

{% endcut %}


{% cut "**0.13.25**" %}

**Release date:** 2025-03-12


### Features
* Introduce `yt whoami` command

### Fixes
* Fix issue-token output format


{% endcut %}


{% cut "**0.13.24**" %}

**Release date:** 2025-03-02


### Features
* Replace `include_scheduler` option with `include_runtime` option in `get_operation` command (it is backward incompatible change)
* Request `type` attributes instead of `operation_type` attribute in `get_operation` command
* Add `redirect_stdout_to_stderr` support
* Support `require_sync_replica` in `push_queue_producer`
* Add `is_prerequisite_check_fail` method to error, add `YtAuthenticationError`
* Support operation suspend reason

### Fixes
* Drop python2 related code in `_py_runner.py`
* Add `python_requires=">=3.8"` to package setup
* Do not request all attributes in operation exists check
* Fix handling request timeout of `start_operation` command 

{% endcut %}


{% cut "**0.13.23**" %}

**Release date:** 2025-02-04


### Features
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

### Fixes
  * YSON: unescape invalid seqs as in bindings implementation.
  * Fix `generate_traceparent`
  * Remove `typing_extensions` module imports for newer python versions
  * Fix native driver configuration

{% endcut %}


{% cut "**0.13.22**" %}

**Release date:** 2025-01-10


**Release page:** [0.13.22](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.22)


**PyPI package:** [0.13.22](https://pypi.org/project/ytsaurus-client/0.13.22/)


#### Fixes:
* Fix import checks for `orc` related functions

{% endcut %}


{% cut "**0.13.21**" %}

**Release date:** 2024-12-26


**Release page:** [0.13.21](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.21)


**PyPI package:** [0.13.21](https://pypi.org/project/ytsaurus-client/0.13.21/)


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


**Release page:** [0.13.19](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.19)


**PyPI package:** [0.13.19](https://pypi.org/project/ytsaurus-client/0.13.19/)


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


**Release page:** [0.13.18](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.18)


**PyPI package:** [0.13.18](https://pypi.org/project/ytsaurus-client/0.13.18/)


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


**Release page:** [0.13.17](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.17)


**PyPI package:** [0.13.17](https://pypi.org/project/ytsaurus-client/0.13.17/)


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


**Release page:** [0.13.16](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.16)


**PyPI package:** [0.13.16](https://pypi.org/project/ytsaurus-client/0.13.16/)


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


**Release page:** [0.13.14](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.14)


**PyPI package:** [0.13.14](https://pypi.org/project/ytsaurus-client/0.13.14/)


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


**Release page:** [0.13.12](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.12)


**PyPI package:** [0.13.12](https://pypi.org/project/ytsaurus-client/0.13.12/)


Features:
* Support `double` and `float` type in `@yt_dataclass`.
* Added `get_query_result` command.

Fixes:
* Fixed setting config from environment variables.
* Eligeable error message if node type is not equal to table in operation spec.

{% endcut %}

