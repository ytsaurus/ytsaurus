## Python SDK


Availabe as a package in [PyPI](https://pypi.org/project/ytsaurus-client/).




**Releases:**

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

