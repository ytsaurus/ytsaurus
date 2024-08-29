### Python

Availabe as a package in [PyPI](https://pypi.org/project/ytsaurus-client/).

**Current release:** {{python-sdk-version}}

**All releases:**

{% cut "**0.13.18**" %}

Features:

- Use expanduser for `config["token_path"]`.
- Support custom dill params.
- Support Nullable patchable config element.
- Add `max_replication_factor` in config.
- Use strawberry ctl address from Cypress client_config.

Fixes:

- Fixes of E721: do not compare types, for exact checks use `is` / `is not`, for instance checks use `isinstance()`.
- Fix bug in {{product-name}} python wrapper: stop transaction pinger before exiting transaction.

{% endcut %}

{% cut "**0.13.17**" %}

Features:

- Support profiles in configuration file.
- Add versioned select.
- Add enum.StrEnum and enum.IntEnum support for yt_dataclasses.

Fixes:

- Fix test_operation_stderr_output in py.test environment.

Thanks to [@thenno](https://github.com/thenno) for considerable contribution!

{% endcut %}

{% cut "**0.13.16**" %}

Features:

- Allow to specify prerequisite transaction ids in client. Transaction context manager (PR: [#638](https://github.com/ytsaurus/ytsaurus/pull/638)). Thanks [@chegoryu](https://github.com/chegoryu) for the PR!
- Add `client` and `chunk_count` parameters to `dirtable_commands`.
- Add `alter_query` command for Query Tracker.
- Add `dump_job_proxy_log` command (PR: [#594](https://github.com/ytsaurus/ytsaurus/pull/594)). Thanks [@tagirhamitov](https://github.com/tagirhamitov) for the PR!

Fixes:

- Fix return result of lock command in case of batch client.
- Fix jupyter notebooks for operations in separate cells (PR: [#654](https://github.com/ytsaurus/ytsaurus/pull/654)). Thanks [@dmi-feo](https://github.com/dmi-feo) for the PR!

{% endcut %}

{% cut "**0.13.15**" %}

Features:

- Add respawn in docker functionality.
- Fixes in conditional imports and dataclasses for type hint support.
- Add option for ignore system py modules while pickling.
- Use pickling by value in interactive envs with dill.
- Add wide time types.
- Add --syntax-version option to client, add composite types member accessors.
- Add retries for skiff.
- Add consumer type handler for node creation.
- Add logic for auto selection of operation layer.
- Added an option for not merging rows in select.
- Add with-monitoring-descriptor option.
- Add missing method in spec builder.
- Add table creation in upload parquet.
- Optimize skiff dump performance for long dataclasses.
- Support parsing type_v1 from yson to schema.
- Add enable_replicated_table_tracker argument to alter_table_replica wrapper methods.
- Add close property to ResponseStream.
- Allow to pass single SortColumn to build_schema_sorted_by.

Fixes:

- Fix typo: comitted, commited -> committed.
- Fix deprecated utcnow.
- Fix ignore of `YT_PREFIX` by certain commands.

{% endcut %}

{% cut "**0.13.14**" %}

Features:

- Added an option for skipping rows merge in `select`.
- Support composite types in QL.
- Add `preserve_account` option to table backup commands.
- Expand the list of dynamic table retriable errors.
- Enhance table creation with specified append attribute.
- Various improvements of maintenance API.
- Support `upload_parquet` command.

Fixes:

- Support `SortColumn` serialization.
- Fix file descriptors leak in config parsing.
- Fix output stream validation for `TypedJobs`.

{% endcut %}

{% cut "**0.13.12**" %}

—

{% endcut %}

{% cut "**0.13.7**" %}

—

{% endcut %}