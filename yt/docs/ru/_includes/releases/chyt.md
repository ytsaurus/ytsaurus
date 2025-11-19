## CHYT


Is released as a docker image.




**Releases:**

{% cut "**2.17.4**" %}

**Release date:** 2025-09-23


- Backport YT-25206: Set up Cypress Transaction Service on Cypress Proxies (Commit: eb104f198aeb5bd30208e0214c03fd50f0535655)
- Backport YT TLS support (Commit: fde3ac361bd81d5c8df21e3bcc13c9710cb446a8)
- Fix usage of CTE in distributed queries (Commit: e275fa81599ff28fbb6a41de4c7c6c9fee0417fd)
- Add aklomp-base64 support for base64 functions (Commit: 5708583fcc58051627bfcd4f9849de6f7915afcf)
- Add usage of new analyzer to ytTables function (Commit: 29a8f6cefa043b8365949e2f6e54aadf40434c6b)

{% endcut %}


{% cut "**2.17.2**" %}

**Release date:** 2025-07-04


No description

{% endcut %}


{% cut "**2.16.0**" %}

**Release date:** 2024-11-06


- Support ClickHouse query cache (may be configured via `clickhouse_config`)
- Read in order optimization (PR #757)
- New PREWHERE algorithm on a data conversion level, turned on by default
- Convert `bool` data type to `Bool` instread of `YtBoolean`. `YtBoolean` type is deprecated
- Convert `dict` data type to `Map` instead of `Array(Typle(Key, Value))`
- Convert `timestamp` data type to `DateTime64` instead of `UInt64`
- Support reading and writing `date32`, `datetime64`, `timestamp64`, `interval64` data types
- Support reading `json` data type as `String`
- Support JSON_* functions from ClickHouse
- The ability to specify a cypress directory as a database
- Support exporting system log tables to cypress (query_log, metric_log, etc)

**Note**: `date32`, `datetime64`, `timestamp64` and `interval64` were introduced in YTsaurus 24.1. If the YTsaurus cluster version is older, trying to store these data types in a table will lead to a `not a valid type` error.

{% endcut %}


{% cut "**2.14.0**" %}

**Release date:** 2024-02-15


- Support SQL UDFs
- Support reading dynamic and static tables via concat-functions

{% endcut %}


{% cut "**2.13.0**" %}

**Release date:** 2024-01-19


- Update ClickHouse code version to the latest LTS release (22.8 -> 23.8)
- Support for reading and writing ordered dynamic tables
- Move dumping query registry debug information to a separate thread
- Configure temporary data storage

{% endcut %}

