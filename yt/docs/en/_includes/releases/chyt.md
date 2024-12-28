## CHYT


Is released as a docker image.




**Releases:**

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

