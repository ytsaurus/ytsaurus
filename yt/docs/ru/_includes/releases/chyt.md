## CHYT


Is released as a docker image.




**Releases:**

{% cut "**2.18.0**" %}

**Release date:** 2026-01-20


#### Features:
- Support RLS in CHYT, [3fe297c](https://github.com/ytsaurus/ytsaurus/commit/3fe297cd8ffc38e019c0121126ceaf5f636166ef).
- Add read range inference from perdicate, [3a9eb82](https://github.com/ytsaurus/ytsaurus/commit/3a9eb82c7ec5495632f13dc3e8884a158312de4d).
- Add chyt\_query\_statistics for insert queries and failed queries, [974b2c2](https://github.com/ytsaurus/ytsaurus/commit/974b2c28d5dcf44316516e285596b9c13090ee71), [99a08a3](https://github.com/ytsaurus/ytsaurus/commit/99a08a3449806035c48dd74ee587486569b7a6e1).
- Add output table to runtime variables, [05cd02a](https://github.com/ytsaurus/ytsaurus/commit/05cd02ade583eaacd9ecf13cd8e27689e4bceefb).
- Add feature to distribute input specs for secondary queries in pull mode, [c4ba9c4](https://github.com/ytsaurus/ytsaurus/commit/c4ba9c46ffd8522946ef34fe505b61805a21f504).
- Add optimization for better performance with min/max queries using columnar statistics from nodes, [d83cd46](https://github.com/ytsaurus/ytsaurus/commit/d83cd46c0b5830fbbcdd23c00106b387b439542b).
- Add optimization for better performance with rle and dictionary encoding (using only distinct values without materialization), [c777591](https://github.com/ytsaurus/ytsaurus/commit/c77759151d82bbfc8720cc269a3c295555974bdf).
- Add revision check to AttributeCache for more stable performance, [3f3be85](https://github.com/ytsaurus/ytsaurus/commit/3f3be8596e05dd30054e7ddc7bfb15421ffd9afc).
- Add EnableComlexOptionalConversion to prevent conversion of arrays (can't be nullable in Clickhouse, will change default to false in future), [57a298a](https://github.com/ytsaurus/ytsaurus/commit/57a298ac4c98659fe014d43cea872925d9750424).
- Add parallel insert-select support in storage\_distributor, [4632de8](https://github.com/ytsaurus/ytsaurus/commit/4632de8546d04ef8f3dedb9b9a26007a5384288b).

#### Fixes:
- Fix usage of master chunk spec fetcher for ordered dynamic tables. Applicable for CHYT over YT server versions up to 24.2, inclusive, [d3df92f](https://github.com/ytsaurus/ytsaurus/commit/d3df92fd4fa2756f397d329e79243be008512311).
- Fix CTE errors in LEFT JOIN with IN condition, [b51e5db](https://github.com/ytsaurus/ytsaurus/commit/b51e5db56c9867a4b6615e24d791b59cef7becab).
- Track total progress on coordinator, [0d5fc5c](https://github.com/ytsaurus/ytsaurus/commit/0d5fc5ce2b0f1c65922627f6d7dfb8bc7d215dd5).
- Fix multithreaded write to secondaryProgress from different pipes, [8db79f4](https://github.com/ytsaurus/ytsaurus/commit/8db79f457cf71f0e00b8f65bd079aa03aaa9ad52).

{% endcut %}


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

