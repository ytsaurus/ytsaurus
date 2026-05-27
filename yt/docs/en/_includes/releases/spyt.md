## SPYT


Is released as a docker image.




**Releases:**

{% cut "**2.10.0**" %}

**Release date:** 2026-05-27


**Release page:** [2.10.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.10.0)


**Docker image:** [ghcr.io/ytsaurus/spyt:2.10.0](https://github.com/orgs/ytsaurus/packages/container/spyt/765852936?tag=2.10.0)


- Spark 4.0.x and Spark 4.1.x support
- pyspark-client Python package support for writing client-side logic using Spark Connect protocol without JVM
- Transactional Streaming
- Drop Spark 3.2.x support
- Drop Java 11 support, all JVM classes are compiled with Java 17
- Drop Python 3.8, 3.9 and 3.10 support
- Drop Livy support for Query Tracker integration

{% endcut %}

{% cut "**2.9.2**" %}

**Release date:** 2026-05-22

Maintenance release with minor enhancements

- Support runtime filters functionality (dynamic partition pruning) for dataframe API
- Fix stacktrace for disabled metrics
- Move wait_for_spark_connect_endpoint method to spyt.connect
- Propagate nullable in pushStructMetadata during column pruning
- Fix writing nullable values of composite columns in dynamic tables
- Other minor fixes and improvements

{% endcut %}

{% cut "**2.9.1**" %}

**Release date:** 2026-05-08

Maintenance release with minor enhancements

- Boost multiple table locking under transactional reading by using asynchronous lock requests
- Fixed pushdown filters application for Spark SQL API
- Support for specifying custom attributes when creating table
- Added ytPartitioning by YT table compressed size instead of data weight. May improve performance for lookup tables. Disabled by default; enable with `spark.yt.read.ytPartitioning.compressedSize.enable=true`.
- Fixed writing nested unsigned types to dyn tables
- Other minor fixes and improvements

{% endcut %}

{% cut "**2.9.0**" %}

**Release date:** 2026-03-30


**Release page:** [2.9.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.9.0)


**Docker image:** [ghcr.io/ytsaurus/spyt:2.9.0](https://github.com/orgs/ytsaurus/packages/container/spyt/765852936?tag=2.9.0)


- Support for Spark Connect in Spark Standalone inner cluster
- Row and column level security (RLS/CLS) support
- Updated read and write statistics
- Perfomance and stability fixes

{% endcut %}


{% cut "**2.8.2**" %}

**Release date:** 2025-12-23


**Release page:** [2.8.2](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.8.2)


**Docker image:** [ghcr.io/ytsaurus/spyt:2.8.2](https://github.com/orgs/ytsaurus/packages/container/spyt/621174080?tag=2.8.2)


Maintenance release with minor enhancements

- Improving stability of distributed write and read API support
- Other minor fixes

{% endcut %}


{% cut "**2.8.0**" %}

**Release date:** 2025-11-27


**Release page:** [2.8.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.8.0)


**Docker image:** [ghcr.io/ytsaurus/spyt:2.8.0](https://github.com/orgs/ytsaurus/packages/container/spyt/591865107?tag=2.8.0)


- Dynamic allocation support in direct submit scenarios
- YTsaurus distributed read and write API support
- Driver auto-shutdown on executor failures
- Spark connect integration improvements

{% endcut %}


{% cut "**2.7.5**" %}

**Release date:** 2025-11-05


**Release page:** [2.7.5](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.7.5)


**Docker image:** [ghcr.io/ytsaurus/spyt:2.7.5](https://github.com/orgs/ytsaurus/packages/container/spyt/566520656?tag=2.7.5)


Maintenance release with minor enhancements

- Correct reading of unsigned types (uint8, uint16, uint32) in arrow and wire formats

{% endcut %}


{% cut "**2.7.4**" %}

**Release date:** 2025-10-07


**Release page:** [2.7.4](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.7.4)


**Docker image:** [ghcr.io/ytsaurus/spyt:2.7.4](https://github.com/orgs/ytsaurus/packages/container/spyt/536915303?tag=2.7.4)


Maintenance release with minor enhancements

- More reliable processing of streaming offsets

{% endcut %}


{% cut "**2.7.3**" %}

**Release date:** 2025-09-08


**Release page:** [2.7.3](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.7.3)


**Docker image:** [ghcr.io/ytsaurus/spyt:2.7.3](https://github.com/orgs/ytsaurus/packages/container/spyt/508561375?tag=2.7.3)


Maintenance release with minor enhancements

- Refactor shuffle data writing and reading
- Metrics improvements

{% endcut %}


{% cut "**2.7.2**" %}

**Release date:** 2025-09-01


**Release page:** [2.7.2](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.7.2)


**Docker image:** [ghcr.io/ytsaurus/spyt:2.7.2](https://github.com/orgs/ytsaurus/packages/container/spyt/501679957?tag=2.7.2)


Maintenance release with minor enhancements

- Improving YTsaurus shuffle service integration
- Spark connect server wrapper for SPYT


{% endcut %}


{% cut "**2.7.1**" %}

**Release date:** 2025-08-15


**Release page:** [2.7.1](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.7.1)


**Docker image:** [ghcr.io/ytsaurus/spyt:2.7.1](https://github.com/orgs/ytsaurus/packages/container/spyt/487987719?tag=2.7.1)


Maintenance release with minor enhancements

- Optimizing the number of requests to master in bulk reading scenarios
- Hiding sensitive information from driver command line and passing it via secure vault
- Fix executors hostname in network project
- Showing executor operation id in driver operation description
- Fix java properties escaping
- Including parsing context in parsing exceptions
- Fix joins by uint64 columns
- Support for setting secure vault in direct submit scenarios


{% endcut %}


{% cut "**2.7.0**" %}

**Release date:** 2025-07-24


**Release page:** [2.7.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.7.0)


**Docker image:** [ghcr.io/ytsaurus/spyt:2.7.0](https://github.com/orgs/ytsaurus/packages/container/spyt/469733902?tag=2.7.0)


- YTsaurus shuffle service support
- Metrics refactoring for inner cluster and direct submit modes
- Dynamic table queries via SQL API doesn't require explicit timestamp
- Bugs and stability fixes:
- - Fix OutOfMemory errors for optimized-for scan sorted tables
- - Fix casting types to uint64 in codegen
- - Fix "Manually specified and authenticated users mismatch" YT error in direct submit
- - Other minor fixes

{% endcut %}


{% cut "**2.6.5**" %}

**Release date:** 2025-06-08


**Release page:** [2.6.5](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.6.5)


**Docker image:** [ghcr.io/ytsaurus/spyt:2.6.5](https://github.com/orgs/ytsaurus/packages/container/spyt/433480410?tag=2.6.5)


Maintenance release with minor enhancements

- Support for Spark 3.5.6
- Minor improvements for Spark Streaming support in YTsaurus


{% endcut %}


{% cut "**2.6.4**" %}

**Release date:** 2025-05-16


**Release page:** [2.6.4](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.6.4)


**Docker image:** [ghcr.io/ytsaurus/spyt:2.6.4](https://github.com/orgs/ytsaurus/packages/container/spyt/417318819?tag=2.6.4)


Maintenance release with minor enhancements and bug fixes

- Support for retrieving driver operation id in direct submit scenarios
- Reducing YTsaurusClient threads by reusing client instances
- Fix JSON layout for log4j2
- Transaction titles for SPYT transactions
- Fix prometeus metrics configuration
- Fix dedicated driver mode for standalone cluster


{% endcut %}


{% cut "**2.6.0**" %}

**Release date:** 2025-04-23


**Release page:** [2.6.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.6.0)


**Docker image:** [ghcr.io/ytsaurus/spyt:2.6.0](https://github.com/orgs/ytsaurus/packages/container/spyt/401104738?tag=2.6.0)


- Java 17 support
- Support for UUID and Json YTsaurus types
- Support for RPC-job proxy in direct submit
- Support for additional task parameters in YTsaurus operation specification via Spark config in direct submit
- Support for taking snapshots locks at reading time
- Explicit flag for truncated result of Query Tracker queries
- Fix compatibility with Spark 3.5.4 and 3.5.5
- Fix for date- and timestamp SQL-functions via Query Tracker
- Many stability and other bug fixes

{% endcut %}


{% cut "**2.5.0**" %}

**Release date:** 2024-12-25


**Release page:** [2.5.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.5.0)


Major release that enables support for Spark 3.4.x and 3.5.x.

- Compile-time Spark version is changed from 3.2.2 to 3.5.4;
- SPYT compile-time Spark version will be the latest available supported version since this release;
- Backward compatibility is still preserved down to Spark 3.2.2;
- Unit tests can be run over different Spark version than used at compile time via `-DtestSparkVersion=3.x.x` sbt flag


{% endcut %}


{% cut "**2.4.4**" %}

**Release date:** 2024-12-20


**Release page:** [2.4.4](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.4.4)


Maintenance release with bug fixes:

- Providing network project for Livy via command line argument


{% endcut %}


{% cut "**2.4.3**" %}

**Release date:** 2024-12-16


**Release page:** [2.4.3](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.4.3)


Maintenance release with bug fixes:

- Specifying network project for direct submit and setting it from Livy
- Fix read and write for structs with float value using Dataset API

{% endcut %}


{% cut "**2.4.2**" %}

**Release date:** 2024-12-06


**Release page:** [2.4.2](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.4.2)


Maintenance release with bug fixes:

- Autocast DatetimeType to TimestampType in spark udf
- Add parsing spark.executorEnv and spark.ytsaurus.driverEnv and set SPARK_LOCAL_DIRS
- Fix worker_disk_limit and worker_disk_account parameters for standalone cluster
- Using compatible SPYT versions instead of latest for direct submit
- Separate proxy role into client (spark.hadoop.yt.proxyRole) and cluster (spark.hadoop.yt.clusterProxyRole)
- Add flag spark.ytsaurus.driver.watch for watching driver operation
- Fix reading Livy logs

{% endcut %}


{% cut "**2.4.1**" %}

**Release date:** 2024-11-12


**Release page:** [2.4.1](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.4.1)


Maintenance release with bug fixes:

- Fix creating tables via Spark SQL without explicitly specifying ytTable schema
- Fix serializing and deserializing nested time types
- Fix casting NULL in nested data structures

{% endcut %}


{% cut "**2.4.0**" %}

**Release date:** 2024-10-31


**Release page:** [2.4.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.4.0)


* Support for running local files and their dependencies in direct submit mode by uploading it to YTsaurus cache
* Support for submitting compiled python binaries as spark applications via direct submit
* Dataframe write schema hints
* Bug fixes:
* * Writing to external S3 from YTsaurus
* * Reading float values from nested structures
* * Columnar format reading for Spark 3.3.x
* * Reading arbitrary files from Cypress when using Spark 3.3.x

{% endcut %}


{% cut "**2.3.0**" %}

**Release date:** 2024-09-11


**Release page:** [2.3.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.3.0)


The major feature of SPYT 2.3.0 is support for Spark 3.3.x. Other notable features are:

* Support for extended Datetime types such as Date32, Datetime32, Timestamp64, Interval64;
* Support for table properties in Spark SQL;
* Support for writing using Hive partitioning schema;
* Support for specifying random port for Shuffle service in inner standalone cluster;
* Fix for runtime statistics;
* Bug-Fixes for user-provided schema and for dataframes persisting.

{% endcut %}


{% cut "**2.2.0**" %}

**Release date:** 2024-08-14


**Release page:** [2.2.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.2.0)


- Support for reading from multiple YTsaurus clusters
- Supplying annotations for YTsaurus operations via conf parameters
- Support for specifying custom schema on read
- Support for --archives parameter in spark-submit
- Fix for int8 and int16 as nested fields
- Transactional read fix
- Other minor fixes

{% endcut %}


{% cut "**2.1.0**" %}

**Release date:** 2024-06-19


**Release page:** [2.1.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.1.0)


* Support for running applications using GPU
* Support for Spark versions 3.2.2-3.2.4
* History server support for direct submit scenarios
* Support for https and TCP proxy in direct submit scenarios
* Other minor fixes and improvements


{% endcut %}


{% cut "**2.0.0**" %}

**Release date:** 2024-05-29


**Release page:** [2.0.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.0.0)


SPYT 2.0.0 is the first release under the new release scheme and in the separate ytsaurus-spyt repository. The main feature of this release is that we have finally switched from Apache Spark fork that was used in previous releases to original Apache Spark distributive. The 2.0.0 SPYT release is still using Apache Spark 3.2.2, but we plan to support all Apache Spark 3.x.x releases in the nearest future!

Other notable changes are:
- Support for direct submit on using Livy via Query Tracker;
- Split data-source module into data-source-base that uses standard Spark types for all YTsaurus types, and data-source-extended for our implementation of custom YTsaurus types that don't have direct matches in Spark type system;
- Support for direct submit from Jupyter notebooks;
- Custom UDT for YTsaurus datetime type.

{% endcut %}

