## SPYT

Is released as a docker image.

**Current release:** {{spyt-version}} (`ghcr.io/ytsaurus/spyt:{{spyt-version}}`)

**All releases:**

{% cut "**2.4.1**" %}

Maintenance release with bug fixes:

- Fix creating tables via Spark SQL without explicitly specifying ytTable schema
- Fix serializing and deserializing nested time types
- Fix casting NULL in nested data structures

{% endcut %}

{% cut "**2.4.0**" %}

- Support for running local files and their dependencies in direct submit mode by uploading it to YTsaurus cache
- Support for submitting compiled python binaries as spark applications via direct submit
- Dataframe write schema hints 
- Bug fixes:
- - Writing to external S3 from YTsaurus
- - Reading float values from nested structures
- - Columnar format reading for Spark 3.3.x
- - Reading arbitrary files from Cypress when using Spark 3.3.x

{% endcut %}


{% cut "**2.3.0**" %}

The major feature of SPYT 2.3.0 is support for Spark 3.3.x. Other notable features are:

- Support for extended Datetime types such as Date32, Datetime32, Timestamp64, Interval64
- Support for table properties in Spark SQL
- Support for writing using Hive partitioning schema
- Support for specifying random port for Shuffle service in inner standalone cluster
- Fix for runtime statistics
- Bug-Fixes for user-provided schema and for dataframes persisting

{% endcut %}

{% cut "**2.2.0**" %}

- Support for reading from multiple {{product-name}} clusters.
- Supplying annotations for {{product-name}} operations via conf parameters.
- Support for specifying custom schema on read.
- Support for `--archives` parameter in `spark-submit`.
- Fix for int8 and int16 as nested fields.
- Transactional read fix.
- Other minor fixes.

{% endcut %}

{% cut "**2.1.0**" %}

- Support for running applications using GPU
- Support for Spark versions 3.2.2-3.2.4
- History server support for direct submit scenarios
- Support for https and TCP proxy in direct submit scenarios
- Other minor fixes and improvements

{% endcut %}

{% cut "**2.0.0**" %}

SPYT 2.0.0 is the first release under the new release scheme and in the separate ytsaurus-spyt repository. The main feature of this release is that we have finally switched from Apache Spark fork that was used in previous releases to original Apache Spark distributive. The 2.0.0 SPYT release is still using Apache Spark 3.2.2, but we plan to support all Apache Spark 3.x.x releases in the nearest future!

Other notable changes are:

- Support for direct submit on using Livy via Query Tracker;
- Split data-source module into data-source-base that uses standard Spark types for all {{product-name}} types, and data-source-extended for our implementation of custom {{product-name}} types that don't have direct matches in Spark type system;
- Support for direct submit from Jupyter notebooks;
- Custom UDT for {{product-name}} datetime type.

{% endcut %}

{% cut "**1.78.0**" %}

- Reverting Spark 3.2.2 fork to its original state
- Support for specifying network name when using direct submit
- Writing all python driver output to stderr when using direct submit
- Several bug fixes

{% endcut %}

{% cut "**1.77.0**" %}

- Support for Spark Streaming using ordered dynamic tables;
- Support for CREATE TABLE AS, DROP TABLE and INSERT operations;
- Session reuse for QT SPYT engine;
- SPYT compilation using vanilla Spark 3.2.2;
- Minor perfomance optimizations

{% endcut %}

{% cut "**1.76.1**" %}

- Fix IPV6 for submitting jobs in cluster mode;
- Fix Livy configuration;
- Support for reading ordered dynamic tables.

{% endcut %}

{% cut "**1.76.0**" %}

- Support for submitting Spark tasks to {{product-name}} via spark-submit;
- Shrinking SPYT distributive size up to 3 times by separating SPYT and Spark dependencies;
- Fix reading nodes with a lot (>32) of dynamic tables ([Issue #240](https://github.com/ytsaurus/ytsaurus/issues/240));
- Assembling sorted table from parts uses concatenate operation instead of merge ([Issue #133](https://github.com/ytsaurus/ytsaurus/issues/133)).

{% endcut %}

{% cut "**1.75.4**" %}

- Fix backward compatibility for ytsaurus-spyt
- Optimizations for count action
- Include livy in SPYT deploying pipeline
- Update default configs

{% endcut %}

{% cut "**1.75.3**" %}

- Added random port attaching for Livy server.
- Disabled {{product-name}} operation stderr tables by default.
- Fix nested schema pruning bug.

{% endcut %}

{% cut "**1.75.2**" %}

- More configurable TCP proxies support: new options --tcp-proxy-range-start and --tcp-proxy-range-size.
- Added aliases for type v3 enabling options: spark.yt.read.typeV3.enabled and spark.yt.write.typeV3.enabled.
- Added option for disabling tmpfs: --disable-tmpfs.
- Fixed minor bugs.

{% endcut %}

{% cut "**1.75.1**" %}

- Extracting {{product-name}} file system bundle outside Spark Fork
- Fix reading arrow tables from Spark SQL engine
- Binding Spark standalone cluster Master and Worker RPC/REST endpoints to wildcard network interface
- Add configurable thread pool size of internal RPC Job proxy

{% endcut %}