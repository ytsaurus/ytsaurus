## SPYT


Is released as a docker image.




**Releases:**

{% cut "**2.5.0**" %}

**Release date:** 2024-12-25


Major release that enables support for Spark 3.4.x and 3.5.x. 

- Compile-time Spark version is changed from 3.2.2 to 3.5.4;
- SPYT compile-time Spark version will be the latest available supported version since this release;
- Backward compatibility is still preserved down to Spark 3.2.2;
- Unit tests can be run over different Spark version than used at compile time via `-DtestSparkVersion=3.x.x` sbt flag


{% endcut %}


{% cut "**2.4.4**" %}

**Release date:** 2024-12-20


Maintenance release with bug fixes:

- Providing network project for Livy via command line argument


{% endcut %}


{% cut "**2.4.3**" %}

**Release date:** 2024-12-16


Maintenance release with bug fixes:

- Specifying network project for direct submit and setting it from Livy
- Fix read and write for structs with float value using Dataset API

{% endcut %}


{% cut "**2.4.2**" %}

**Release date:** 2024-12-06


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


Maintenance release with bug fixes:

- Fix creating tables via Spark SQL without explicitly specifying ytTable schema
- Fix serializing and deserializing nested time types
- Fix casting NULL in nested data structures

{% endcut %}


{% cut "**2.4.0**" %}

**Release date:** 2024-10-31


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


* Support for running applications using GPU
* Support for Spark versions 3.2.2-3.2.4
* History server support for direct submit scenarios
* Support for https and TCP proxy in direct submit scenarios
* Other minor fixes and improvements


{% endcut %}


{% cut "**2.0.0**" %}

**Release date:** 2024-05-29


SPYT 2.0.0 is the first release under the new release scheme and in the separate ytsaurus-spyt repository. The main feature of this release is that we have finally switched from Apache Spark fork that was used in previous releases to original Apache Spark distributive. The 2.0.0 SPYT release is still using Apache Spark 3.2.2, but we plan to support all Apache Spark 3.x.x releases in the nearest future!

Other notable changes are:
- Support for direct submit on using Livy via Query Tracker;
- Split data-source module into data-source-base that uses standard Spark types for all YTsaurus types, and data-source-extended for our implementation of custom YTsaurus types that don't have direct matches in Spark type system;
- Support for direct submit from Jupyter notebooks;
- Custom UDT for YTsaurus datetime type.

{% endcut %}

