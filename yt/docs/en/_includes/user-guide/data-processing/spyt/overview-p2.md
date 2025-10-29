
## Compatibility of SPYT versions with Apache Spark{ #spyt-compatibility }, Java, Scala, and Python

#|
|| **SPYT version** | **Spark version** | **Java** | **Scala** | **Python** ||
|| 1.x.x, 2.0.x | 3.2.2 | 11 | 2.12 | 3.8, 3.9, 3.11, 3.12 ||
|| 2.1.x, 2.2.x | 3.2.2 - 3.2.4 | 11 | 2.12 | 3.8, 3.9, 3.11, 3.12 ||
|| 2.3.x, 2.4.x | 3.2.2 - 3.3.4 | 11 | 2.12 | 3.8, 3.9, 3.11, 3.12 ||
|| 2.5.0 | 3.2.2 - 3.5.3 | 11 | 2.12 | 3.8, 3.9, 3.11, 3.12 ||
|| 2.6.x, 2.7.x | 3.2.2 - 3.5.7 | 11, 17 | 2.12 | 3.8, 3.9, 3.11, 3.12 ||
|#


## When to use SPYT { #what-to-do }

SPYT is optimal in the following cases:
- Developing in Java and using MapReduce in {{product-name}}.
- Optimizing pipeline performance on {{product-name}} with two or more joins or groupings.
- Writing integrational ETL pipelines from other storage systems.
- Ad-hoc analytics in interactive mode using `Jupyter`, `pyspark`, `spark-shell`, or the [Query Tracker](../../../../user-guide/query-tracker/about.md) component built into the UI.

Do not use SPYT if:
- You need to process over 10 TB of data in a single transaction.
- Your processing boils down to individual Map or MapReduce operations.

## Ways to run Spark calculations in {{product-name}} { #submit }

- Submitting directly to {{product-name}} using the `spark-submit` command. [Learn more](../../../../user-guide/data-processing/spyt/launch.md#submit).
- Creating a Standalone Spark cluster as a persistent resource within {{product-name}} using a Vanilla operation. [Learn more](../../../../user-guide/data-processing/spyt/launch.md#standalone).

## Supported programming languages { #lang }

Spark supports the following languages and development environments:

* [Jupyter](../../../../user-guide/data-processing/spyt/API/spyt-jupyter.md).
* [Python](../../../../user-guide/data-processing/spyt/API/spyt-python.md).
* [Java](../../../../user-guide/data-processing/spyt/API/spyt-java.md).
* [Scala](../../../../user-guide/data-processing/spyt/API/spyt-scala.md).

