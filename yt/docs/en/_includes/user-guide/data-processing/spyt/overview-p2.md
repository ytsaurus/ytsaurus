
# SPYT compatibility with Apache Spark versions { #spyt-compatibility }

#| 
|| **SPYT version** | **Apache Spark version** ||
|| 1.x.x, 2.0.x | 3.2.2 ||
|| 2.1.x, 2.2.x | 3.2.2 - 3.2.4 ||
|| 2.3.x, 2.4.x | 3.2.2 - 3.3.4 ||
|| 2.5.x | 3.2.2 - 3.5.x ||
|#

## When to use SPYT { #what-to-do }

SPYT is an optimal choice for:
- Developing in Java and using MapReduce in {{product-name}}.
- Optimizing pipeline performance on {{product-name}} with two or more joins or groupings.
- Writing integrational ETL pipelines from other storage systems.
- Ad-hoc analytics in interactive mode using `Jupyter`, `pyspark` or `spark-shell`.

Do not use SPYT if:
- You need to process over 10 TB of data in a single transaction.
- Your processing boils down to individual Map or MapReduce operations.

## Submitting Spark applications to {{product-name}} { #submit }

- Submitting directly to {{product-name}} using `spark-submit` command [Details](../../../../user-guide/data-processing/spyt/launch.md#submit).
- Launching an inner standalone Spark cluster inside {{product-name}} using Vanilla operation [Details](../../../../user-guide/data-processing/spyt/launch.md#standalone).

## Languages to code in { #lang }

Spark supports following programming languages and environments:

* [Jupyter](../../../../user-guide/data-processing/spyt/API/spyt-jupyter.md)
* [Python](../../../../user-guide/data-processing/spyt/API/spyt-python.md)
* [Java](../../../../user-guide/data-processing/spyt/API/spyt-java.md)
* [Scala](../../../../user-guide/data-processing/spyt/API/spyt-scala.md)


