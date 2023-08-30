
## Languages to code in { #lang }

In Spark, you can code in one of three languages: Python, Java, and Scala.

## When to use SPYT { #what-to-do }

Pick SPYT if you are:
- Developing in Java and using MapReduce in {{product-name}}.
- Optimizing pipeline performance on {{product-name}} with two or more joins or groupings.

Do not use SPYT if:
- You need to process over 10 TB of data in a single transaction.
- Your processing boils down to individual Map or MapReduce operations.

## Getting access to SPYT { #access }

1. Start a Spark cluster.
2. Take advantage of the Spark cluster using several methods:
   * Code in [Jupyter](../../../../user-guide/data-processing/spyt/API/spyt-jupyter.md).
   * Code in [Python](../../../../user-guide/data-processing/spyt/API/spyt-python.md) and run the code on your cluster.
   * Code in [Java](../../../../user-guide/data-processing/spyt/API/spyt-java.md) and run the code on your cluster.
   * Code in [Scala](../../../../user-guide/data-processing/spyt/API/spyt-scala.md) and run the code on your cluster.


