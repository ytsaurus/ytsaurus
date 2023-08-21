# Overview

## What is Spark? { #what-is-spark }

[Apache Spark](https://spark.apache.org/) is an engine for computations using big data (joins, groups, filters, ets.).

Spark uses RAM to process its data. The key difference between computing in memory and the "conventional" 2005 MapReduce model is that the data has minimum disk impact, which minimizes I/O costs as the slowest part of computing. For a single Map transaction, the effect from using Spark will be negligible. However, even a single Map and Reduce sequence saves on writing intermediate results out to disk provided there is enough memory.

For each subsequent MapReduce sequence the efficiencies mount, and you can cache the output. For large and complex analytics pipelines, efficiency will increase many fold.

Spark is also equipped by a full-scale [Catalyst](https://github.com/tupol/spark-catalyst-study/blob/master/docs/catalyst-description.md) query optimizer that plans execution and takes into consideration:
- Input data location and size.
- Predicate pushdown to the file system.
- Expediency and step sequence in query execution.
- Collection of final table attributes.
- Use of local data for processing.
- Potential for computation pipelining.


