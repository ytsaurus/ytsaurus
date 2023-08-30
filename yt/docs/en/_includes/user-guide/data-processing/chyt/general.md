# Introduction

This article provides a rationale for the selected ClickHouse model within the {{product-name}} ecosystem. In particular, why CHYT has *cliques*, while regular ClickHouse has nothing analogous.

## What is a clique? { #what-is }

A *clique* is a full-fledged ClickHouse cluster that can read data from tables located in {{product-name}} via the built-in {{product-name}} internal protocol.
From the point of view of the [{{product-name}} scheduler](../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md), a clique is a Vanilla operation containing in a typical case up to dozens of jobs, each of which consumes up to 16 CPU cores by default. The name *clique* was chosen as a play on words in consonance with the name ClickHouse, but the correct spelling in English is *Clique*.

A fundamental property of CHYT is that a clique is "administered" by *users*, not {{product-name}} administrators (for more information, see [Administering a private clique](../../../../user-guide/data-processing/chyt/cliques/administration.md)). Specifically, any {{product-name}} user can start their own clique (or even several cliques) at any time.


## Reliable isolation { #isolation }

This representation solves the problem of *isolating* the load from various users. OLAP databases like ClickHouse can have a fundamentally diverse load. We can mention production processes like ETL which create a constant background of load from under robotic users. Besides that, there are one-time ad-hoc queries from human users through tools like YQL, as well as one-time or regular uploads of datasets to external BI systems, such as [DataLens](https://cloud.yandex.ru/docs/datalens/) or [Tableau](https://www.tableau.com/), for further visualization.

If the database faces many "heavy" queries made at the same time, this can overload the cluster in many different ways:
- The compute nodes may run out of CPU resources, causing all queries (including fast ones under normal circumstances) to get less CPU time and randomly degrade.
- As a consequence, the IO subsystem or the network stack (for example, a TCP or DNS subsystem) which also need CPU time to function may degrade.
- Compute nodes can bump into the network bandwidth and become [IO-bound](https://en.wikipedia.org/wiki/I/O_bound).
- Finally, processing substantial amounts of data can consume a lot of RAM, both directly for computations that require maintaining large intermediate states (such as heavy `GROUP BY`, sorting, or `COUNT DISTINCT`) and elementarily for reading data for buffers over the network.

Despite the fact that ClickHouse has built-in individual protection mechanisms to solve the problems described above, they do not always help in the realities of {{product-name}}. A key property of {{product-name}} as a data source for ClickHouse is that any even somewhat large table is "very heavily distributed". Roughly speaking, you can assume that every next GB of data in the table will live on a new host. In addition, [static {{product-name}} tables](../../../../user-guide/storage/static-tables.md), and especially those storing analytical data, can often be compressed with a heavy compression codec (see [Compression](../../../../user-guide/storage/compression.md)).

This leads to the fact that processing even a "relatively light" table of a dozen GB with only a few narrow columns with a total weight of hundreds of MB is heavily dependent on the network stack and the data decompression subsystem and is quite demanding on RAM for buffers to read from each machine over the network.

Not wanting to solve the difficult task of managing CPU, RAM, network and disk IO bandwidth resources within ClickHouse, the decision was made to separate users physically between different processes, which is just achieved by having the required number of cliques used by small groups of users and independent of each other. Cliques are expected to be small-granular enough not to serve different types of load or a large number of users at the same time.

The only exception to this rule are [public cliques](../../../../user-guide/data-processing/chyt/try-chyt.md#public) which are intentionally open to all, but are provided on the best effort principle: they may be unavailable or overloaded at any time, although their performance is monitored. There are restrictions on the complexity of executed queries, sometimes quite severe. Consider public cliques to be a demo version of CHYT.

## CHYT as a service: reuse of resources in {{product-name}} and exploitation by users { #as-a-service }

This system of distributing resources between different cliques within one large {{product-name}} cluster resembles ClickHouse "cloud" hosting similar to [Managed ClickHouse](https://cloud.yandex.ru/services/managed-clickhouse) in a cloud provider. The only difference is that users do not pay with rubles in the external cloud, but with a computing quota in {{product-name}}. This enables many analytical and development teams to flexibly manage existing computing resources in {{product-name}}. Some of them can be used in "classic" computational pipelines for minutes/hours/days with Map-Reduce, for example, via YQL over {{product-name}}. And the other small part can be used to create a clique of a suitable size for fast ad-hoc computations with delays of seconds.

Besides that, as is always the case with cloud technologies, providing something on an "as a service" model enables you to clearly specify the boundaries of responsibility for the operation of certain technical solutions.

The greatest success in the operation of any database is achieved when two conditions are met simultaneously:

- The user has a good understanding of the database structure and is able to evaluate the bottlenecks of the future computational pipeline using it.
- The user is well aware of the specifics of the stored data, as well as the necessary computations on them, also knows where the data arises from in the database, and is responsible for its specific representation and form of storage.

Users are expected to use the documentation to solve the majority of operational problems, including diagnosing slow queries and intelligently evaluating the need for resources.
