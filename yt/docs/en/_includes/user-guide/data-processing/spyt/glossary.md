# Definitions in SPYT

SPYT sits at the intersection of two systems:

- Apache Spark as a compute engine;
- {{product-name}} as a data storage system, resource manager, and process launcher.

Because of this, the same terms used in documentation and discussions may refer to different entities depending on the context. For example, the terms “cluster”, “job”, “operation”, “partition”, “table”, etc. are used differently in Spark and {{product-name}}.

The goal of this section is to provide a unified set of definitions and help distinguish overlapping concepts in Spark, SPYT, and {{product-name}}.

## SPYT overview and launch modes

### SPYT overview

**SPYT** (Spark over {{product-name}}) is the integration of Apache Spark with the {{product-name}} infrastructure.

SPYT lets you run Spark applications on data stored in {{product-name}} and use {{product-name}} as an environment for resource allocation, data storage, logging, and storing service information.

SPYT is not a separate compute engine: Spark performs the computations, while {{product-name}} provides the infrastructure layer.

### SPYT cluster { #spyt-cluster }

**SPYT cluster** is a pre‑launched Spark Standalone cluster inside {{product-name}}.


Such a cluster consists of a Spark Master, one or more Spark Workers, and usually a Spark History Server. In {{product-name}}, it is launched as one or more [Vanilla operations](#vanilla).


It’s important not to confuse an SPYT cluster with a {{product-name}} cluster:

- **{{product-name}} cluster** is the entire compute cluster;
- **SPYT cluster** is a Spark Standalone cluster deployed inside a {{product-name}} cluster.

### Direct Submit { #direct-submit }

**Direct Submit** is a mode for launching a Spark application without a pre‑launched SPYT cluster.

In this mode, separate Spark Master and Spark Worker instances are not used. The driver is launched directly in {{product-name}}, and executors are allocated on demand. From an architectural perspective, the {{product-name}} scheduler largely takes on the role of resource management.

Direct Submit is contrasted with the standalone cluster mode, where the application connects to an already running SPYT cluster.

## Spark components in the context of {{product-name}}

### Spark Master { #spark-master }

**Spark Master** is a component of a Spark Standalone cluster that manages workers and allocates resources among Spark applications.

In SPYT, Spark Master exists only in standalone cluster mode. It is launched inside {{product-name}} as part of an SPYT cluster and publishes information about itself in [discovery_path](#discovery-path).


There is no Spark Master in Direct Submit mode.

### Spark Worker { #spark-worker }

**Spark Worker** is a component of a Spark Standalone cluster that manages the resources of a specific node and launches executors when instructed by the Spark Master.

Spark Worker exists only in an SPYT cluster. Workers are absent in Direct Submit mode.

It’s important not to confuse Spark Worker and Executor:

- **Worker** is part of the Spark cluster that manages node resources;
- **Executor** is a process of a specific Spark application that performs computations.

### Spark Driver { #spark-driver }

**Spark Driver** is the coordinating process of a Spark application.

The driver builds an execution plan, breaks down computations into stages and tasks, requests resources, sends tasks to executors, and collects results.

In SPYT, the driver can be launched:

- as a separate process inside {{product-name}};
- locally on a client machine if client mode is used.

It’s important not to confuse the driver and [Spark Master](#spark-master):

- **Spark Master** manages the cluster;
- **Spark Driver** manages one specific Spark application.


### Executor { #executor }

**Executor** is a Spark application process that directly performs tasks on data.

Executors are launched upon request from the driver and operate within a single Spark application. They read data, perform computations, participate in shuffle, and can store cached data.

In SPYT, executors are launched inside {{product-name}} and use resources allocated by {{product-name}}.

It’s important not to confuse an executor with [Spark Worker](#spark-worker):

- **Worker** provides resources;
- **Executor** consumes those resources to perform application tasks.

### Spark History Server { #shs }


**Spark History Server** (SHS) is a service for viewing logs of completed Spark applications.

It lets you analyze completed applications, their stages, tasks, resource consumption, and errors after they finish running.

In SPYT, SHS is usually launched as part of an SPYT cluster. Its data source is the event logs of Spark applications.


## Data — {{product-name}} tables vs Spark abstractions { #data }


### Spark Partition and {{product-name}} Chunk/Partition { #partitions }


In the context of SPYT, the word “partition” can refer to different entities, so it’s important to distinguish at least three concepts: **Spark Partition**, **{{product-name}} Chunk**, and **{{product-name}} Partition**.


**Spark Partition** is a unit of parallelism in Spark.

One Spark task usually processes one Spark partition. Spark Partition determines how data is distributed across tasks and define the parallelism level for reads and computations.


**{{product-name}} Chunk** is a unit of physical data storage in {{product-name}}.

A chunk is a low‑level storage and replication object. A {{product-name}} table physically consists of chunks, and reading data in batch scenarios ultimately relies on them.

**{{product-name}} Partition** is a logical data partitioning in {{product-name}}, depending on the table type and processing scenario.

When reading {{product-name}} tables via SPYT, these entities do not directly correspond:

- **chunk** refers to the physical storage of data in {{product-name}};
- **{{product-name}} partition** refers to the logical or internal data partitioning in {{product-name}};
- **Spark partition** refers to computation execution in Spark.


### DataFrame { #dataframe }

**DataFrame** is the main structured data abstraction in Spark SQL.

A DataFrame is a collection of rows with named columns and a schema. In SPYT, a DataFrame is usually created by reading {{product-name}} tables.

It’s important not to confuse a DataFrame and a {{product-name}} table:

- **{{product-name}} table** is a data storage object in {{product-name}};
- **DataFrame** is a data representation inside a Spark application.


## Infrastructure and integration { #infra }

### discovery_path { #discovery-path }

**`discovery_path`** is a path in {{product-name}} Cypress that Spark applications use to find a running SPYT cluster.

In cluster mode, the Spark Master publishes service information in this path: service addresses, connection parameters, and other cluster metadata.

`discovery_path` is a concept specific to SPYT. Standard Spark does not have such a mechanism.

### Vanilla operation in {{product-name}} { #vanilla }


**Vanilla operation in {{product-name}}** is a universal type of {{product-name}} operation for launching arbitrary user processes.

In SPYT, Vanilla operations are used to launch Spark infrastructure components:


- `Spark Master`;
- `Spark Worker`;
- `Spark Driver`;
- `Executor`;
- `Spark History Server`.

### Shuffle { #shuffle }

**Shuffle** is a mechanism for redistributing data among executors in Spark.

Shuffle occurs when data needs to be regrouped by key or redistributed among nodes for further computations. Typical examples include `join`, `groupBy`, `distinct`, sorts, and some window operations.


Shuffle is one of the most expensive operations in Spark because it involves network communication, serialization, writing intermediate data, and additional load on memory and disk.

### {{product-name}} Shuffle Service { #yt-shuffle-service }

**{{product-name}} Shuffle Service** is an implementation of the shuffle infrastructure for SPYT on top of {{product-name}}.

It is used for storing and transferring intermediate shuffle data in the integration of Spark with {{product-name}}. This is an infrastructure component that helps adapt Spark’s shuffle model to the {{product-name}} environment.


It’s important to distinguish between:

- **shuffle** is a Spark mechanism;
- **{{product-name}} Shuffle Service** is an infrastructure implementation for storing and transferring shuffle data in {{product-name}}.

## Work units — Spark vs {{product-name}} { #units }


### Spark Application { #spark-application }


**Spark Application** is a unit of a user program in Spark.


A Spark application consists of:

- one driver;
- a set of executors;
- a logical computation plan and related job, stage, and task.

From a practical perspective, one launch of `spark-submit`, `spark-submit-yt`, or one user Spark session corresponds to one Spark Application.

It’s important not to confuse Spark Application with a {{product-name}} operation, Spark Job, or {{product-name}} Job:

- **Spark Application** is a logical execution unit in Spark;
- **{{product-name}} operation** is an infrastructure unit for launching processes in {{product-name}}.

One Spark application in SPYT can use one or more {{product-name}} operations to launch its components. Conversely, one {{product-name}} operation can be used to launch several Spark applications (`standalone cluster`).

### Spark Job { #spark-job }

**Spark Job** is a unit of computation inside a Spark application, typically spawned by a single action.

For example, calls to `count()`, `collect()`, `show()`, or writing a result can launch separate Spark Jobs.

A Spark Job is divided into:

- **stage** — an execution stage;
- **task** — an individual task within a stage.

### {{product-name}} Job { #yt-job }


**{{product-name}} Job** is a process launched by the {{product-name}} scheduler within a {{product-name}} operation.

This is an infrastructure execution unit in {{product-name}}. For example, in the context of SPYT, a {{product-name}} Job may correspond to a process where an executor or another Spark component runs.


The difference between Spark Job and {{product-name}} Job is fundamental:

- **Spark Job** is a logical unit of the Spark execution plan;
- **{{product-name}} Job** is an infrastructure process launched by {{product-name}}.
