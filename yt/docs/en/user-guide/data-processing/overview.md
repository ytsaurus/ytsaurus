## Data processing

This section contains information about running computations and data processing in a {{product-name}} cluster.

Data processing is performed in a distributed manner. The logical unit of computation is an **operation**. Most operations have a set of input and output tables. They process data from the input tables, writing the results to an output table. For example, an operation may sort table `A` and save the result to table `B`, or it may execute user code to process rows from table `A` and write the result to table `B`. {{product-name}} also allows running operations that execute a custom bash command, such as running a user-defined executable, in a distributed manner. This feature is particularly useful for running distributed ML operations on a cluster.

There are several [types of operations](operations/overview#obzor). Among other things, they implement the [MapReduce](https://en.wikipedia.org/wiki/MapReduce) paradigm for distributed data processing.

Each operation consists of a number of **jobs**, which are executed in the cluster in parallel and usually independently. Each job is an isolated process that handles a part of the overall computation and is executed on one of the cluster nodes. Before starting a job, an **allocation** is made on the node. An allocation is a logical entity characterized by a set of computational resources, such as RAM and CPU. After this, the job is executed within the created allocation.

The general concept and design of the {{product-name}} data processing system is similar to such systems as:
* [Hadoop MapReduce](https://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html) and [Apache YARN](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YARN.html).
* [Apache Mesos](https://mesos.apache.org/).

{% note info "Note" %}

Please be aware that {{product-name}} and Hadoop use different terminology: a {{product-name}} operation would be called a job in Hadoop's terms, while a {{product-name}} job would be called a task.

{% endnote %}


## Architecture

The components involved in running computations in a cluster include the scheduler, controller agents, and exec nodes.

![](../../../images/data_processing_overview.png)

The **scheduler** is the key component that stores information about all operations, makes new allocations for operations, and implements hierarchical resource distribution.

**Controller agents** are responsible for scheduling the jobs of individual operations. They contain the logic for the operation's execution plan, which breaks down the operation into individual jobs. The implementation correctly handles loss of individual jobs and ensures transactional execution of operations.

An **exec node** is a process that runs on all compute nodes within the cluster. It handles the execution of individual jobs within allocations, prepares the environment for their execution, and ensures that the jobs are isolated from each other.

At any moment, the cluster state consists of:
- A single active scheduler that handles all allocation tasks.
- Several independent controller agents that are responsible for scheduling operations.
- A set of exec nodes that perform the computations.

The scheduler and the controller agents store their state in Cypress, while exec nodes don't have a persistent state and can be easily added to and removed from the cluster.

Let's make a short overview of the interaction between these components.

The scheduler provides an API for starting new operations and managing operations that are already running. It also periodically reads its own configuration and the [pool](scheduler/scheduler-and-pools.md) hierarchy from Cypress. In addition, the scheduler uses Cypress as a persistent storage for the operation's meta state. An operation's meta state is a small set of attributes that are necessary to manage transactional execution of the operation and ensure fault tolerance.

When an operation is started, the scheduler initializes its state in Cypress and assigns it a controller agent, which will handle the job scheduling for that operation.

Each exec node periodically sends a heartbeat to the scheduler, informing it about the status of current allocations and receiving new allocations. It also receives requests to preempt some of the existing allocations and abort any associated jobs. In addition, exec nodes send heartbeats to each controller agent, informing them of the status of currently running jobs and retrieving specifications for starting new ones.

## Features and capabilities

Below is a description of the features and various capabilities provided by the {{product-name}} data processing system.

### Fault tolerance

{{product-name}} guarantees reliability and availability of data processing. Even if individual components fail, the cluster can still recover and continue to function. In such situations, the progress of individual operations may be lost, but the operations' states are safely stored in Cypress and operations will be correctly recovered.

In addition, the system aims to minimize loss of progress for running operations. For more information, see [this document](reliability).

### Scalability

The system offers a transparent way to add new exec nodes to the cluster in order to increase computational resources. It also supports the addition of extra controller agents, which is necessary for scaling up the number of operations that can be executed on the cluster.

The largest {{product-name}} cluster installations service more than 1M CPUs and can run tens of thousands of operations simultaneously.

Note that the active scheduler is a single process, so the system's scalability is still limited. For more information about scheduler's scalability, see [this presentation](https://www.youtube.com/watch?v=Vy6rHf-BIG8){% if lang == "en" %} (in Russian)
{% endif %}.

### Resource management

The scheduler provides a rich API for distributing cluster resources among different users and projects. The basic unit for managing resources is a **pool**. The pools form a hierarchy called a **pool tree**. For each pool, you can configure guaranteed resources, manage resource distribution weights, and set resource limits and various [preemption](scheduler/preemption.md) settings.

For more information about resource allocation, see [Scheduler and pools](scheduler/scheduler-and-pools).

### Resource heterogeneity

{{product-name}} clusters can handle heterogeneous exec nodes and heterogeneous resource requirements of running jobs. For example, the same cluster can run both memory-intensive and CPU-intensive computations simultaneously.

In addition, the system is designed to be reactive, which means that scheduler tries to start new jobs as quickly as possible using any available resources, while the efficient packing of jobs on the exec nodes is not a priority for the scheduling algorithm. Because of this, highly imbalanced loads may lead to fragmentation of resources in the cluster.

### GPU support

Exec nodes are capable to start jobs that require GPU devices. In addition, the scheduler can be informed of the network topology on GPU hosts, and it will be taken into account for [gang operations](https://en.wikipedia.org/wiki/Gang_scheduling) scheduling.

Internally, GPU resource is represented as a single integer indicating the number of devices, e.g. the total number of GPUs on a node or a number of GPUs required by a job. For this reason, it's recommended to split nodes with different GPU types into separate [pool trees](scheduler/scheduler-and-pools#puly-i-derevya-pulov).

{% if audience == "internal" %}
For more information about GPU support, see [GPU support in YT](operations/gpu).
{% endif %}

### Support for multiple data processing interfaces

The {{product-name}} data processing system not only implements the MapReduce paradigm, but also supports running other data processing applications over shared computational resources. For example, you can run [YQL](../../yql) queries over {{product-name}} or create [ClickHouse cliques](chyt/about-chyt) and [Spark clusters](spyt/overview).
