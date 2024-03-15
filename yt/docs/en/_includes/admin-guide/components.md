# {{product-name}} сluster components

This section describes the main components that make up the {{product-name}}cluster.

## Master

Masters are one of the key cluster components. {{product-name}} masters are responsible for fault-tolerant storage of cluster metadata. This includes information about system users, stored objects, and the location of the data itself. The masters implement a file system called Cypress.

To ensure fault tolerance, a {{product-name}} cluster needs to have several masters. All masters share the same state, which is enabled by `Hydra`, a consensus algorithm similar to the [Raft](https://raft.github.io/) protocol. With `Hydra`, the cluster remains operational as long as more than half of its masters are available. For instance, if a cluster has three masters, it will continue to function as long as any two of them are available. The number of masters should be odd. For production installations, we recommend using three or five masters.

{{product-name}} masters are stateful components of the cluster, because they store changelogs and snapshots of the cluster state on the disk. For optimal master performance, we recommend using a storage medium that's good at handling a large stream of small records, such as NVMe SSD.

![](../../../images/yt_cluster_components.png)

## Data Nodes

While {{product-name}} masters are responsible for storing cluster metadata, the actual cluster data is stored on `Data Nodes`. Files and tables saved in {{product-name}} are split into smaller segments known as [chunks](../../user-guide/storage/chunks.md). `Data Nodes` store chunks in the disk file system, while masters hold information about which chunks make up specific tables and files as well as which `Data Nodes` they are stored on.

Like {{product-name}} masters, `Data Nodes` are stateful components, since they store chunks on disks. However, unlike masters, chunks are typically read and written at coarse scale, which means that both HDDs and SSDs can be used for their storage, depending on the system load. {{product-name}} supports a variety of disk types and allows grouping them into [media](../../user-guide/storage/media.md) sets to store data on different types of storage devices.

## Scheduler

Cluster computing is enabled by a scheduling subsystem comprising a scheduler and agent controllers. The computations themselves are carried out on `Exec Nodes`.

A sharded scheduler is used in {{product-name}}, so it consists of two components: the scheduler itself and the agent controllers.

The scheduler is responsible for fair distribution of resources among cluster users through the HDRF algorithm, which itself is a generalization of the [fair-share](https://en.wikipedia.org/wiki/Fair-share_scheduling) algorithm. The scheduler communicates with `Exec Nodes`, initiating new jobs and receiving information on their completion.

At any given moment, the cluster has only one active scheduler, which is responsible for allocating resources and initiating jobs. To ensure fault tolerance, we recommend running multiple schedulers within the same cluster. In this case, only one scheduler is active, with the rest being on standby in case it fails. The scheduler is a stateless component, because part of its state (like the list of all active jobs) is transient. The persistent part, such as the descriptions of currently running operations, is stored on masters in Cypress.

Agent controllers represent the second element of the scheduling subsystem. They are responsible for the logic behind the execution of operations. If we take the Map operation, agent controllers determine which chunks to process, how to divide the input data into jobs, which resources are needed to initiate these jobs, and how to merge job results into output tables. Meanwhile, the scheduler determines the amount of jobs that can be initiated simultaneously and the `Exec Nodes` they can be executed on given the user's computing quotas. Like schedulers, agent controllers are stateless components, though they store part of their state in Cypress.

## Exec Nodes

`Exec Nodes` are directly involved in executing jobs on the cluster's computing resources. They send heartbeats to the scheduler, informing it about the initiated/completed jobs and the available resources.`` In response, `Exec Nodes` receive requests from the scheduler to initiate new jobs. `Exec Nodes` are responsible for preparing the job environment, launching containers to isolate processes, managing the lifecycle of user processes, and much more.

To function, `Exec Nodes` require access to the disk. First, this is necessary to create directories for initiating user processes and loading files that the user specified in the operation. Second, the disk contains a `Chunk Cache`, or an artifact cache that stores the files that were loaded to a job for reuse in different jobs as part of one or multiple operations. Such disks don't have to be reliable: unlike masters or `Data Nodes`, an `Exec Node` losing its state results in a loss of progress only for the jobs that were executed on that node.

## Tablet Nodes

`Tablet Nodes` are responsible for the operation of dynamic tables. Each dynamic table is divided into a set of tablets managed by tablet cells that run in a `Tablet Node`. Despite the fact that dynamic tables have a fairly complex persistent state, `Tablet Nodes` are stateless components. All necessary data is stored on `Data Nodes`.

## YQL Agent

YQL agent instances orchestrate the execution of YQL queries, converting them into a chain of map/reduce operations.

## Proxy

The entry point to a cluster is a proxy. All user queries submitted to the cluster pass through the proxy before reaching the appropriate subsystems. {{product-name}} features two types of proxies: HTTP and RPC. Proxies are stateless components.

HTTP proxies accept commands as HTTP requests, which makes it easy to submit requests to {{product-name}} in any programming language.

RPC proxies use the {{product-name}} internal protocol, which makes working with them more complex but speeds up query processing. Using an RPC proxy is recommended for latency-critical commands with a high execution frequency, such as queries to dynamic tables.

