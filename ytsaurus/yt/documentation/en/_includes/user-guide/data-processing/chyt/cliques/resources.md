# Resources and preemption

A CHYT clique is a Vanilla {{product-name}} operation and instances are run in the jobs of this operation and consume resources. A typical CHYT instance runs on about 10 CPU cores and about 60 GB of RAM.

Before reading this article, read the [Scheduler and pools](../../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md) article.

## Preemption { #preemption }

The main mechanism for ensuring fair resource allocation in the {{product-name}} scheduler is preemption. This means that if at some point an operation starts consuming more resources than it is entitled to, any of its jobs will be "preempted", i.e. forcibly stopped, and someone who is actually entitled to resources will be started in its place. This job is called an *aborted job* and the reason for `abort` is `preemption`.

This approach works well for jobs of regular {{product-name}} operations, since a typical job in {{product-name}} has a duration of a few minutes and losing a job progress is not a problem — the scheduler will restart the job later when there are available resources for it. Map-Reduce has delays of about several minutes, so this situation is absolutely tolerable.

The situation with CHYT is a bit more complicated. The built-in ClickHouse protocol does not provide a mechanism to recover queries that failed due to a component failure at runtime. This means that if queries are now running on the instance and `abort` is sent to the job where the instance is running, then all running queries will fail with network layer errors (`NetException`, `Attempt to read after eof`, `Connection refused`, and others). By then, the client could have returned a random set of strings from the response to the query, which only complicates the task of restarting the query.

## Interruption { #interruption }

MR jobs have long used a mechanism called *Interruption*: if {{product-name}} realizes that it is about to interrupt a job, it simply stops feeding data to the job's input. If the job manages to complete within a certain `interruption_timeout`, it is considered `completed`, and the data it has not processed will be fed to a new job. If the job does not manage to complete within an `interruption_timeout`, it is simply aborted and its input is fully processed by another job.

ClickHouse instances do not process any input in the usual way for {{product-name}}, independently executing queries coming from the outer world, so information about the upcoming interruption comes to such jobs in a different way. For CHYT, the `interruption_signal` option is supported in the operation specification. This is a signal that will be sent to the job to notify it of an imminent completion.

When receiving the signal, the instance goes to completion mode, sends this information to the others using gossip, and no longer accepts any new queries, but continues executing those that are already in progress. After all current queries are executed, the instance will end with the `completed` status.

## Regular preemption and graceful preemption { #preemption-dif }

The {{product-name}} scheduler has the following logic for preempting jobs: if a node has jobs whose `fair-share_ratio` of the operation is less than `usage_ratio`, this job is considered `preemptable`. If the scheduler has a job of the starving operation that can be run on a node if preemptable jobs are preempted, then it does it — it preempts the required number of preemptable jobs and plans new ones in their place. In this model, it is unacceptable to wait very long to preempt jobs, so the `interruption_timeout` is only 15 seconds.

A typical usage scenario for CHYT is small and medium queries that run from single-digit seconds to several minutes; `interruption_timeout = 15 sec` is often less than the standard query time, which means that many queries that were running on a given instance will end with an error.

To combat instance preemption, you can start a clique in the pool with guarantees, but not all users have guarantees. Besides that, some users wanted the clique not to interfere with other operations in the pool — to be small when there is demand for resources in the pool and inflate when there are extra resources.

To solve this problem, a new operation scheduling mode — *Graceful Preemption* — was added. To avoid preemption problems in this mode, preemption is performed in advance: if there are `preemptable` jobs, a completion signal is sent to them, regardless of whether there is a candidate for these resources. Thus, the operation independently seeks that `usage_ratio = fair-share_ratio`, catching up with the fair amount of resources it is due at the moment. Due to this, you can increase the preemption timeout to 10 minutes, because no one is idle, waiting for the release of resources.

Below is an example of how `usage_ratio` (orange graph) and `fair-share_ratio` (green graph) change after a heavyweight operation is started in the same pool as the clique.

![](../../../../../../images/chyt_graceful_preemption.png){ .center }

To use *Graceful Preemption*, specify it in the `--spec {preemption_mode = graceful}` parameter.
