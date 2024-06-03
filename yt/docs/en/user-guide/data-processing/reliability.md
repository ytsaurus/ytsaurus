## Fault tolerance

This article describes how the data processing system handles failure of individual cluster nodes and what guarantees are provided to {{product-name}} cluster users.

In terms of fault tolerance, the data processing system pursues the following objectives:
- Ensure availability and reliability.
- Preserve the progress of already completed jobs for running operations.

### Availability

The availability of the data processing system is directly connected with liveness of the scheduler.

Within the cluster, the scheduler is represented as several independent processes, which are typically executed on different nodes. Each process constantly tries to acquire a lock on the `//sys/scheduler/lock` node in Cypress until it succeeds. The succeeding process fetches meta information about current operations from Cypress, recovers its state, and becomes **active**. This means that it begins to handle user requests related to operations, process heartbeats from exec nodes, etc.

If, for whatever reason, there is no active scheduler, the system can't schedule new jobs, make new allocations, or run new operations on the cluster. Essentially, the data processing system is unavailable.

If the active scheduler fails, it takes some time for another process to acquire the lock and become active. This usually takes tens of seconds, the duration of such downtime depends on the following parameters:
- The frequency of attempts to acquire the lock by the scheduler process.
- The timeout of the transaction which is used for the lock acquiring.
- The size of meta information about current operations. This value becomes significant if the cluster has thousands of running operations simultaneously, as it takes considerable time to fetch all of them from Cypress and recover the scheduler state.

Usually users don't notice these periods of unavailability, since it is handled by retries in the {{product-name}} SDKs.

### Reliability

Remind that computations on the cluster are represented by operations. To be reliable, the data processing system must guarantee that running operations won't be lost or fail in case of failure of any data processing component in the cluster.

The system guarantees that if the scheduler or any other component fails, all operations will be recovered and continue running. However, part of a running operation's progress may still be lost.

The reliability of operations is ensured by Cypress, which safely stores meta information of current operations. When an operation is started, the system additionally creates transactions and acquires proper locks on input and output tables and files. This allows the operation to continue execution even if the input tables are removed or if the active scheduler has changed and operation revival happened.

Note that these transactions have a timeout, which is typically a few hours. If the scheduler remains unavailable for a longer period then the new incarnation will attempt to restart operations from scratch. However, it may not have the necessary input or output tables or files. In this case, the operation aborts with an error.

### Preserving computation progress

Unlike with real-time cluster components, the execution of a single operation takes a significant amount of time: from minutes to days depending on the size of processed data, the available computational quotas, and the code executed within the jobs. Because of this, the requirements for the availability of the data processing system are rather relaxed: delays of a few minutes, and in most even a few dozens of minutes, are not supposed to affect the SLOs of systems that run their computations on {{product-name}} clusters.

To define exactly the guarantees for preserving the progress of computations, we first need to describe how the life cycle of operations and jobs is organized.

The state of an operation can be divided into 3 parts:
1. The operation's meta information: its specification, set of Cypress transactions, input/output tables and files that are locked to run the operation, various runtime settings, and the current [operation state](operations/overview#status). The scheduler essentially implements a finite-state machine that handles each operation, and the operation state is a node within it.
2. The state of the operation controller. An operation controller is an object that resides within the controller agent process and is responsible for the operation's execution. The controller manages information about which jobs are currently running within the operation, which ones have finished, which data has already been processed and which hasn't. Overall, this object encapsulates all the data processing logic for the operation as well as its state.
3. A set of all currently running and some of the finished jobs on the cluster's exec nodes.

The scheduler *synchronously* writes the operation's meta information to the corresponding Cypress node, which resides in the `//sys/operations` subtree. The operation controller consistently writes its state to Cypress as a binary file. This file is called a **snapshot of the operation**. Each controller agent periodically generates snapshots of its current operations.

Active jobs within each operation periodically report their state to the operation controller via heartbeats from exec nodes. Each exec node sends heartbeats with information about all relevant jobs to each controller agent.

Let's consider the behavior of the data processing system in case of failures of individual components.

* If the failed component is a cluster's exec node, after a small timeout (typically 1–2 minutes), the controller agents and the scheduler notice that the node has stopped sending heartbeats and as a result consider all allocations and corresponding jobs to be lost. These computations will be re-scheduled on the other cluster nodes and logically these computations will be represented as new jobs instead of the lost ones.
* If the failed component is a controller agent, the current state of the operation controllers that were running on that agent is lost. In this case, the scheduler detects that the controller agent is unavailable and re-assigns all of its assigned operations to other controller agents. When re-assigned to a new agent, the operation controller is recovered from the last saved snapshot. While handling heartbeats from exec nodes, recovered operation controller gradually updates its state with the actual state of the cluster.
* If the failed component is the active scheduler, each controller agent detects this situation, resets the inner state and starts waiting for the new active scheduler. The new scheduler reads the current meta information about operations, then re-distributes these operations across controller agents and starts handling user requests and node heartbeats. In this case, the operations are recovered from the latest snapshots written to Cypress.

The process of recovering operations from a snapshot is called _reviving_. Note that if there's no snapshot, the operation is still revived but starts from scratch.

Importantly, nodes also store information about completed jobs to report them to the controller if the operation needs to be revived — this helps avoid losing these jobs' progress. Let's consider an example of a situation where loss of information about a completed job can be avoided.

Imagine there's a a job (`J`) within an operation (`O`) running on a node. The execution sequence can be as follows:

#|
|| **Timestamp**| **Event** ||
|| `X`| Job `J` is running on the node. The controller agent captures a snapshot of operation `O`. In this snapshot, the operation controller considers job `J` to be in progress. ||
|| `X+1`| Job `J` completes, and the operation controller processes the completion event.||
|| `X+2` | Operation `O` is revived (for example, after a restart of the controller agent). After recovery from the snapshot, the controller of operation `O` assumes that job `J` is still running.||
|| `X+3` | While handling a heartbeat from the node on which job `J` was running, the operation controller notices that it isn't presented among currently running jobs.

This prompts it to request information about all previously completed jobs from the node. In the next heartbeat, the node informs the controller that job `J` has completed. The controller processes this event and doesn't start a new job.||
|#

For more information about the revival mechanism, see this [report](../../_includes/other/video-posts.md#2019-map-reduce-operaciya-dlinoyu-v-god). It should be noted, however, that this mechanism has undergone a number of changes since the report's publication.

In summary:
- If the scheduler or one of the controller agents fails, it takes a few minutes to recover the state. In this case, some of the progress since the last saved operation snapshot is lost.
- If an exec node fails, progress is lost for all jobs that were running on that node.

#### Consistency of scheduler and controller agent states

The communication protocol between the scheduler and controller agents is fairly complex and extensive. It's important to understand how the system ensures consistency of their states and actions in the event of individual component failure or loss of network connectivity.

This consistency is primarily achieved by *coupling* the controller agents with the active scheduler. Below is a detailed explanation of how the scheduler and agents become active and what this *coupling* means.

Let's recall how the active scheduler is selected. Each scheduler process constantly creates a transaction and tries to acquire an exclusive lock on a Cypress node within that transaction. The scheduler that succeeds becomes active. It reads meta information about all operations from Cypress and recovers their state.

Controller agents operate as follows:
- First, an agent establishes a connection with the scheduler and receives the transaction in which the scheduler became active.
- Then, the agent creates its own transaction, which is nested within the scheduler transaction, and attempts to acquire a lock on the `//sys/controller_agents/instances/<instance_address>/lock` node. Typically, the agent doesn't have to compete with other processes to acquire the lock, but it may conflict with its previous incarnation.
- When the agent succeeds in acquiring the lock, it also becomes *active*.

Once the agent has become active, the scheduler starts assigning it operations, and the agent schedules the jobs for these operations.

The agent transaction serves several functions:
- It allows the scheduler to manage the agent and ensures that the agent is coupled with the currently active scheduler. With this transaction, the scheduler can disconnect the controller agent in various scenarios, such as loss of network connectivity or receiving outdated messages. To do this, it simply needs to abort this agent's transaction. In addition, if the scheduler transaction is aborted, all associated agent transactions are aborted as well.
- It's used by the agent as a prerequisite for its interactions with Cypress. This ensures that the operation state in Cypress can be changed exclusively by the agent to which this operation is assigned.
- It's used as a session ID for communication with the scheduler. Specifically, agent transaction is passed within each message from agent to scheduler. If the scheduler starts processing a message and detects that the current agent transaction differs from the one in the message, this means that the message came from a previous incarnation and should be ignored.

All of this ensures that an operation is handled by only one controller agent at any given time.
