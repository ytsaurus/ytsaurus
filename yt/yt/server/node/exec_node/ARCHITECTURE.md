# Exec Node Architecture

This document summarizes the architecture of `yt/yt/server/node/exec_node` for future development work, especially for AI agents that need to identify the right extension points quickly.

## Scope

`exec_node` is the part of a cluster node responsible for:

- receiving and tracking scheduler allocations
- talking to controller agents for per-job lifecycle management
- preparing job sandboxes, layers, volumes, and container/runtime environment
- spawning and supervising `job_proxy`
- accounting and releasing CPU / memory / disk / GPU resources
- reporting node, allocation, and job state back to scheduler / controller agents / masters

It is not a standalone daemon under this directory. It is a component of the cluster node process.

## Process entry point and lifecycle

There is no `program.cpp` inside `exec_node` itself.

The process starts from cluster node code:

- `yt/yt/server/node/cluster_node/program.cpp`
- cluster-node bootstrap creates the node and then initializes sub-bootstraps, including exec-node bootstrap

The exec-node-specific lifecycle begins in:

- `bootstrap.h`
- `bootstrap.cpp`

The key interface is `NExecNode::IBootstrap`.

### Initialization order

`TBootstrap::Initialize()` wires together the subsystem graph in a deliberate order:

1. subscribe to dynamic config and master-cell changes
2. create `TSlotManager`
3. create and initialize `TGpuManager`
4. create `TJobReporter`
5. create `IMasterConnector`
6. create `TSchedulerConnector`
7. create `IJobController`
8. optionally create `IJobProxyLogManager`
9. create `TControllerAgentConnectorPool`
10. build job-proxy config template
11. create `TArtifactCache`
12. create `IThrottlerManager`
13. create `IJobInputCache`
14. register RPC services
15. initialize caches and core managers

Important ordering invariant from `bootstrap.cpp`:

- `SlotManager_` must be initialized before `JobController_` so the first job-proxy build-info update can reach the job controller before the node starts accepting work.

### Run phase

`TBootstrap::Run()` then starts runtime services:

- throttlers
- optional NBD server and chunk-reader hosts
- Orchid nodes
- signature rotation
- job-proxy log manager
- slot manager
- job controller
- job-proxy Solomon exporter
- scheduler connector
- controller-agent connector pool

## Main subsystem map

### 1. Bootstrap and service locator

Files:

- `bootstrap.h`
- `bootstrap.cpp`
- `public.h`

Responsibilities:

- own and expose exec-node singletons
- bridge exec-node with cluster-node bootstrap
- build reusable job-proxy config template
- register RPC services
- expose shared services to the rest of the subtree

If you need a cross-cutting dependency, first check whether it should be exposed through `IBootstrap`.

### 2. Job controller: authoritative runtime state

Files:

- `job_controller.h`
- `job_controller.cpp`

`IJobController` is the main authority for live exec-node work.

It owns and coordinates:

- all active allocations
- all jobs
- recently removed jobs retained for late confirmations / reporting
- scheduler heartbeat payload generation and response processing
- controller-agent heartbeat payload generation and response processing
- job disabling / abort / interruption logic
- resource-adjustment and profiling periodic executors

Conceptually:

- scheduler owns allocations
- controller agents own job specs and job-level control
- `TJobController` stitches them together on the node

This is the first file to read when changing:

- allocation startup / teardown
- heartbeat semantics
- adoption / orphaning of jobs
- interruption / preemption behavior
- resource-based scheduling behavior on the node

### 3. Allocation: scheduler-granted execution budget

Files:

- `allocation.h`
- `allocation.cpp`

`TAllocation` represents scheduler-level permission to run work with a given resource envelope.

Responsibilities:

- hold scheduler allocation identity and state
- own resource reservation via `TResourceOwner`
- request job settlement from controller agent
- create a `TJob` after a job spec is received
- handle preemption / abort / completion of the allocation
- transfer resources from allocation scope to job scope

Useful mental model:

- allocation is the scheduler-facing container for resources
- job is the controller-agent-facing execution instance that may run inside that allocation

### 4. Job: actual execution state machine

Files:

- `job.h`
- `job.cpp`

`TJob` is the largest and most operationally dense type in this component.

Responsibilities:

- hold job spec, state, phase, result, statistics, and timings
- acquire slot / GPU / other resources from resource holder
- prepare workspace, artifacts, root FS, tmpfs, setup commands, and GPU checks
- spawn `job_proxy`
- manage probes / monitoring / periodic reporting state
- process abort, failure, interruption, and finish transitions
- perform cleanup and resource release

Key phase transitions visible in code:

- created
- preparing job / environment-specific preparation phases
- spawning job proxy
- preparing artifacts
- running
- finishing / waiting for cleanup
- cleanup
- finished

Important operational facts:

- `TJob::Start()` validates slot usability first
- `TJob::RunJobProxy()` injects hot-chunk information from `IJobInputCache` before spawning job proxy
- `TJob::OnJobProxyFinished()` may trigger an extra GPU check before finalization
- `TJob::Cleanup()` is the place where processes, volumes, sandbox, NBD subscriptions, and resource holders are actually released

When investigating job failures, `job.cpp` is almost always the main file.

### 5. Slot manager: node-local execution environment and slot availability

Files:

- `slot_manager.h`
- `slot_manager.cpp`
- `slot.h`
- `slot.cpp`
- `slot_location.h`
- `slot_location.cpp`

`TSlotManager` controls user slots and overall job-environment availability.

Responsibilities:

- initialize and resurrect job environment
- own slot locations and free-slot queues
- acquire and release slots
- maintain alerts that can disable scheduling
- react to porto/container-health failures
- expose disk resources and Orchid state
- create per-slot runtime objects through `CreateSlot(...)`

Important state:

- manager state: disabled / disabling / initializing / initialized
- alert set: persistent and transient alerts affect scheduling availability
- free-slot queue and NUMA bookkeeping
- `JobEnvironment_` and `VolumeManager_`

Important invariant:

- slot manager health directly participates in `IJobController::AreJobsDisabled()`
- if slot manager is disabled, scheduler work may still be known to the node, but new execution should not start

### 6. Job environment: runtime backend abstraction

Files:

- `job_environment.h`
- `job_environment.cpp`

`IJobEnvironment` abstracts the execution backend.

Responsibilities:

- initialize backend-specific runtime
- initialize per-slot runtime state
- run job proxy
- clean processes
- provide job directory manager
- enrich job-proxy config with backend-specific settings
- create workspace builder and volume manager
- run setup / preparation commands

This abstraction is the boundary between generic exec-node logic and backend-specific container/process execution.

Examples of backend-specific behavior include:

- Porto-based environment
- simple environment
- CRI-related workspace / image logic

If you need to change how jobs are launched inside containers or how the runtime is initialized, start here.

### 7. Workspace, artifacts, and filesystem preparation

Files:

- `job_workspace_builder.h`
- `job_workspace_builder.cpp`
- `artifact_cache.h`
- `artifact_cache.cpp`
- `job_directory_manager.h`
- `job_directory_manager.cpp`
- `job_input_cache.h`
- `job_input_cache.cpp`
- `volume_manager.h`
- `volume_manager.cpp`
- `volume.h`
- `volume.cpp`
- `volume_cache*.{h,cpp}`
- `porto_volume*.{h,cpp}`
- `tmpfs_layer_cache*.{h,cpp}`
- `layer_location*.{h,cpp}`
- `cache_location*.{h,cpp}`

These files implement filesystem and artifact preparation before `job_proxy` starts.

#### `TJobWorkspaceBuilder`

This is the orchestration layer for job workspace preparation.

It sequences steps such as:

- preparing root volume
- validating root FS
- preparing tmpfs volumes
- preparing GPU-check volume
- binding/linking root and tmpfs volumes
- preparing sandbox directories
- running setup commands
- running custom preparations
- running GPU check command

Different builders exist for different environments:

- simple
- porto
- cri

#### `TArtifactCache`

This is the node-local artifact cache.

Responsibilities:

- maintain cache locations
- download artifact chunks asynchronously
- expose cached artifacts
- provide stream producers for direct artifact materialization

Use this area when changing layer or file materialization behavior.

#### `IVolumeManager`

Volume manager is responsible for root volumes and tmpfs volumes.

Responsibilities:

- prepare root overlay volume
- prepare tmpfs volumes
- rebind root volume if needed
- link tmpfs mounts into target directories
- remove planted volumes
- manage layer-cache state

The volume stack is especially relevant for Porto-based jobs, layered root filesystems, and GPU-check roots.

### 8. GPU management

Files:

- `gpu_manager.h`
- `gpu_manager.cpp`
- `job_gpu_checker.h`
- `job_gpu_checker.cpp`

`TGpuManager` is the node-level GPU resource and health subsystem.

Responsibilities:

- discover GPU devices
- track free / acquired / lost devices
- run health checks and RDMA updates
- apply network priority
- provide GPU setup commands and topping layers
- serve Orchid diagnostics

`TJobGpuChecker` is job-scoped and used from `TJob` / workspace preparation for GPU validation.

Important behaviors:

- jobs may need GPU-specific setup layers and commands
- failed jobs may trigger an extra GPU check in `OnJobProxyFinished`
- slot manager can raise alerts based on GPU-check failures or too many consecutive GPU job failures

### 9. Scheduler connector: allocation control plane

Files:

- `scheduler_connector.h`
- `scheduler_connector.cpp`

`TSchedulerConnector` is responsible for scheduler heartbeats.

Responsibilities:

- collect node resource usage / limits from job-resource manager
- send regular and out-of-band scheduler heartbeats
- forward heartbeat preparation / processing to `IJobController`
- feed back min spare resources to control out-of-band heartbeats
- track finished allocations until acknowledged

Threading model:

- RPC send/receive logic runs on control invoker
- heartbeat content preparation / processing is delegated onto job invoker

This split is important: scheduler protocol handling is control-thread oriented, but authoritative allocation/job mutation stays on the job thread.

### 10. Controller-agent connector pool: job control plane

Files:

- `controller_agent_connector.h`
- `controller_agent_connector.cpp`

There is one connector per controller-agent descriptor, managed by `TControllerAgentConnectorPool`.

Responsibilities:

- maintain channels and heartbeat executors for controller agents
- settle jobs for allocations via `SettleJob(...)`
- report running / finished job state and statistics
- receive requests to store, confirm, and interrupt jobs
- react to agent incarnation changes

Important distinction from scheduler connector:

- scheduler heartbeats manage allocations and operation-to-agent affinity
- controller-agent heartbeats manage actual jobs and job specs

### 11. Master connector

Files:

- `master_connector.h`
- `master_connector.cpp`

`IMasterConnector` reports exec-node-specific data to masters.

Responsibilities:

- send slot-location statistics and job-proxy build version
- receive `disable_scheduler_jobs` control signal
- propagate master-driven scheduling disablement to job controller

This is the main path by which master/decommission state can disable job execution on the node.

### 12. RPC/admin/introspection services

Files:

- `exec_node_admin_service.*`
- `job_prober_service.*`
- `supervisor_service.*`
- `proxying_data_node_service.*`
- `orchid.*`
- `job_proxy_log_manager.*`
- `throttler_manager.*`

These provide operational support around the main execution pipeline.

Examples:

- admin RPCs
- job probing / supervision
- proxying to data-node functionality needed by jobs
- Orchid introspection tree
- per-job-proxy logging support
- per-traffic-kind throttlers

## Control flow summaries

## Scheduler-to-job path

1. scheduler connector sends heartbeat with current resource usage and allocation state
2. scheduler responds with allocations to start / abort / preempt and operation info
3. job controller processes the response on job invoker
4. new `TAllocation` objects are created
5. allocation waits for resources and then starts
6. allocation settles a job with the responsible controller agent
7. job controller / allocation creates `TJob`
8. job prepares workspace and starts `job_proxy`
9. job runs, reports status/statistics through controller-agent heartbeats
10. job finishes and cleans up
11. allocation is reported back to scheduler until acknowledged

## Controller-agent interaction path

1. scheduler associates operations with controller-agent incarnations
2. controller-agent connector pool maintains connectors for registered agents
3. allocation uses its current agent descriptor to call `SettleJob(...)`
4. controller agent returns job id + job spec
5. node runs the job
6. controller-agent heartbeats carry job status/statistics/results
7. controller agent may ask node to:
   - store finished jobs
   - confirm jobs
   - interrupt jobs
   - remap orphaned operations/jobs to a new agent incarnation

## Job execution path

1. `TJob::Start()` validates slot state and transitions to running
2. address resolution and other async preparation starts
3. workspace builder prepares root FS / layers / tmpfs / artifacts / setup commands
4. `TJob::RunJobProxy()` builds config and spawns job proxy via slot / environment
5. job proxy executes user payload
6. job proxy exit is handled by `TJob::OnJobProxyFinished()`
7. optional extra GPU check may run
8. `Finalize(...)` determines final job state/result
9. `Cleanup()` stops processes, removes volumes, cleans sandbox, unregisters caches, and releases resources

## Resource ownership model

The resource model is easy to misunderstand; keep this in mind:

- `TJobResourceManager` is the underlying resource authority shared with node job-agent logic
- `TAllocation` initially owns a resource holder for scheduler allocation resources
- resources are transferred to `TJob` when a job is settled/prepared
- `IUserSlot` and `TGpuSlot` are attached to the resource holder
- cleanup must release both slot-related and non-slot resources

Relevant files outside this directory:

- `yt/yt/server/node/job_agent/job_resource_manager.h`
- corresponding implementation in node job-agent

## Threading model and invariants

The subtree relies heavily on thread-affinity annotations.

Two important invokers dominate behavior:

- job invoker / `JobThread`: authoritative mutation of jobs, allocations, and slot-manager internals
- control invoker / `ControlThread`: outbound heartbeats and connector control-plane work

Practical rule:

- if code mutates `IdToJob_`, `IdToAllocations_`, slot queues, job state, or allocation state, it likely belongs on job invoker
- if code performs periodic RPC heartbeat transmission, it likely starts on control invoker but delegates stateful preparation/processing back to job invoker

Common invariant pattern:

- connector sends RPC on control thread
- request/response marshaling that touches job/allocation state is bounced onto job thread

Violating this model is a common source of subtle bugs.

## Dynamic config propagation

Many subsystems own atomic dynamic config pointers and expose `OnDynamicConfigChanged(...)`:

- slot manager
- scheduler connector
- controller-agent connector pool / per-agent connectors
- GPU manager
- job controller
- volume manager / job environment
- master connector

When adding runtime-tunable behavior, look for the subsystem-local dynamic config first before threading new config through unrelated layers.

## Failure and disablement model

There are several independent mechanisms that can stop new work:

- master can disable scheduler jobs
- loss of master connectivity disables jobs from job-controller perspective
- slot manager can disable scheduling due to environment / porto / GPU / cleanup alerts
- job-level failures only terminate one job unless they trip slot-manager-wide alert thresholds

Important implication:

- not every failure should disable the node
- node-wide disablement usually flows through `TSlotManager` or master state, not directly through `TJob`

## Where to edit for common tasks

- Change scheduler heartbeat payload or allocation start/abort/preempt behavior:
  - `job_controller.cpp`
  - `scheduler_connector.cpp`

- Change controller-agent reporting, job confirmations, or job settlement:
  - `job_controller.cpp`
  - `controller_agent_connector.cpp`

- Change how jobs are prepared or cleaned up:
  - `job.cpp`
  - `job_workspace_builder.*`
  - `slot.cpp`

- Change slot acquisition / scheduling disablement / environment resurrection:
  - `slot_manager.cpp`
  - `job_environment.cpp`

- Change artifact download/materialization or layer cache behavior:
  - `artifact_cache.cpp`
  - `volume_manager.cpp`
  - `volume_cache.cpp`
  - `layer_location.cpp`

- Change GPU discovery/check/setup behavior:
  - `gpu_manager.cpp`
  - `job_gpu_checker.cpp`
  - GPU-related branches in `job.cpp`

- Change node-to-master reporting or master-driven disablement:
  - `master_connector.cpp`

## High-risk areas for changes

Be extra careful in these areas:

- job-thread vs control-thread boundaries
- resource-holder transfer between allocation and job
- cleanup ordering in `TJob::Cleanup()`
- slot-manager initialization order and resurrection logic
- controller-agent incarnation migration / orphaned job handling
- CPU vs vCPU conversion in scheduler heartbeat code
- any path that disables slot manager or jobs globally

## Recommended reading order for future agents

For most tasks, read in this order:

1. `bootstrap.h` / `bootstrap.cpp`
2. `job_controller.h` / `job_controller.cpp`
3. `allocation.h` / `allocation.cpp`
4. `job.h` / `job.cpp`
5. one of:
   - `slot_manager.*` for environment/slot issues
   - `scheduler_connector.*` for scheduler protocol issues
   - `controller_agent_connector.*` for controller-agent issues
   - `artifact_cache.*` + `job_workspace_builder.*` + `volume_manager.*` for filesystem/artifact issues
   - `gpu_manager.*` for GPU issues

## Short mental model

Use this simplified model when navigating the code:

- `bootstrap` owns subsystems
- `scheduler connector` brings allocations
- `job controller` owns authoritative job/allocation state
- `allocation` owns resource budget and obtains job specs
- `job` performs execution and cleanup
- `slot manager` owns runtime readiness and slot availability
- `job environment` abstracts the backend runtime
- `workspace/artifact/volume` code prepares filesystem state
- `controller-agent connector` is the job-level control plane
- `master connector` can globally disable scheduling

If you only remember one thing: `job_controller.cpp` is the control center, and `job.cpp` is the execution state machine.
