# Job Proxy Architecture

This document summarizes the architecture of `yt/yt/server/job_proxy` for future development work, especially for AI agents that need to identify the right extension points quickly.

## Scope

`job_proxy` is a standalone process spawned by exec-node for a single job.

It is responsible for:

- fetching the job spec from exec-node supervisor
- constructing the per-job execution environment
- running either a built-in job implementation or a user job executor
- exposing local control/probing APIs over Unix-domain-socket-backed RPC/GRPC/HTTP servers
- tracking resource usage, heartbeats, stderr, profiles, traces, and final job result
- reporting job progress and final status back to exec-node supervisor

It is not the component that allocates slots or decides scheduling. Those responsibilities stay in `yt/yt/server/node/exec_node`.

## Process entry point and lifecycle

The process entry point is:

- `program.cpp`

`TJobProxyProgram::DoRun()` does early process bootstrap:

1. set thread name
2. optionally close inherited file descriptors
3. configure crash handling, allocator, `SIGPIPE`, pdeathsig, setsid, and memory mapping behavior
4. create stderr file early
5. load config and configure singletons
6. construct `TJobProxy`
7. call `TJobProxy::Run()`

The main runtime lives in:

- `job_proxy.h`
- `job_proxy.cpp`

`TJobProxy::Run()` is a thin wrapper around `DoRun()` with optional fatal-on-uncaught-exception behavior.

### High-level execution sequence

`TJobProxy::DoRun()` delegates actual execution to `RunJob()` on `JobThread_`, then performs teardown and final reporting.

The authoritative startup flow is in `TJobProxy::RunJob()`:

1. create root tracing span for the job
2. optionally initialize TVM bridge and auth helpers
3. create Solomon exporter
4. create `IJobProxyEnvironment`
5. create local RPC servers and register probe/API/Orchid services
6. connect to exec-node supervisor
7. retrieve job spec and initial resource reservation
8. create native cluster client as authenticated user
9. initialize CPU monitor and throttlers
10. optionally enable embedded RPC proxy / shuffle service inside job proxy
11. create concrete `IJob` implementation
12. initialize chunk reader host
13. initialize the job object
14. notify supervisor that job proxy has spawned
15. prepare artifacts
16. notify supervisor that artifacts are prepared
17. start periodic executors: memory watchdog, heartbeat, CPU monitor
18. start sidecars if requested by the spec
19. call `job->Run()`

After `job->Run()` returns or throws, `TJobProxy::DoRun()`:

1. stops heartbeat, memory watchdog, and CPU monitor
2. shuts down sidecars
3. stops local RPC/GRPC/HTTP servers
4. enriches and finalizes job result
5. reports result to supervisor via `OnJobFinished`
6. clears the live `Job_` pointer
7. stops global tracer and logs final system stats

## Main subsystem map

### 1. `TJobProxy`: process-level orchestrator and host interface

Files:

- `job_proxy.h`
- `job_proxy.cpp`
- `job.h`

`TJobProxy` plays two roles:

- process orchestrator for the standalone job proxy process
- host/context implementation for the concrete job object

Important interfaces:

- `IJobHost`: services that job implementations can use
- `IJobProbe`: control/probing surface exposed remotely
- `IJob`: concrete job implementation contract

`TJobProxy` owns or coordinates:

- config, operation id, job id
- job thread and control thread
- supervisor RPC channel/proxy
- local RPC/GRPC/HTTP servers
- native cluster client
- environment object
- current live job object
- throttlers, traffic meter, CPU monitor
- heartbeat and memory watchdog executors
- Orchid root
- job-level block cache / chunk reader host

When you need to understand top-level control flow, start with `TJobProxy::RunJob()`.

### 2. Supervisor boundary: authoritative external control plane

Files:

- `job_proxy.cpp`
- `yt/yt/server/lib/exec_node/supervisor_service_proxy.h`

Exec-node supervisor is the main external authority for a running job proxy.

The job proxy uses it to:

- fetch the job spec via `GetJobSpec`
- notify about `OnJobProxySpawned`
- notify about `OnArtifactsPrepared`
- stream periodic `OnJobProgress` heartbeats
- update resource usage via `UpdateResourceUsage`
- request node-side artifact materialization via `PrepareArtifact`
- report artifact failures via `OnArtifactPreparationFailed`
- report memory thrashing via `OnJobMemoryThrashing`
- report final result via `OnJobFinished`

Important invariant:

- supervisor communication failures are generally fatal and lead to abort, because the process is no longer controllable by exec-node.

### 3. Local control servers: probe/API surface inside the job sandbox

Files:

- `job_prober_service.cpp`
- `job_api_service.cpp`
- `user_job_synchronizer_service.cpp`

`job_proxy` starts private local servers reachable through Unix domain sockets:

- bus RPC server
- optional GRPC server
- optional HTTP server

These are intentionally local to the job environment.

#### `TJobProberService`

Exposes operational probing/control methods backed by `IJobProbe`:

- `DumpInputContext`
- `GetStderr`
- `PollJobShell`
- `Interrupt`
- `GracefulAbort`
- `Fail`
- `DumpSensors`

This is the main external control surface for a live job.

#### `TJobApiService`

Currently exposes `OnProgressSaved`.

This is used by the executor/user-job side to notify the job proxy that durable progress was saved, which then triggers an out-of-band heartbeat.

#### `TUserJobSynchronizerService`

Used only for user jobs.

The executor process calls `ExecutorPrepared(pid)` to notify the job proxy that it has opened/duplicated the expected pipes and is ready. `user_job.cpp` waits on this before considering the executor fully started.

### 4. Job abstraction hierarchy

Files:

- `job.h`
- `job_detail.h`
- `job_detail.cpp`
- `user_job.h`
- `user_job.cpp`
- builtin job files such as `merge_job.cpp`, `partition_job.cpp`, `remote_copy_job.cpp`, `simple_sort_job.cpp`, `sorted_merge_job.cpp`, `shallow_merge_job.cpp`

There are two major kinds of jobs inside `job_proxy`:

- user jobs: execute a separate user-job executor process and communicate through pipes
- built-in jobs: implemented directly in C++ inside job proxy

`IJob` defines the common lifecycle:

- `Initialize()`
- `PrepareArtifacts()`
- `Run()`
- `Cleanup()`
- probe/control methods inherited from `IJobProbe`
- statistics/result helpers

`TJob` in `job_detail.*` is the shared base class.

`TSimpleJobBase` is the base for built-in data-processing jobs that mostly compose chunk readers/writers and do not spawn a user executor.

`TJobProxy::CreateBuiltinJob()` is the authoritative dispatch point for non-user job types.

### 5. User job path

Files:

- `user_job.cpp`
- `user_job.h`
- `stderr_writer.*`
- `memory_tracker.*`
- `tmpfs_manager.*`
- `trace_event_processor.*`
- `trace_consumer.*`
- `core_watcher.*`
- `job_profiler.*`

`user_job.cpp` is the largest and most operationally dense file in this component.

Responsibilities of the user-job implementation include:

- build user-job environment via `IJobHost::CreateUserJobEnvironment`
- prepare stdin/stdout/stderr/profile/trace/core pipes and sandbox layout
- register `TUserJobSynchronizerService`
- spawn the executor process (`ExecProgramName`) inside the chosen runtime environment
- wait for executor readiness callback
- stream stderr/profile/trace data
- track tmpfs and memory usage
- handle interruption, failure, cleanup, and core collection
- assemble result statistics and output metadata

Useful mental model:

- `job_proxy` is the control/monitoring process
- executor is the child process that actually launches the user payload
- user payload may itself run inside a container/rootfs managed by `IUserJobEnvironment`

### 6. Environment abstraction: runtime backend boundary

Files:

- `environment.h`
- `environment.cpp`

This is the main abstraction boundary between generic job-proxy logic and backend-specific runtime/container behavior.

Key interfaces:

- `IJobProxyEnvironment`: process-level environment for the job proxy, sidecars, and shared resource controls
- `IUserJobEnvironment`: user-job runtime handle used by `user_job.cpp`

Capabilities exposed by the environment layer include:

- set CPU guarantee/limit/policy
- create per-job runtime environment
- spawn the user executor process
- enumerate/clean job processes
- retrieve CPU, memory, block I/O, and network statistics
- expose runtime-specific environment variables
- manage rootfs/container details
- manage sidecars

If you need to change how the job proxy or executor is run inside Porto/CRI/rootfs-like environments, start here.

### 7. Artifact preparation path

Files:

- `user_job.cpp`
- `job_proxy.cpp`

Artifact preparation is deliberately split between job proxy and exec-node.

For user jobs, `PrepareArtifacts()` iterates over relevant files from the job spec and materializes them into sandbox paths.

Important mechanism:

1. `user_job.cpp` creates a named pipe
2. it asks `IJobHost::PrepareArtifact(artifactName, pipePath)`
3. `TJobProxy::PrepareArtifact(...)` forwards that request to supervisor
4. exec-node writes artifact bytes into the named pipe
5. job proxy splices pipe contents into the destination file and fixes permissions

Important invariant:

- job proxy does not fetch artifacts directly from storage here; it asks exec-node supervisor to prepare them.

Failure path:

- user-job side catches materialization errors
- `IJobHost::OnArtifactPreparationFailed(...)` forwards them to supervisor

### 8. Chunk I/O and cluster access

Files:

- `job_proxy.cpp`
- `job_detail.*`
- built-in job implementations

After the job spec is retrieved, job proxy creates a native cluster client as the authenticated user from the spec.

`InitializeChunkReaderHost()` builds a `TMultiChunkReaderHost` that:

- supports local and remote input clusters
- optionally respects per-cluster network preferences from the spec
- wires in in-bandwidth throttlers and out-RPS throttlers
- collects chunk reader statistics per cluster

This object is passed implicitly through `IJobHost` to job implementations.

If you are changing cross-cluster input behavior, this is one of the first places to inspect.

### 9. Heartbeats, progress, and final reporting

Files:

- `job_proxy.cpp`
- `job_api_service.cpp`

Progress reporting is centered in `TJobProxy::SendHeartbeat()`.

Heartbeat includes:

- progress fraction
- enriched statistics
- stderr size
- job-trace presence
- last durable progress-save timestamp
- monotonically increasing epoch

Important invariant:

- epochs are used because RPC/network retries can reorder heartbeat delivery.

Final reporting is done in `TJobProxy::ReportResult()` and includes:

- `TJobResult`
- timing information
- enriched statistics
- core infos for user jobs
- fail context if available
- final stderr blob
- job profiles

`FillJobResult()` also patches interruption metadata, failed chunk ids, restart-needed semantics, and restart-exit-code handling.

### 10. Resource accounting and watchdogs

Files:

- `job_proxy.cpp`
- `cpu_monitor.*`
- `memory_tracker.*`
- `tmpfs_manager.*`
- `job_throttler.*`

There are two different resource-accounting scopes to keep in mind:

- job-proxy process accounting
- user-job accounting

#### Job-proxy accounting

`TJobProxy::CheckMemoryUsage()` measures job proxy RSS minus shared pages, tracks max/cumulative usage, compares against reserved memory, and may:

- request larger reserve from supervisor
- adjust `oom_score_adj`
- abort on overdraft depending on config/spec
- fatal if overcommit limit is exceeded

#### User-job accounting

`TMemoryTracker` and `TTmpfsManager` provide user-job memory numbers, either from environment-provided statistics or by enumerating job processes.

User-job memory is used for:

- statistics
- user-job limit enforcement in `user_job.cpp`
- reporting thrashing / overdraft conditions

#### Throttlers

`job_proxy` can create:

- per-cluster in-bandwidth throttlers
- out-bandwidth throttler
- out-RPS throttler
- user-job-container-creation throttler

These are delegated to supervisor-backed job throttling infrastructure when enabled.

### 11. Embedded RPC proxy mode

Files:

- `job_proxy.cpp`
- `signature_proxy.*`

For some user jobs, the spec can request an embedded RPC proxy inside job proxy.

`EnableRpcProxyInJobProxy(...)` then:

- creates a separate native connection from `OriginalClusterConnection`
- starts synchronizers and optional queue-consumer registration manager
- creates API-service worker pool
- installs signature generation/validation tied to supervisor
- optionally starts a public TCP RPC server for shuffle service
- registers YT API service inside the job proxy

Important architectural point:

- this is an optional extension of job proxy into an API-serving process, not the default control path.

### 12. Orchid and observability

Files:

- `job_proxy.cpp`
- `trace_event_processor.*`
- `trace_consumer.*`
- `job_profiler.*`

Observability surfaces include:

- Solomon sensor dump via `DumpSensors()`
- Orchid service under `/job_proxy`
- final profiles attached to result reporting
- optional job trace events written to operations archive
- stderr retrieval through probe service

For tracing, `TJobProxy` creates a root span for the job, and user-job tracing is handled by `TJobTraceEventProcessor` plus `TTraceConsumer`.

## Threading model

The important executors/queues are:

- `JobThread_`: main serialized job lifecycle and heartbeat/memory-watchdog work
- `ControlThread_`: control-plane RPC service invoker
- API-service worker pool: only when embedded RPC proxy is enabled
- user-job-specific auxiliary queues/pools inside `user_job.cpp`

Practical invariant:

- top-level lifecycle methods such as `RunJob()`, heartbeat sending, and memory watchdog execution are expected to be serialized through `JobThread_`.

## Key invariants and failure semantics

- one `job_proxy` process handles exactly one job
- `Job_` may be absent before initialization and after teardown; probe code must handle this
- supervisor connectivity is critical; many failures here lead to immediate abort
- artifact preparation for user jobs is supervisor-mediated, not direct
- local probe/API servers are private and primarily Unix-domain-socket-based
- `OnPrepared()` only marks the job as prepared asynchronously after supervisor acknowledgement
- `Abort()` tries best-effort `job->Cleanup()` before terminating the process
- heartbeat executor may be forced before abort if config requests it
- user processes are not necessarily synchronously killed on every communication failure; container/runtime cleanup is relied on for eventual cleanup

## Where to start for common changes

### Change job startup/teardown semantics

Start with:

- `job_proxy.cpp` (`RunJob()`, `DoRun()`, `Abort()`)

### Change probe/control RPC behavior

Start with:

- `job_prober_service.cpp`
- `job_api_service.cpp`

### Change user-job execution, pipes, stderr, cores, or tracing

Start with:

- `user_job.cpp`
- `memory_tracker.*`
- `stderr_writer.*`
- `trace_event_processor.*`
- `core_watcher.*`

### Change runtime/container behavior

Start with:

- `environment.h`
- `environment.cpp`

### Change built-in job logic

Start with the concrete job file selected by `TJobProxy::CreateBuiltinJob()`.

### Change final result composition or heartbeat payload

Start with:

- `job_proxy.cpp` (`SendHeartbeat()`, `ReportResult()`, `FillJobResult()`, `GetEnrichedStatistics()`)

## Relationship to exec-node

`exec_node` owns slot allocation, job spawning, and high-level lifecycle.

`job_proxy` is the per-job child process that:

- fetches the prepared spec from exec-node
- executes the job
- exposes local job control endpoints
- reports live/final state back to exec-node

A useful rule of thumb:

- if the change affects many jobs on the node, start in `exec_node`
- if the change affects one running job process and its executor/runtime behavior, start in `job_proxy`
