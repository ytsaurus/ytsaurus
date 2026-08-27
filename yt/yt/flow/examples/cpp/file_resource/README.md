# File resource example

The example binary defines a text-backed `TFileResourceBase<TTextData>` and a materialized
transform that joins every input message with the current text snapshot. A separate Swift reader
only forwards queue rows and does not access the mutable resource. The transform is a batch process
function: it calls `Lock()` once for the whole epoch input and releases the accessor when the batch
returns. Do not retain an accessor across run iterations; it keeps the old snapshot alive and
blocks activation until it is released. After `file_snapshot_rollout_warning_period`, the worker
reports this condition as `/file_snapshot_activation` without forcibly revoking the accessor.

A mutable file resource must not be read from Swift user logic: replay after a restart could use a
different file revision and violate Swift's determinism requirement. Use a materialized transform,
as this example does, or use a source whose object identity is immutable for the whole deployment.

The resource spec declares the source as `file_sources.file`, alongside the resource's ordinary
`parameters`. The controller discovers an exact revision for every declared name and delivers one
target snapshot; workers materialize that target rather than discovering "latest" again. A
multi-file resource can override `Initialize(TMaterializedFileSourceSnapshotPtr)` and read each
named root with `GetFileSource(name)`. Initialization, validation, and publication cover the whole
snapshot atomically.

Named files are materialized only on workers. Every computation requirement that can reach this
resource must set `controller = %false`; the pipeline controller rejects a file-source-backed
resource in its own resource-loading graph.

Build the binary:

```bash
ya make yt/yt/flow/examples/cpp/file_resource
```

Every worker that may load a file resource needs persistent storage:

```yson
worker = {
    file_storage = {
        path = "/absolute/dedicated/file-resource-cache";
        soft_size_limit = 1073741824;
        hard_size_limit = 1342177280;
        cleanup_period = "5m";
    };
};
```

The directory is always the exact cache root for one worker process; Flow does not append a
pipeline, operation, or job-cookie component. The worker holds an exclusive nonblocking lock on
`<path>/.lock` for its lifetime and fails at startup if another process owns the same physical
root. A deployment or test harness must therefore configure a distinct path for every concurrent
worker on a shared filesystem. A private vanilla-job sandbox isolates the path but does not make it
persistent across allocation or host changes; persistence requires an externally provisioned
worker-local volume. Operators should size the soft limit for the normal cache working set and
leave headroom up to the hard limit for pinned objects and atomic fills.

Create a YT file, put its rich path into `pipeline-yt-file.yson`, configure `cluster_url`, the
pipeline `path`, queue source, and sink for an existing Flow deployment, and run the binary:

```bash
yt create file //path/to/config-file
echo first | yt write-file //path/to/config-file
./file_resource --config pipeline-yt-file.yson
```

`TYTFileSource` derives an immutable storage object id from the cluster, YT object id, node
revision, and file name. An in-place overwrite publishes a new revision after
`file_source_discover_period`;
workers keep serving the prior snapshot until they have streamed and validated the new bytes:

```bash
echo second | yt write-file //path/to/config-file
```

File-source implementations can also expose typed dynamic parameters under
`dynamic_spec.resources.<resource>.file_sources.<name>.parameters`. For example,
`TYTDirectoryLastFileSource` accepts `pinned_file_name` to select one exact direct child instead
of the lexicographically greatest file. Changing a pin triggers discovery immediately; workers
still materialize the exact revision delivered in the resource target snapshot.

Configuration snippets for all built-in file sources and the persistent-cache policy are in the
Flow file-resource documentation.
