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

Create a static BLOB table, put its rich path or a link to it into `pipeline-yt-file.yson`, configure
`cluster_url`, the pipeline `path`, queue source, and sink for an existing Flow deployment, and run
the binary. The exact table schema is documented in the Flow file-resource guide; its rows have the
key `(filename, part_index)` and the payload column `data`:

```bash
# Create //path/to/config-files-revisions/001 with the strict, unique-key BLOB-table schema first.
echo '{filename="config-file";part_index=0;data="first";}' | \
    yt write-table --format yson //path/to/config-files-revisions/001
yt link //path/to/config-files-revisions/001 //path/to/config-files
./file_resource --config pipeline-yt-file.yson
```

One BLOB table is one immutable revision of the complete file set. To publish another revision,
create another table and atomically repoint the link. After `file_source_discover_period`, workers
materialize all files from the new table while keeping the prior snapshot available until the new
one has been streamed and validated:

```bash
echo '{filename="config-file";part_index=0;data="second";}' | \
    yt write-table --format yson //path/to/config-files-revisions/002
yt link --force //path/to/config-files-revisions/002 //path/to/config-files
```

File-source implementations can also expose typed dynamic parameters under
`dynamic_spec.resources.<resource>.file_sources.<name>.parameters`. For example,
`TYTDirectoryLastFileSource` treats each direct child BLOB table as a complete file-set revision and
normally selects the lexicographically greatest child name. Its `pinned_file_name` parameter selects
one exact child table instead. Changing a pin triggers discovery immediately; workers still
materialize the exact revision delivered in the resource target snapshot.

Configuration snippets for all built-in file sources and the persistent-cache policy are in the
Flow file-resource documentation.
