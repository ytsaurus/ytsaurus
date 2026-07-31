# File resource example

The example binary defines a text-backed `TFileResourceBase<TTextData>` and a materialized
transform that joins every input message with the current text snapshot. A separate Swift reader
only forwards queue rows and does not access the mutable resource. The transform keeps `Lock()`
only long enough to copy the text, so a rollout does not retain an old cached object for the rest
of message processing.

A mutable file resource must not be read from Swift user logic: replay after a restart could use a
different file revision and violate Swift's determinism requirement. Use a materialized transform,
as this example does, or use a source whose object identity is immutable for the whole deployment.

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
revision, and file name. An in-place overwrite publishes a new revision after `discover_period`;
workers keep serving the prior snapshot until they have streamed and validated the new bytes:

```bash
echo second | yt write-file //path/to/config-file
```

Configuration snippets for all built-in file sources and the persistent-cache policy are in the
Flow file-resource documentation.
