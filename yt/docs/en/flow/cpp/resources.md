# Resources

A resource is an object shared by all jobs in one Flow process. A computation can use a resource
to access an external-system client, reference data, a model, or another object that would be
expensive to create for every message.

Declare resources in the `resources` section of the pipeline spec. Computations list the resources
they need in `required_resource_ids`. One resource instance is created per worker process. Keep the
constructor lightweight and perform long-running initialization in `Load()`.

The examples below show only resource-related fragments of classes and the pipeline spec. See the
[getting-started guide](getting-started.md) for a complete pipeline project and build commands.

A user-defined resource usually derives from `TResourceBase` and is registered with
`YT_FLOW_DEFINE_RESOURCE`:

```cpp
class TMyResource
    : public TResourceBase
{
public:
    using TResourceBase::TResourceBase;

    TFuture<void> Load(
        const THashMap<TResourceId, IResourcePtr>& dependencies) override;
};

YT_FLOW_DEFINE_RESOURCE(TMyResource);
```

Specify class parameters in `resources.<name>.parameters`; they are available through
`GetParameters()`. Declare dependencies on other resources in `resources.<name>.dependencies`.

## Where to load a resource

For each requirement, independently specify whether the resource is needed on workers and on the
controller:

```yson
required_resource_ids = {
    geobase = {
        worker = %true;
        controller = %false;
    };
};
```

Most resources that hold user data are needed only on workers. Load a resource on the controller
only if it is required by computation logic that actually runs there. This restriction also
applies to resource dependencies.

## Resource controller

A resource needs a controller-side component when Flow must track an external data version and
tell workers which version to use. For files, this logic is already implemented by
`TResourceControllerBase`.

Define a custom controller to add resource-specific logic:

```cpp
class TMyResourceController
    : public TResourceControllerBase
{
public:
    using TResourceControllerBase::TResourceControllerBase;

protected:
    NYTree::INodePtr DoBuildTargetRevisionSpec() override;
    void DoCollectStatuses(
        const THashMap<std::string, TWorkerResourceStatusPtr>& workerStatuses,
        const TWorkerResourceStatusPtr& controllerStatus) override;
    NYTree::IMapNodePtr DoGetView() override;
};

class TMyResource
    : public TResourceBase
{
public:
    using TController = TMyResourceController;
    using TResourceBase::TResourceBase;
};
```

The controller publishes a target resource revision. Workers receive it through regular
reconfiguration and report which revision they have applied. This lets the controller distinguish
between a target that has not been delivered yet and a delivered target that is still being
prepared.

`DoCollectStatuses()` receives resource states from live workers, while `DoGetView()` builds the
resource section of the Flow view. If custom state must survive a controller restart, persist it
through the `DoInit()` context.

## File-backed resources

A file-backed resource is suitable for reference data, models, and other immutable structures that
can be built from one or more files. Declare sources as the named `file_sources` map next to the
ordinary resource parameters:

```yson
resources = {
    geobase = {
        resource_class_name = "TGeobaseResource";
        parameters = {
            format = "binary";
        };
        file_sources = {
            countries = {
                file_source_class_name = "NYT::NFlow::TYTFileSource";
                parameters = {
                    path = "<cluster=primary>//models/countries";
                };
            };
            cities = {
                file_source_class_name = "NYT::NFlow::TYTFileSource";
                parameters = {
                    path = "<cluster=primary>//models/cities";
                };
            };
        };
    };
};
```

Such a resource must be loaded only on workers: set `controller = %false` in every reachable
requirement. An invalid spec is rejected before the pipeline starts.

### Implementing the resource

For the standard workflow, derive from `TFileResourceBase<TData>` and implement `Initialize()`.
Override `Validate()` when the structure needs additional validation:

```cpp
class TGeobaseResource
    : public TFileResourceBase<TGeobase>
{
protected:
    TGeobasePtr Initialize(
        const TMaterializedFileSourceSnapshotPtr& files) override
    {
        const auto& countries = files->GetFileSource(TFileSourceId("countries"));
        const auto& cities = files->GetFileSource(TFileSourceId("cities"));
        return LoadGeobase(countries->GetRootPath(), cities->GetRootPath());
    }

    void Validate(const TGeobasePtr& geobase) override;
};
```

Flow discovers an exact revision for every named source and combines them into one snapshot. On
each worker, the snapshot is downloaded, initialized, and validated in order. New data becomes
available only after all three steps succeed for every file. If a step fails, the resource retries
after `file_source_update_retry_period`. During initial loading, `Load()` remains pending until a
retry succeeds or a replacement target arrives. During an update, the resource keeps serving the
previous valid version.

The controller keeps an active snapshot and the next snapshot being prepared. A new worker first
loads the active snapshot and then prepares the next one. The controller admits the preparing
snapshot after one current resource instance on the current target revision validates it. This is
a canary admission check, not an all-worker barrier or quorum. After admission, workers converge
asynchronously and may complete the switch at different times, but each worker receives the same
set of exact file revisions.

If a user-defined resource does not need the standard preparation order, `TResourceBase` also
provides the protected `MaterializeFileSource()` and `MaterializeFileSources()` methods for
downloading named files from an already delivered target revision.

### Reading data

`Lock()` returns a `TFileResourceAccessor<TData>` that pins one data version. An accessor that has
already been returned does not change when the resource switches versions.

Acquire one accessor per `RunIteration` or per batch-function call, and release it before the next
iteration. A long-lived accessor retains the old version and prevents the switch from completing.
If the wait exceeds `file_snapshot_rollout_warning_period`, the worker reports
`/file_snapshot_activation`; an issued accessor cannot be revoked forcibly.

Do not read a mutable file-backed resource from user logic in a Swift computation. When an epoch
is retried, the computation could observe a different file version and become nondeterministic.
Use a materializing transformation such as `TTransformComputation`, or use a source that is
guaranteed not to change during the entire run.

### Pinning a version

A source may define dynamic parameters. For example, `TYTDirectoryLastFileSource` can temporarily
select a specific table revision instead of the latest one:

```yson
dynamic_spec = {
    resources = {
        geobase = {
            file_sources = {
                release = {
                    parameters = {
                        pinned_file_name = "000001";
                    };
                };
            };
        };
    };
};
```

After the parameters change, the controller discovers revisions again. Workers keep using the
previous snapshot until a complete new set has been discovered successfully.

## Built-in file sources

### Immutable local file

`TLocalFileSource` is intended primarily for tests and environments where the same absolute path
refers to the same file on every worker:

```yson
file_sources = {
    file = {
        file_source_class_name = "NYT::NFlow::TLocalFileSource";
        parameters = {
            path = "/absolute/path/visible/to/every/worker/data.bin";
        };
    };
};
```

The file is treated as immutable. Do not replace its contents in place; publish a new version at a
new path.

### Files in a {{product-name}} BLOB table

`TYTFileSource` materializes all files from one static sorted BLOB table. `path` may point either
to the table itself or to a link to it. The table must have exactly this strict, unique-key schema:

```yson
<strict=%true;unique_keys=%true>[
    {name="filename";type="string";sort_order="ascending";};
    {name="part_index";type="int64";sort_order="ascending";};
    {name="data";type="string";};
]
```

Every filename becomes a regular file in the materialized root:

```yson
file_sources = {
    model = {
        file_source_class_name = "NYT::NFlow::TYTFileSource";
        parameters = {
            path = "<cluster=primary>//path/to/current-model-files";
        };
    };
};
```

Every file starts at part index zero and part indexes are consecutive. Filenames must be single
path components.

One table represents one immutable revision of the complete file set. To publish the next revision,
create a new table and atomically repoint the link configured in `path`. Do not modify an already
published table.

The controller snapshot-locks the object referenced by `path` and identifies a revision by cluster,
table object ID, and content revision. Workers lock and verify that exact table before streaming
its rows.

### Latest BLOB table in a {{product-name}} directory

`TYTDirectoryLastFileSource` treats every immediate directory child as a separate revision of the
complete file set. It selects the child with the lexicographically greatest name and materializes
all files from the selected BLOB table:

```yson
file_sources = {
    release = {
        file_source_class_name = "NYT::NFlow::TYTDirectoryLastFileSource";
        parameters = {
            path = "<cluster=primary>//path/to/releases";
        };
    };
};
```

Add new immutable tables under lexicographically sortable timestamp names such as
`2026-08-31T07:00:00Z` and `2026-08-31T08:00:00Z`. The dynamic
`pinned_file_name` parameter selects one exact child table name.

## Worker disk cache

Every worker that loads file-backed resources needs `worker.file_storage`:

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

`path` is the exact cache root for one process. Flow does not append a pipeline or worker ID. The
process holds the `<path>/.lock` lock and refuses to start if another process already uses the same
directory. Tests and environments with a shared file system must therefore assign a separate path
to every worker that runs concurrently.

Successfully downloaded versions survive resource recreation and can be reused after a worker
restart if the volume and path are preserved. Changing unpacking code does not download the source
file again: only `Initialize()` and `Validate()` run again. Required system tools such as `tar`
must be available in the worker environment.

The cache evicts only resource versions that are not currently in use, following LRU order.
`soft_size_limit` is the target size after cleanup; `hard_size_limit` is the admission boundary for
new data. It is not a physical volume quota: leave space for metadata and downloads whose size is
not known in advance. If pinned data occupies more than half of the hard limit, the component
reports a warning.

The resource status shows file preparation progress. A discovery error identifies the source.
Download, initialization, and validation errors identify the resource, the snapshot, and the
revisions of its sources. Snapshot and individual-revision state distributions are reported
through the `/resource_controller/file_snapshot_instance_count` and
`/resource_controller/file_source_revision_instance_count` metrics. Cache state and insufficient-
space errors are reported under `/file_storage`.
