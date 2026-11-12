# Running C++ process functions in a companion

The C++ companion SDK hosts [process functions](process-functions.md) in a separate process managed by a Flow worker. It is useful when your user code needs a separate executable and address space. Build the companion separately from the worker and the runner; the worker sends batches to it through the [companion protocol](../concepts/companion.md).

This reference assumes you already have a C++ process function and know how to configure and deploy a pipeline. The maintained [resource companion executable]({{source-root}}/yt/yt/flow/tests/companion/resource/cpp/companion/main.cpp), its [build file]({{source-root}}/yt/yt/flow/tests/companion/resource/cpp/companion/ya.make) and [pipeline spec]({{source-root}}/yt/yt/flow/tests/companion/resource/cpp/pipeline.yson) show the registration and wiring together.

## Register hosted functions and resources {#registration}

Use `NYT::NFlow::NCompanionServer::TPipeline` from `yt/yt/flow/library/cpp/companion/server/pipeline.h`. Populate it before starting the server:

- `AddTransform<TFunction, TStaticParameters, TDynamicParameters>(computationId)` declares a hosted transform computation and registers its process function and parameter schemas.
- `AddSource<TFunction, TStaticParameters, TDynamicParameters>(computationId)` declares a hosted source computation. Both parameter types default to `TEmptyProcessFunctionParameters`.
- `AddResource<TResource>()` registers a companion-hosted resource class. Resource ids are declared in the spec, not in this call.

The computation id must match the spec. The spec's `processing_function` still selects the registered function by its C++ type name. Typed function registration replaces `YT_FLOW_DEFINE_PROCESS_FUNCTION` for that declaration; `AddResource` replaces `YT_FLOW_DEFINE_RESOURCE`. Avoid duplicate registration through both mechanisms. Duplicate computation ids are rejected.

Pass the populated pipeline to `NYT::NFlow::NCompanionServer::RunCompanionMain(argc, argv, std::move(pipeline))`, declared in `companion_main.h`. The executable's `ya.make` must depend on `yt/yt/flow/library/cpp/companion/server`; add the dependencies required by your functions and resources. The maintained build file above contains the complete dependency list for its example.

## Connect the executable to the worker {#wiring}

1. Make the companion executable and its runtime dependencies available in the worker job environment. Use the existing [deployment instructions](../devops/vanilla/initial-deploy.md) for the worker; a path in the spec does not upload the executable by itself.
1. Declare a `NYT::NFlow::NCompanion::TCompanionManager` resource. In its `parameters.entrypoint`, set `executable` to the executable's absolute path in that environment. `args` supplies command-line arguments and `env` supplies additional environment variables.
1. For each hosted computation, select a [companion computation class](../concepts/companion.md#computation-types) in `computation_class_name`, set `processing_function`, and require the manager on the worker under alias `CompanionManager` (`worker = %true`, `controller = %false`). Keep the computation id consistent with `AddTransform` or `AddSource`.
1. When using the standard `flow_server` runner with function types registered only in the companion, set the runner configuration's `abort_on_specs_parseability_error` to `%false`. The runner cannot parse a function type absent from its own registry. The companion executable must still register the selected function and parameter schemas correctly.

The [C++ companion test harness]({{source-root}}/yt/yt/flow/library/python/integration_test_base/yt_flow_cpp_base.py) shows this separation: `flow_server` acts as the runner, while the manager entrypoint points to the C++ executable. Its integration test checks output that includes values loaded in the serving companion process.

## Startup and execution boundaries {#execution}

`RunCompanionMain` starts the companion server. It requires worker-supplied `YT_FLOW_MODE=Worker` and `YT_FLOW_COMPANION_CONFIG` containing a positive RPC `port`. Use the runner to launch the pipeline; the companion entry point does not implement the runner's pipeline-launch command.

The C++ companion uses one multithreaded process. Make shared objects safe for concurrent use.

The supported interfaces and their boundaries are described in [C++ companion limitations](../concepts/companion.md#cpp-companion). Select the hosting mode explicitly rather than assuming every native process-function facility is available out of process.

## Companion-hosted resources and HTTP clients {#resources-and-http}

Register hosted resource classes with `AddResource<TResource>()` and connect worker proxies using [Companion resource wiring](../concepts/companion.md#companion-resources). In a process function's `Init`, `IRuntimeInitContext::GetStaticResource(alias)` returns the bound companion-side resource. The [maintained mapper]({{source-root}}/yt/yt/flow/tests/companion/resource/cpp/companion/main.cpp) demonstrates this path. A resource that exists only in the worker is not transferred into the companion automatically.

`IRuntimeInitContext::GetHttpClient()` and `GetHttpsClient()` provide shared HTTP clients. Configure them through `http_client_config`, `https_client_config` and `http_poller_threads` in the worker's `companion` block; see [TCompanionConfig](../generated_docs/all_yson_structs.md#NYT_NFlow_NCompanion_TCompanionConfig).

## Source and tests {#source-and-tests}

- [Companion server SDK]({{source-root}}/yt/yt/flow/library/cpp/companion/server).
- [Registration tests]({{source-root}}/yt/yt/flow/library/cpp/companion/server/unittest/pipeline_ut.cpp) and [startup configuration tests]({{source-root}}/yt/yt/flow/library/cpp/companion/server/unittest/config_ut.cpp).
- [Resource integration test]({{source-root}}/yt/yt/flow/tests/companion/resource/cpp/test_resource.py) and [shared lifecycle scenario]({{source-root}}/yt/yt/flow/tests/companion/resource/common/companion_resource_test_base.py), covering dependency values, dynamic updates and recovery after companion restart.
