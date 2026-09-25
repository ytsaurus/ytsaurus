# Quick start with {{product-name}} Flow (C++)

In this section, you’ll learn step by step how to implement your first [pipeline](../../../flow/concepts/glossary.md#pipeline) in C++ using Flow. As an example, we’ll walk through a word count task: reading text messages from a queue, splitting them into words, and counting the occurrences of each word.

## Prerequisites

- Check out the [repository]({{source-root}}).
- Set up `ya make` (the build system).
- Familiarize yourself with the [basic concepts](../../../flow/concepts/glossary.md) of Flow.

## Step-by-step guide

### 1. Define message types {#define-messages}

To work with messages in a type-safe way, use `TYsonMessage`, a special subclass of `NYTree::TYsonStruct`. You need to register each message type in the global registry using the `YT_FLOW_DEFINE_YSON_MESSAGE` macro.

```cpp
#include <yt/yt/flow/library/cpp/common/registry.h>

struct TWordMessage
    : public TYsonMessage
{
    std::string Word;

    REGISTER_YSON_STRUCT(TWordMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("word", &TThis::Word)
            .Default();
    }
};

YT_FLOW_DEFINE_YSON_MESSAGE(TWordMessage);
```

For more details on message conversion, see [Process functions (C++)](../../../flow/cpp/process-functions.md).

### 2. Prepare external state {#define-state}

In this example, the counters live in an external {{product-name}} table, and `TWordCountFunction` reads and updates them through `TMutableStateKeyClient<TSimpleExternalState>`. You don't need a separate state class: create the table in step 8 and configure `TSimpleExternalStateManager` in the step 6 spec. For details, see [Working with states (C++)](../../../flow/cpp/state.md).
### 3. Implement a process function for the [Source](../../../flow/concepts/glossary.md#source) {#implement-source}

Implement C++ user logic only as a [process function](../../../flow/cpp/process-functions.md). For element-wise processing, inherit from `IProcessFunction` and implement `ProcessMessage`:

```cpp
class TTextReadFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto text = GetColumnValue<std::string>(message, "text");
        for (const auto& word : StringSplitter(text).SplitBySet(" \t\n\r").SkipEmpty()) {
            auto wordMessage = New<TWordMessage>();
            wordMessage->Word = word;
            output->AddMessage(context->ConvertToMessage(wordMessage));
        }
    }
};

YT_FLOW_DEFINE_PROCESS_FUNCTION(TTextReadFunction);
```

In the spec, the built-in `TProcessFunctionSourceComputation` executes this function. It selects source mode: output messages aren’t materialized in YT, and only recovery metadata is stored. For more details, see [Process functions](../../../flow/cpp/process-functions.md#how-it-works).

### 4. Implement a stateful process function {#implement-transform}

Use `IProcessFunction` for stateful processing as well. To work with external state, use `TSimpleExternalStateManager`:

```cpp
class TWordCountFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        initContext->InitExternalStateClient(StateClient_, "/state");
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    {
        auto state = StateClient_.GetState(message->Key);
        i64 count = state->GetColumnValue<std::optional<i64>>("count").value_or(0);
        TPayloadBuilder builder(state->Schema);
        builder.Set(count + 1, "count");
        state->Payload = builder.Finish();
    }

private:
    TMutableStateKeyClient<TSimpleExternalState> StateClient_;
};

YT_FLOW_DEFINE_PROCESS_FUNCTION(TWordCountFunction);
```

Key points:
- `TMutableStateKeyClient<TState>` is a type-safe client for external state. You set the manager’s parameters in the `Computation` spec (see below), not in your own `TParameters`/`TDynamicParameters`.
- `InitExternalStateClient(StateClient_, "/state")` binds the client to the external state manager named `"/state"`, which is declared in the `external_state_managers` spec.
- In the spec, the built-in `TProcessFunctionComputation` executes the function and provides transform-mode exactly-once state commits.

### 5. Write main.cpp {#write-main}

The `main` function ties all components together:

```cpp
#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    TSimpleSpecBuilder builder;
    builder.RegisterStream<TWordMessage>("words");
    return NYT::NFlow::TSimpleRunnerProgram(std::move(builder)).Run(argc, argv);
}
```

Here:
- `Initialize(argc, argv)` initializes the Flow runtime.
- `TSimpleSpecBuilder` is a builder where you register all type-safe streams. It automatically infers schemas from the registered `TYsonMessage` types.
- `RegisterStream<TWordMessage>("words")` registers the `words` stream with the `TWordMessage` message type.
- `TSimpleRunnerProgram` is a standard runner that handles starting and managing computations.

Full source code for the example:

{% code '/yt/yt/flow/examples/cpp/word_count/main.cpp' lang='cpp' %}

### 6. Describe the pipeline spec {#write-spec}

The spec describes the pipeline topology in YSON format. Here’s an example for word count:

```yson
{
    "spec" = {
        "computations" = {
            "reader" = {
                "computation_class_name" = "NYT::NFlow::TProcessFunctionSourceComputation";
                "processing_function" = "NYT::NFlow::NExample::TTextReadFunction";
                "output_stream_ids" = ["words"];
                "source_streams" = {
                    "queue" = {
                        "source_class_name" = "NYT::NFlow::TQueueSource";
                        "parameters" = {
                            "queue_path" = "<cluster=cluster_name>//path/to/queue";
                            "consumer_path" = "<cluster=cluster_name>//path/to/consumer";
                        };
                    };
                };
            };
            "counter" = {
                "computation_class_name" = "NYT::NFlow::TProcessFunctionComputation";
                "processing_function" = "NYT::NFlow::NExample::TWordCountFunction";
                "input_stream_ids" = ["words"];
                "output_stream_ids" = [];
                "group_by_schema" = [
                    {"name" = "hash"; "type" = "uint64"; "expression" = "farm_hash(word)";};
                    {"name" = "word"; "type" = "string";};
                ];
                "external_state_managers" = {
                    "/state" = {
                        "external_state_manager_class_name" = "NYT::NFlow::TSimpleExternalStateManager";
                        "parameters" = {
                            "path" = "//path/to/state/table";
                        };
                    };
                };
            };
        };
    };
}
```

You don’t need to fill the `streams` section when you use `TYsonMessage` and `TSimpleSpecBuilder`; the stream information is inferred automatically.

For more details on the spec format, see [Spec & DynamicSpec](../../../flow/concepts/spec.md).

### 7. Build the project {#build}

The code fragments above explain the checked-in Word Count example; they aren't a standalone project. Build the example from the repository. Its dependencies are listed in [ya.make]({{source-root}}/yt/yt/flow/examples/cpp/word_count/ya.make):

```bash
ya make yt/yt/flow/examples/cpp/word_count
```

The binary is `yt/yt/flow/examples/cpp/word_count/word_count`. The simplified step 6 spec omits `min_word_length`, so the default `0` applies; the checked-in config sets it to `4`. If you move the process functions into another namespace, use their fully qualified names in the spec's `processing_function` fields.

### 8. Create objects in YT {#create-yt-objects}

Before you run the pipeline, you need to create:
- An input queue (if it doesn’t exist yet).
- A consumer for the input queue, registered on it.
- A state table (for `ExternalState`).
{% if audience == "internal" %}- A pipeline object with [Flow inner tables](../../../flow/concepts/glossary.md#inner-pipeline-tables).{% endif %}

{% if audience == "internal" %}To create the objects, use the [YtSync]({{yt-sync-docs}}/) utility (the pipeline spec is described [here]({{yt-sync-docs}}/pipeline_specification)).{% else %}For a working example of creating the queue, consumer, and state table, see [Prepare the pipeline](../../../flow/quickstart.md#prepare) in the quick start. The C++ runner creates the pipeline and internal tables on first launch; for a separate preparation step, see [Creating a pipeline](../../../flow/concepts/pipeline-object.md#create).{% endif %}

### 9. Run and test {#run-and-test}

{% if audience == "internal" %}Run the pipeline and monitor its operation in the {{product-name}} UI, following the path of your `pipeline`.{% else %}Save the YSON from step 6 as `<config.yson>`. Replace the queue, consumer, and external state table paths, then add `cluster_url`, the pipeline `path`, and a `vanilla` block with `enable = %true`, your scheduler `pool`, and `worker = {"count" = 1;}`, as shown in [Prepare the pipeline](../../../flow/quickstart.md#prepare). Launch the built binary with `--config <config.yson>` as in [Validate and launch](../../../flow/quickstart.md#launch). Check the state with `yt --proxy <cluster> flow get-pipeline-state <pipeline_path>`, where `<pipeline_path>` is the `path` value from your config; the CLI should print `working`.{% endif %}

Before you release a new version, read [Release and table change rules](../../../flow/devops/vanilla/releases.md#release-and-configure-basic-rules). For lifecycle commands, see [Pipeline operations](../../../flow/devops/vanilla/pipeline-operations.md).

## See also

- [Process functions (C++)](../../../flow/cpp/process-functions.md)
- [Computation modes (C++)](../../../flow/cpp/computation.md)
- [Working with states (C++)](../../../flow/cpp/state.md)
- [Watermarks](../../../flow/concepts/watermarks.md)
- [Timers](../../../flow/concepts/timers.md)
- [Spec & DynamicSpec](../../../flow/concepts/spec.md)
{% if audience == "internal" %}- [Logbroker WaitClickJoin (C++)](../../../yandex-specific/flow/cpp/examples/lb_wait_click_join.md){% endif %}
