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

For more details on message conversion, see the [Computation (C++)](../../../flow/cpp/computation.md) section, “TYsonMessage”.

### 2. Define the state {#define-state}

If a [computation](../../../flow/concepts/glossary.md#stream-and-computation) works with a [state](../../../flow/concepts/glossary.md#state), define a class that inherits from `TStateBase`:

```cpp
struct TWordCountState
    : public TStateBase
{
    i64 Count{};

    REGISTER_YSON_STRUCT(TWordCountState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("count", &TThis::Count)
            .Default(0);
    }
};
```

For more details on working with states, see [Working with states (C++)](../../../flow/cpp/state.md).

### 3. Implement the [Source](../../../flow/concepts/glossary.md#source) Computation {#implement-source}

To read data from external sources, inherit from `TSwiftOrderedSourceComputation`. In the `DoProcessMessage` method, transform the input messages:

```cpp
class TTextReader
    : public TSwiftOrderedSourceComputation
{
public:
    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        auto text = GetColumnValue<std::string>(message, "text");
        for (const auto& word : StringSplitter(text).SplitBySet(" \t\n\r").SkipEmpty()) {
            auto wordMessage = New<TWordMessage>();
            wordMessage->Word = word;
            output->AddMessage(ConvertToMessage(wordMessage));
        }
    }
};

YT_FLOW_DEFINE_COMPUTATION(TTextReader);
```

Note that `TSwiftOrderedSourceComputation` doesn’t materialize the messages themselves in YT; it only stores metadata for recovery. For more details, see [Computation (C++)](../../../flow/cpp/computation.md#tswiftorderedsourcecomputation).

### 4. Implement the Transform Computation {#implement-transform}

To process data with state, inherit from `TTransformComputation`. To work with external states, use `TSimpleExternalStateManager`:

```cpp
class TWordCounter
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitExternalStateClient(StateClient_, "/state");
    }

    void DoProcessMessage(
        const TInputMessageConstPtr& message,
        IOutputCollectorPtr /*output*/) override
    {
        const auto wordMessage = ConvertToYsonMessage<TWordMessage>(message);
        auto state = StateClient_.GetState(message->Key);
        i64 count = state->GetColumnValue<std::optional<i64>>("count").value_or(0);
        TPayloadBuilder builder(state->Schema);
        builder.Set(count + 1, "count");
        state->Payload = builder.Finish();
    }

private:
    TMutableStateKeyClient<TSimpleExternalState> StateClient_;
};

YT_FLOW_DEFINE_COMPUTATION(TWordCounter);
```

Key points:
- `TMutableStateKeyClient<TState>` is a type-safe client for external state. You set the manager’s parameters in the `Computation` spec (see below), not in your own `TParameters`/`TDynamicParameters`.
- `InitExternalStateClient(StateClient_, "/state")` binds the client to the external state manager named `"/state"`, which is declared in the `external_state_managers` spec.
- `ConvertToYsonMessage<T>` converts input messages into a type-safe structure.

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
    computations = {
        text_reader = {
            computation_ref = "TTextReader";
            outputs = ["words"];
            sources = {
                source = {
                    type = "TQueueSource";
                    parameters = {
                        queue_path = "//path/to/input/queue";
                    };
                };
            };
            watermark_strategy = {
                watermark_generator = {
                    out_of_orderness_bound = "10s";
                };
            };
        };
        word_counter = {
            computation_ref = "TWordCounter";
            inputs = ["words"];
            group_by_schema = [
                {name = "hash"; type = "uint64"; expression = "farm_hash(word)"};
                {name = "word"; type = "string"};
            ];
            external_state_managers = {
                "/state" = {
                    external_state_manager_class_name = "NYT::NFlow::TSimpleExternalStateManager";
                    parameters = {
                        path = "//path/to/state/table";
                    };
                };
            };
        };
    };
    streams = {};
}
```

You don’t need to fill the `streams` section when you use `TYsonMessage` and `TSimpleSpecBuilder`; the stream information is inferred automatically.

For more details on the spec format, see [Spec & DynamicSpec](../../../flow/concepts/spec.md).

### 7. Build the project {#build}

Add the dependencies to your project’s `ya.make` and build it:

```bash
ya make path/to/your/project
```

### 8. Create objects in YT {#create-yt-objects}

Before you run the pipeline, you need to create:
- An input queue (if it doesn’t exist yet).
- A state table (for `ExternalState`).
- A pipeline object with [Flow inner tables](../../../flow/concepts/glossary.md#inner-pipeline-tables).

{% if audience == "internal" %}To create the objects, use the [YtSync]({{yt-sync-docs}}/) utility (the pipeline spec is described [here]({{yt-sync-docs}}/pipeline_specification)).{% endif %}

### 9. Run and test {#run-and-test}

Run the pipeline and monitor its operation in the {{product-name}} UI, following the path of your `pipeline`.

For detailed information about releases and pipeline management, read the [Releases and pipeline management](../../../flow/release/basic-rules.md) section.

## See also

- [Computation (C++)](../../../flow/cpp/computation.md)
- [Working with states (C++)](../../../flow/cpp/state.md)
- [Watermarks](../../../flow/concepts/watermarks.md)
- [Timers](../../../flow/concepts/timers.md)
- [Spec & DynamicSpec](../../../flow/concepts/spec.md)
{% if audience == "internal" %}- [Logbroker WaitClickJoin (C++)](../../../yandex-specific/flow/cpp/examples/lb_wait_click_join.md){% endif %}