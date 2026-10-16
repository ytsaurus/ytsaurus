# Shuffle in {{product-name}} Flow (C++)

You use the [pipeline]({{source-root}}/yt/yt/flow/examples/cpp/shuffle) to read a JSON stream from a sorted dynamic table, group it multiple times by different keys, and then count the number of unique keys in all resulting streams. A [test]({{source-root}}/yt/yt/flow/examples/cpp/shuffle/test/test_shuffle.py) is also described for the pipeline.

This pipeline doesn’t aim to solve any business problem.

## General pipeline overview

### Reader. Reading input data

The first computation is `Reader`. It reads and performs initial transformation of data from the queue. Here are the parts of the [spec](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec) related to this computation:

```yson
{
    "spec" = {
        "computations" = {
            "reader" = {
                "computation_class_name" = "NYT::NFlow::TProcessFunctionSourceComputation";
                "processing_function" = "NYT::NFlow::NExample::TQueueReader";
                "output_stream_ids" = ["event"];
                "source_streams" = {
                    "queue" = {
                        "source_class_name" = "NYT::NFlow::TQueueSource";
                        "parameters" = {
                            "queue_path" = "<cluster=cluster_name>//path/to/queue";
                            "consumer_path" = "<cluster=cluster_name>//path/to/consumer";
                            "finite" = false;
                        };
                    };
                };
            };
        };
        "streams" = {
            "event" = {
                "schema" = [
                    {"name" = "key_a"; "type" = "uint64";};
                    {"name" = "key_b"; "type" = "uint64";};
                    {"name" = "key_c"; "type" = "uint64";};
                    {"name" = "key_d"; "type" = "uint64";};
                    {"name" = "value"; "type" = "string";};
                ];
            }
        };
    };
}
```

Let’s break it down.

- `computations/reader/source_streams` contains the `queue` source with the type `NYT::NFlow::TQueueSource`. This `source` reads data from a sorted dynamic table using a `consumer`. In `parameters`, you specify which queue to read from and which consumer to use. You can learn more about the parameters in the `NYT::NFlow::TQueueSourceParameters` class.
- Use `NYT::NFlow::TQueueSourceController` to manage the `Computation`, because you need to define the number and settings of the [partitions](../../../../flow/concepts/glossary.md#partition) based on the input sorted dynamic table.
- `streams` contains a single `event` flow — the parsed flow output from `reader`, which is available to other `Computation` components. It has an associated schema. This same flow is also registered in `computations/reader/output_stream_ids`.
- The `TQueueReader` class implements `IProcessFunction`; the `TProcessFunctionSourceComputation` named in `computation_class_name` hosts it. A built-in passthrough isn’t suitable here because parsing `JSON` requires a custom `ProcessMessage` implementation.

{% code '/yt/yt/flow/examples/cpp/shuffle/lib/shuffle_functions.cpp' lang='cpp' lines='[BEGIN example_shuffle_queue_reader]-[END example_shuffle_queue_reader]' %}

- The source adapter runs in Swift mode: it doesn’t materialize output streams in {{product-name}} and saves only the metadata required for deterministic operation.
- `TQueueSource` and the adapter manage queue access, including access to a remote cluster. The process function receives an already-read message and doesn’t access a {{product-name}} client directly.

### Shuffle

The pipeline includes several shuffles: `shuffle_a`, `shuffle_b`, `shuffle_c`, and `shuffle_d`. Each of them groups the input stream by the corresponding key — `key_a`, `key_b`, `key_c`, or `key_d`. They don’t transform the data; they just demonstrate the ability to group different objects.

Let’s examine the spec for `shuffle_b`:

```yson
{
    "spec" = {
        "computations" = {
            "shuffle_b" = {
                "computation_class_name" = "NYT::NFlow::TSwiftPassthroughComputation";
                "group_by_schema" = [
                    {"name" = "hash"; "expression" = "farm_hash(key_b)"; "type" = "uint64";};
                    {"name" = "key_b"; "type" = "uint64";};
                ];
                "input_stream_ids" = ["event_a"];
                "output_stream_ids" = ["event_b"];
            };
        };
        streams = {
            "event_a" = {
                "schema" = [
                    {"name" = "key_a"; "type" = "uint64";};
                    {"name" = "key_b"; "type" = "uint64";};
                    {"name" = "key_c"; "type" = "uint64";};
                    {"name" = "key_d"; "type" = "uint64";};
                    {"name" = "value"; "type" = "string";};
                ];
            };
            "event_b" = {
                "schema" = [
                    {"name" = "key_a"; "type" = "uint64";};
                    {"name" = "key_b"; "type" = "uint64";};
                    {"name" = "key_c"; "type" = "uint64";};
                    {"name" = "key_d"; "type" = "uint64";};
                    {"name" = "value"; "type" = "string";};
                ];
            };
        };
    };
}
```

- Since this example doesn’t include any data transformation, `NYT::NFlow::TSwiftPassthroughComputation` is sufficient. If transformation is required, implement the custom logic as `IProcessFunction` and host it with `NYT::NFlow::TProcessFunctionSwiftMapComputation`.
- `NYT::NFlow::TSwiftPassthroughComputation` doesn’t materialize data in {{product-name}}.
- `group_by_schema` contains the corresponding `key_b` key. The `hash` column is added, because partitioning in `Flow` works only under the assumption that the first column contains uniformly distributed `uint64` values.
- `input_stream_ids` and `output_stream_ids` contain `event_a` and `event_b`, respectively.
- `spec/streams` also contains `event_a` and `event_b` with the full schema description.

### Reduce

This is the final `Computation`. It reads the `event_a`, `event_b`, `event_c`, and `event_d` streams and counts how many times each `value` appears. In effect, the original stream is processed four times.

```yson
{
    "spec" = {
        "computations" = {
            "reducer" = {
                "computation_class_name" = "NYT::NFlow::TProcessFunctionComputation";
                "processing_function" = "NYT::NFlow::NExample::TReducer";
                "group_by_schema" = [
                    {"name" = "hash"; "expression" = "farm_hash(value)"; "type" = "uint64";};
                    {"name" = "value"; "type" = "string";};
                ];
                "input_stream_ids" = ["event_a"; "event_b"; "event_c"; "event_d";];
                "output_stream_ids" = [];
                "external_state_managers" = {
                    "/state" = {
                        "external_state_manager_class_name" = "NYT::NFlow::TSimpleExternalStateManager";
                        "parameters" = {
                            "path" = "//path/to/state";
                        };
                    };
                };
            };
        };
    };
};
```

- The process function `TReducer`, hosted by `TProcessFunctionComputation`, implements the logic.
- To work with the [state](../../../../flow/concepts/glossary.md#state), the example uses `TSimpleExternalStateManager`, which provides direct access to the table. `TReducer` stores a `TMutableStateKeyClient<TSimpleExternalState>` and binds it to `"/state"` in `Init(const IRuntimeInitContextPtr&)`.

{% code '/yt/yt/flow/examples/cpp/shuffle/lib/shuffle_functions.cpp' lang='cpp' lines='[BEGIN example_shuffle_reducer]-[END example_shuffle_reducer]' %}
