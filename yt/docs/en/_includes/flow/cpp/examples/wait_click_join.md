# Join hits, impressions, and clicks over a time window ({{product-name}} Flow, C++)

## Problem statement

Imagine a system with three types of events:
- hit — a user request event. It contains some hit_payload with useful data.
- action show — an event indicating that the user saw the response; it contains action_time.
- action click — an event indicating that the user clicked the response they saw; it contains action_time.

All events include hit_id and hit_time.

For simplicity, assume that within a user request, a hit must be present, and there can be no more than one show and no more than one click. The data arrives in two topics: hit and action.

The output will be a join of all three logs. In general, there are many possible join variants, but here we’ll focus on one specific approach:

1. An action can occur several hours after a hit. However, this [pipeline](../../../../flow/concepts/glossary.md#pipeline) only considers actions where action_time < hit_time + wait_for_actions.
2. The system ignores all events that happen after the hit is closed. In other words, it doesn’t allow any updates to previously emitted events.
3. Hits without impressions are ignored.
4. For each show, you get information about whether a click occurred and the hit_payload from the hit.

Points 1 and 2 can have other variants depending on business goals. The article [The Dataflow Model](https://static.googleusercontent.com/media/research.google.com/ru//pubs/archive/43864.pdf) covers various possible system behaviors quite well. We strongly recommend reading it.

You can find the code [here]({{source-root}}/yt/yt/flow/examples/cpp/wait_click_join).

## Reading data

This part is pretty standard. Since the data is already in the required format in the input queue, you can use the combination `TSwiftPassthroughSourceComputation + TQueueSource`. You register `action_reader`, which generates the `action` stream, and `hit_reader`, which generates the `hit` stream.

However, for the pipeline to work correctly, you need to configure time handling properly. For both `SourceComputation` instances, you must set up `watermark_strategy`:

- You need to set `event_timestamp` to the appropriate time. Use `event_timestamp_assigner` for this. For `action_reader`, specify the `action_time` column; for `hit_reader`, use `hit_time`.
- You explicitly set `watermark_generator/out_of_orderness_bound` to `10s`. This is a key parameter for the heuristic that estimates the [watermark](../../../../flow/concepts/glossary.md#timestamps-and-watermarks). This value is only for tests; for real tasks, choose it based on the properties of the stream you’re reading.

You can read about all the settings in detail [here](../../../../flow/concepts/spec.md#watermark-strategy).

Here’s an example [spec](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec) for `action_reader`:

```yson
"spec" = {
    "computations" = {
        "action_reader" = {
            "computation_class_name" = "NYT::NFlow::TSwiftPassthroughOrderedSourceComputation";
            "input_stream_ids" = [];
            "output_stream_ids" = ["action"];
            "watermark_strategy" = {
                "event_timestamp_assigner" = {
                    "column" = "action_time";
                };
                "watermark_generator" = {
                    "out_of_orderness_bound" = "10s";
                };
            };
            "sources" = {
                "source_stream" = {
                    "source_class_name" = "NYT::NFlow::TQueueSource";
                    "stream_id" = "injected_stream";
                    "parameters" = {
                    };
                };
            };
            "parameters" = {};
        };
    };
    "streams" = {
        "action" = {
            "schema" = [
                {name = "hit_id"; type = "string";};
                {name = "hit_time"; type = "uint64";};
                {name = "is_click"; type = "boolean";};
                {name = "action_time"; type = "uint64";};
            ];
        };
    };
};
```

Keep in mind that our watermark generator is heuristic, so errors are inevitable. That means that from time to time, some events will violate the watermark rules. These are usually called `late data`. Depending on your system’s requirements, you can handle these events in a special way. In this pipeline, they’re completely ignored, which inevitably leads to some data loss.

## Joining streams

For convenience, the join is performed by the key `(hit_id, hit_time)`. When the first event for this key arrives, a timer starts with `max_time == hit_time + wait_for_actions`. The system waits until all events with `event_time < max_time` are processed (allowing for the accuracy of our `watermark`, of course) and then permanently closes the corresponding hit. It generates the necessary `output` event and deletes the original hit profile.

Any data that arrives after the hit is closed is ignored.

First, let’s look more closely at the join spec:

```yson
{
    "computations" = {
        "join" = {
            "computation_class_name" = "NYT::NFlow::TProcessFunctionComputation";
            "processing_function" = "NYT::NFlow::NExample::TJoinFunction";
            "processing_function_parameters" = {
                "wait_for_actions" = "10s";
            };
            "group_by_schema" = [
                {"name" = "hash"; "expression" = "farm_hash(hit_id)"; "type" = "uint64"};
                {"name" = "hit_id"; "type" = "string"};
                {"name" = "hit_time"; "type" = "uint64"};
            ];
            "input_stream_ids" = ["action"; "hit"];
            "output_stream_ids" = ["joined_action";];
            "timers" = {
                "timer" = {};
            };
            "sinks" = {
                "queue" = {
                    "sink_class_name" = "NYT::NFlow::TQueueSink";
                    "input_stream_ids" = ["joined_action"];
                    "parameters" = {
                    };
                };
            };
        };
    };
    streams = {
        "joined_action" = {
            "schema" = [
                {name = "hit_id"; type = "string";};
                {name = "hit_time"; type = "uint64";};
                {name = "is_click"; type = "boolean";};
                {name = "show_time"; type = "uint64";};
                {name = "click_time"; type = "uint64";};
                {name = "hit_payload"; type = "string";};
            ];
        };
    };
};
```

- In `group_by_schema`, in addition to `hit_id` and `hit_time`, we include `hash`. This is for the correct operation of the [partitioning](../../../../flow/concepts/glossary.md#partition) algorithm.
- The pipeline needs a [timer](../../../../flow/concepts/glossary.md#timer) to close the hit, so we register `timer` in `timers`. We don’t specify extra settings because timers, by default, use `event_time` and the input streams.
- To send the `joined_event` to an ordered dynamic table (which might be on another cluster), we use an asynchronous `TQueueSink`.

The join itself is implemented as a [process function](../../../../flow/cpp/process-functions.md) called `TJoinFunction` (a subclass of `IProcessFunction` that processes messages and timers element by element). It’s executed by the built-in `TProcessFunctionComputation`. In the spec, you set it via `processing_function`, and `wait_for_actions` is passed through `processing_function_parameters`. We recommend reading the code in the repository because it’s continuously improved.

Key ideas:

- When the first event for a key arrives, you create a timer to close it.
- The state for each key is described by an arbitrary type (in this example, a `TYsonStruct` subclass). This object is stored in the internal `Flow` tables. You access the state via `TMutableStateKeyClient`, which you initialize in `Init` using `IRuntimeInitContext::InitClient`.
- For each event, you record the necessary information from that event in the profile.
- When the timer fires, you generate an output event (if you have the required data) and clear the profile by calling `state.Clear()`.
- You get the current watermark with `context->GetInputEventWatermark()` and discard all late-arriving events.

{% code '/yt/yt/flow/examples/cpp/wait_click_join/lib/wait_click_join_functions.cpp' lang='cpp' lines='[BEGIN join_process_message]-[END join_process_message]' keep-indents %}

{% code '/yt/yt/flow/examples/cpp/wait_click_join/lib/wait_click_join_functions.cpp' lang='cpp' lines='[BEGIN join_process_timer]-[END join_process_timer]' keep-indents %}

## Managing objects in {{product-name}}{% if audience == "internal" %} (`YtSync`){% endif %} {#yt-sync}

The imaginary system described above includes the following objects:

- `action_queue` and `hit_queue` — input [queues](../../../../user-guide/dynamic-tables/queues.md#data_model).
- `consumer` — a [consumer](../../../../user-guide/dynamic-tables/queues.md#data_model) for reading the input queues and notifying the queue broker which messages are fully processed and no longer needed by the system.
- `output_queue` — the output queue with the joined input logs.
- `producer` — a producer for [exactly-once](../../../../flow/concepts/glossary.md#exactly-once) writes to the output queue.
- `state` — a [table](../../../../user-guide/dynamic-tables/sorted-dynamic-tables.md) for storing the key profile.
- `pipeline` — the pipeline itself, which is a collection of tables and files.

