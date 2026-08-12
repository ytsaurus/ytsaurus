# Wait Click Join in {{product-name}} Flow (Go)

An example of a join [pipeline](../../../../flow/concepts/glossary.md#pipeline) that joins the [streams](../../../../flow/concepts/glossary.md#stream-and-computation) of impressions (`hit`) and actions (`action`) by a shared key, using [timers](../../../../flow/concepts/glossary.md#timer) and an external [state](../../../../flow/concepts/glossary.md#state). This is a Go implementation of the similar [C++ example](../../../../flow/cpp/examples/wait_click_join.md).

[Source code]({{source-root}}/yt/yt/flow/examples/go/wait_click_join)

## Structure {#structure}

The pipeline consists of three [computations](../../../../flow/concepts/glossary.md#stream-and-computation):

- `hit_reader` and `action_reader` — native sources (`TSwiftPassthroughOrderedSourceComputation`) declared directly in the [spec](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec): they read two queues and publish the `hit` and `action` streams. They have no Go code. Each source declares a `watermark_strategy`, so the [watermark](../../../../flow/concepts/glossary.md#timestamps-and-watermarks) of the pipeline is computed from the event time.
- `join` (`joinFunction`) — a transform computation served by the companion.

The `join` computation works as follows:

1. It receives messages from two input streams: `hit` and `action`.
2. It accumulates the window of one impression in the external state `/join-state`.
3. It sets a timer for the moment the waiting window closes.
4. When the timer fires, it publishes the join result to the `joined_action` stream or simply clears the state.

Both input streams are grouped by `hit_id` and `hit_time`, so everything that belongs to one impression falls into one key. The length of the window is set by the `wait_for_actions` computation parameter from the spec.

## `main.go` {#main-go}

The entry point: creating the pipeline, registering the only computation, and starting it.

{% code '/yt/yt/flow/examples/go/wait_click_join/main.go' lang='go' %}

## `join_function.go` {#join-function-go}

The main logic of the join: the function handles two streams and uses timers for window aggregation.

### `OnMessage` {#on-message}

It drops late messages, appends its own field to the impression window, and sets the timer for closing the window:

{% code '/yt/yt/flow/examples/go/wait_click_join/join_function.go' lang='go' lines='[BEGIN join_function]-[END join_function]' %}

### `OnTimer` {#on-timer}

It closes the window: it publishes the join result if both sides arrived, and clears the state in any case:

{% code '/yt/yt/flow/examples/go/wait_click_join/join_function.go' lang='go' lines='[BEGIN on_timer]-[END on_timer]' %}

## Key patterns {#key-patterns}

- External state through `flow.OpenExternalState(rt, joinStateName, msg)`: `ConvertTo` reads the window into `joinState`, the message changes the field it needs in the structure, and `ConvertFrom` saves the window.
- A timer with two timestamps: `out.AddTimer(flow.TimerRequest{TriggerTimestamp: closeTime, EventTimestamp: hitTime})` — `TriggerTimestamp` sets when the window will close, `EventTimestamp` ties the timer to the event. Messages from both streams set the timer, not only `hit`: an action can arrive earlier than the impression it belongs to, and the window still has to be closed. A message that already falls outside its window or behind the watermark returns early and sets no timer.
- Filtering out late data: `msg.EventTimestamp < rt.MinWatermark()` drops the messages of already closed windows — otherwise a second join result would be published for one impression. For details, see [watermarks](../../../../flow/concepts/glossary.md#timestamps-and-watermarks).
- Computation parameters: `rt.Parameters().Get(waitForActionsParameter, &spelled)` reads the window length from the spec instead of hardcoding it.
- Handling several input streams: the branch on `msg.StreamID` splits the logic for `hit` and `action`, and an unknown stream returns an error instead of a silent skip.
- The key instead of the state: `timer.Key` carries `hit_id` and `hit_time` from `group_by_schema`, so the window holds only the data being joined — there is no need to duplicate the key in it.
