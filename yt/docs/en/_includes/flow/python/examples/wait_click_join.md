# Wait Click Join in {{product-name}} Flow (Python)

This is an example of a join-pipeline that merges hit and action streams by key, using timers and external state. It’s a Python implementation of a similar C++ example.

[Source code]({{source-root}}/yt/yt/flow/examples/python/wait_click_join)

## Structure

The pipeline includes a single transform computation called `join`, which:

- Receives messages from two input streams: `hit` and `action`.
- Accumulates data in the external state.
- Sets a timer for the end of the waiting window.
- When the timer triggers, it generates a join result or clears the state.

## `__main__.py`

{% code '/yt/yt/flow/examples/python/wait_click_join/__main__.py' lang='python' lines='[BEGIN main]-[END main]' %}

## `join_process_function.py`

This file holds the core join logic. The function processes two streams and uses timers for windowed aggregation:

### `on_message`

{% code '/yt/yt/flow/examples/python/wait_click_join/join_process_function.py' lang='python' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

### `on_timer`

{% code '/yt/yt/flow/examples/python/wait_click_join/join_process_function.py' lang='python' lines='[BEGIN on_timer]-[END on_timer]' keep-indents %}

## Key patterns

- **External state** with `ctx.external_state("/join-state", message)`: use the `to_builder()` / `set()` / `clear()` pattern to accumulate data from multiple streams.
- **Timers** via `output.add_timer(max_time, hit_time)`: set a timer with `trigger_timestamp` (when it fires) and `event_timestamp` (linked to the event).
- **Late data filtering**: use `message.event_timestamp < ctx.min_watermark` to discard late data (learn more about watermarks).
- **Computation parameters**: read configuration from the spec with `ctx.parameters["wait_for_actions"]`.
- **MessageBuilder**: create output messages with a defined schema using `ctx.message_builder("joined_action")`.
- **Processing multiple streams**: branch on `message.stream_id` to apply different logic for handling hit and action streams.

