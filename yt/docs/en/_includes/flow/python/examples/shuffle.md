# Shuffle in {{product-name}} Flow (Python)

Use this pipeline example with two computations: the source computation parses JSON data and sends typed messages, and the transform computation counts events in an external state.

[Source code]({{source-root}}/yt/yt/flow/examples/python/shuffle)

## Structure

- `reader` (source) -- `EventMapper`: parses JSON from the `data` field and sends typed messages to the `event` stream.
- `reducer` (transform) -- `EventReducer`: counts the number of events by key using external state.

## `__main__.py`

{% code '/yt/yt/flow/examples/python/shuffle/__main__.py' lang='python' lines='[BEGIN main]-[END main]' %}

## `event_mapper.py`

The source function parses JSON from the `data` field of the input message and creates a typed message using `ctx.message_builder()`:

{% code '/yt/yt/flow/examples/python/shuffle/event_mapper.py' lang='python' lines='[BEGIN event_mapper]-[END event_mapper]' %}

## `event_reducer.py`

The transform function uses external state to count events:

{% code '/yt/yt/flow/examples/python/shuffle/event_reducer.py' lang='python' lines='[BEGIN event_reducer]-[END event_reducer]' %}

## Key patterns

- A pipeline with multiple computations: source + transform.
- A [source](../../../../flow/concepts/glossary.md#source) computation with `source=True` to read from an external source.
- Parsing JSON and creating typed messages using `ctx.message_builder()`.
- External state with the `to_builder()` / `set()` / `finish()` pattern.

