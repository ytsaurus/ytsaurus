# Word Count in {{product-name}} Flow (Python)

Use this simple stateful-pipeline example in Python to count how many times each word appears. You’ll use an internal YSON-state for the counts.

[Source code]({{source-root}}/yt/yt/flow/examples/python/word_count)

## Structure

The pipeline includes a single transform-computation called `mapper`. You use it to read words from the input stream and update the counter in the state.

## `__main__.py`

This is the entry point. You create the pipeline and register the only computation.

{% code '/yt/yt/flow/examples/python/word_count/__main__.py' lang='python' lines='[BEGIN main]-[END main]' %}

## `word_count_mapper.py`

This `RowFunction` uses `ctx.state()` to work with the YSON-state. For each message key, you store a Map with the `word` and `count` fields.

{% code '/yt/yt/flow/examples/python/word_count/word_count_mapper.py' lang='python' lines='[BEGIN word_count_mapper]-[END word_count_mapper]' %}

## Key patterns

- A simple stateful pipeline with a single computation.
- Internal YSON-state via `ctx.state()` with `get_or_default` and `set`.
- The state key is defined by `group_by_schema` from the spec (in this case, by the `word` field).

