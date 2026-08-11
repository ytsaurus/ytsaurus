# The distribute flag in {{product-name}} Flow (Python)

The `distribute` flag is a per-message flag that you set when you add an output [message](../../../flow/concepts/glossary.md#message) to [SourceComputation](../../../flow/python/computation.md#sourcecomputation). It controls whether the message is published further along the processing graph.

The `distribute` flag ensures:

- Correct [watermark](../../../flow/concepts/watermarks.md) evaluation: messages with `distribute=False` are still accounted for by the watermark generator (unlike filtering in `on_message`, which can break the watermark).
- Assignment of deterministic identifiers to messages.

{% note warning %}

To filter a message in `SourceComputation`, don’t skip it in `on_message`. Instead, emit it with `distribute=False`. This way, the message isn’t published further, but it remains accounted for in watermark evaluation.

{% endnote %}

## When to use distribute=False

Use the `distribute=False` flag when:

- You need to filter some output messages at the source-computation stage.
- Correct watermark evaluation is important.

If you don’t set the flag, it defaults to `True`, and the message is published further.

## Usage {#usage}

You move the filtering logic to the processing function: instead of a separate filtering step, you emit the message with the required flag.

```python
from yt.yt.flow.library.python.companion.computation import RowFunction


class HitParsingFunction(RowFunction):
    def on_message(self, message, output, ctx):
        builder = ctx.message_builder("hit")
        builder.set("hit_id", message.payload["hit_id"])
        builder.set("hit_payload", message.payload["hit_payload"])
        # Duplicates are emitted but not published further.
        is_duplicate = message.payload["hit_payload"] == "duplicate_payload"
        output.add_message(builder.finish(), distribute=not is_duplicate)
```

## Registering a source-computation {#registration}

You register a source-computation via `Pipeline.add()` with `source=True`. You no longer need a separate filtering parameter — the decision about publishing is made in the processing function.

```python
from yt.yt.flow.library.python.companion import Pipeline

pipeline = Pipeline()
pipeline.add("hit_reader", HitParsingFunction(), source=True)
```

## See also

- [Computation (Python)](../../../flow/python/computation.md)
- [Watermarks](../../../flow/concepts/watermarks.md)
- [The distribute flag (Java)](../../../flow/java/distribute.md)
{% if audience == "internal" %}- [Example lb_wait_click_join](../../../yandex-specific/flow/python/examples/lb_wait_click_join.md){% endif %}