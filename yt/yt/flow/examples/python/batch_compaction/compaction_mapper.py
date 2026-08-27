"""EventCompactor: Swift BatchFunction that merges same-key events of a batch."""

from yt.yt.flow.library.python.companion.computation import BatchFunction


# [BEGIN event_compactor]
class EventCompactor(BatchFunction):
    """Collapses all events of one word within a batch into a single message.

    Runs as a Swift computation with allow_batching_with_relaxed_guarantees:
    a merged message has several parents, so the lineage of each group must
    be set explicitly via set_parent_ids. The grouping dict preserves
    insertion order, which keeps the output deterministic, as Swift requires.
    """

    def on_messages(self, messages, output, ctx):
        groups = {}
        for message in messages:
            groups.setdefault(message.key["word"], []).append(message)
        for word, group in groups.items():
            out = output.set_parent_ids([m.message_id for m in group])
            builder = ctx.message_builder("compacted")
            builder.set("word", word)
            builder.set("count", sum(m.payload["count"] for m in group))
            out.add_message(builder.finish())


# [END event_compactor]
