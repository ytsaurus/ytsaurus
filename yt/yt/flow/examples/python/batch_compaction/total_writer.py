"""TotalWriter: RowFunction that accumulates per-word totals in YSON state."""

from yt.yt.flow.library.python.companion.computation import RowFunction


# [BEGIN total_writer]
class TotalWriter(RowFunction):
    """Adds compacted counts to the per-word total."""

    def on_message(self, message, output, ctx):
        state = ctx.state("total-state", message)
        data = state.get_or_default({"word": message.payload["word"], "count": 0})
        data["count"] += message.payload["count"]
        state.set(data)


# [END total_writer]
