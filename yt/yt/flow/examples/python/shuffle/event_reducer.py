"""EventReducer: RowFunction that maintains count in external state."""

from yt.yt.flow.library.python.companion.computation import RowFunction


# [BEGIN event_reducer]
class EventReducer(RowFunction):
    """Counts events per key using external state."""

    def on_message(self, message, output, ctx):
        state = ctx.external_state("/shuffle-state", message)
        builder = state.to_builder()
        builder.set("count", (state.get("count") or 0) + 1)
        state.set(builder.finish())


# [END event_reducer]
