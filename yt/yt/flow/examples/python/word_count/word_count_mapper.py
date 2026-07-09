"""WordCountMapper: RowFunction that counts words using YSON state."""

from yt.yt.flow.library.python.companion.computation import RowFunction


# [BEGIN word_count_mapper]
class WordCountMapper(RowFunction):
    """Processes Word messages and maintains word count state."""

    def on_message(self, message, output, ctx):
        state = ctx.state("word-state", message)
        data = state.get_or_default({"word": message.payload["word"], "count": 0})
        data["count"] += 1
        state.set(data)
# [END word_count_mapper]
