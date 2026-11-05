"""TextMapper: mirrors the input row and adds a column computed in Python."""

from yt.yt.flow.library.python.companion.computation import RowFunction

MIRRORED_COLUMNS = ("key", "text")


# [BEGIN text_mapper]
class TextMapper(RowFunction):
    """Copies every input column to the output stream and uppercases |text|."""

    def on_message(self, message, output, ctx):
        out = ctx.message_builder("mapped")
        for column in MIRRORED_COLUMNS:
            out.set(column, message.payload[column])
        out.set("text_upper", message.payload["text"].upper())
        output.add_message(out.finish())


# [END text_mapper]
