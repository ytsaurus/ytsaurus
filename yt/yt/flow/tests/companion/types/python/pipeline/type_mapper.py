"""RowFunction that performs a straightforward identity mapping of every typed
field. It reads each column of the incoming message and writes the same value
to the output stream, exercising the companion wire-protocol round-trip for
each distinct value type (string, int64, uint64, double, boolean)."""

from yt.yt.flow.library.python.companion.computation import RowFunction

# Columns mirrored verbatim, one per distinct wire value type.
PAYLOAD_FIELDS = ("f_string", "f_int64", "f_uint64", "f_double", "f_bool")


class TypeMapper(RowFunction):
    def on_message(self, message, output, ctx):
        out = ctx.message_builder("mapped")
        out.set("key", message.payload["key"])
        for column in PAYLOAD_FIELDS:
            out.set(column, message.payload[column])
        output.add_message(out.finish())
