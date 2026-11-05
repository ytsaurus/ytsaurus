"""Reader: RowFunction of the source computation."""

from yt.yt.flow.library.python.companion.computation import RowFunction


class Reader(RowFunction):
    """Receives the records of the source and emits nothing; put your logic here."""

    def on_message(self, message, output, ctx):
        pass
