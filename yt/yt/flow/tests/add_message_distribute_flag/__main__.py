from yt.yt.flow.library.python.companion import Pipeline
from yt.yt.flow.library.python.companion.computation import RowFunction


class DistributeReader(RowFunction):
    def on_message(self, message, output, ctx):
        data = message.payload["data"]
        # event_ts comes straight from the source row; it must reach the watermark even for
        # the non-distributed ("nodist_") message.
        event_ts = message.payload["event_ts"]

        if data.startswith("nodist_"):
            # Only a distribute=False message -- no distributed output.
            duplicate = ctx.message_builder("data")
            duplicate.set("data", "dup_" + data)
            duplicate.set("event_ts", event_ts)
            output.add_message(duplicate.finish(), distribute=False)
            return

        original = ctx.message_builder("data")
        original.set("data", data)
        original.set("event_ts", event_ts)
        output.add_message(original.finish(), distribute=True)

        duplicate = ctx.message_builder("data")
        duplicate.set("data", "dup_" + data)
        duplicate.set("event_ts", event_ts)
        output.add_message(duplicate.finish(), distribute=False)


def main():
    pipeline = Pipeline()
    pipeline.add("reader", DistributeReader(), source=True)
    pipeline.run()


if __name__ == "__main__":
    main()
