from yt.yt.flow.library.python.companion import Pipeline


def read_row(message, output, ctx):
    # Forward the source row verbatim to the "data" stream.
    builder = ctx.message_builder("data")
    builder.set("data", message.payload["data"])
    builder.set("event_ts", message.payload["event_ts"])
    output.add_message(builder.finish())


def main():
    pipeline = Pipeline()
    pipeline.add("reader", read_row, source=True)
    # The "passthrough" computation runs native C++ passthrough (see pipeline.yson) and is
    # therefore not registered with the companion.
    pipeline.run()


if __name__ == "__main__":
    main()
