"""Entry point for the Python static-table-join companion process.

A static reference table is streamed row by row into ``ReferenceLoader``, which
normalizes each name and persists it to keyed external state. ``Enricher`` joins
the realtime ``event`` stream against that state and emits the enriched row.
"""

import logging

from yt.yt.flow.library.python.companion import Pipeline
from yt.yt.flow.library.python.companion.computation import RowFunction

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# [BEGIN reference_loader]
class ReferenceLoader(RowFunction):
    """Trims and lowercases the reference name, storing it in external state."""

    def on_message(self, message, output, ctx):
        name = message.payload["name"]
        normalized_name = name.strip().lower()

        state = ctx.external_state("/reference_state", message)
        builder = state.to_builder()
        builder.set("normalized_name", normalized_name)
        state.set(builder.finish())


# [END reference_loader]


# [BEGIN enricher]
class Enricher(RowFunction):
    """Joins each event against the reference state and emits the enriched row."""

    def on_message(self, message, output, ctx):
        state = ctx.joined_external_state("/reference_state", message)
        normalized_name = state.get("normalized_name")
        if normalized_name is None:
            return

        builder = ctx.message_builder("enriched")
        builder.set("key", message.payload["key"])
        builder.set("name", normalized_name)
        output.add_message(builder.finish())


# [END enricher]


# [BEGIN main]
def main():
    log.info("Starting companion execution")

    pipeline = Pipeline()
    pipeline.add("reference_loader", ReferenceLoader())
    pipeline.add("enricher", Enricher())

    log.info("Starting pipeline...")
    pipeline.run()
    log.info("Pipeline completed")


# [END main]


if __name__ == "__main__":
    main()
