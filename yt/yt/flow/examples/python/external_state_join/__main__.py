"""Entry point for the Python external-state-join companion process.

``LookupJoin`` joins the realtime ``event`` stream against a pre-built dynamic
reference table reached through the ``/reference`` external state. The join is
read-only: it never writes the state, it only looks up the ``name`` by key.
The external state joiner resolves the Cypress symlink behind that path per lookup, so
repointing the symlink swaps the reference dataset for keys looked up afterwards
without a pipeline restart.
"""

import logging

from yt.yt.flow.library.python.companion import Pipeline
from yt.yt.flow.library.python.companion.computation import RowFunction

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# [BEGIN lookup_join]
class LookupJoin(RowFunction):
    """Looks up each event's name in the reference state and emits the joined row."""

    def on_message(self, message, output, ctx):
        name = ctx.joined_external_state("/reference", message).get("name")
        if name is None:
            return

        builder = ctx.message_builder("enriched")
        builder.set("key", message.payload["key"])
        builder.set("name", name)
        output.add_message(builder.finish())


# [END lookup_join]


# [BEGIN main]
def main():
    log.info("Starting companion execution")

    pipeline = Pipeline()
    pipeline.add("lookup_join", LookupJoin())

    log.info("Starting pipeline...")
    pipeline.run()
    log.info("Pipeline completed")


# [END main]


if __name__ == "__main__":
    main()
