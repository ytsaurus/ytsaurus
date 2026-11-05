"""SecretSink: counts processed messages per key and records the secret from the environment.

The internal state (the pipeline's `states` table) is the observable output: a growing total count
proves the pipeline keeps processing (e.g. after reanimate) and the secret column proves the secret
declared via vanilla.secret_env reached the companion process.
"""

import os

from yt.yt.flow.library.python.companion.computation import RowFunction


class SecretSink(RowFunction):
    def on_message(self, message, output, ctx):
        state = ctx.state("secret-state", message)
        data = state.get_or_default({"count": 0, "secret": ""})
        data["count"] += 1
        data["secret"] = os.environ.get("YT_MY_SECRET", "")
        state.set(data)
