"""RowFunction that mirrors the cpp TVisitTesterComputation behaviour over the
python companion API: stores the latest payload per key, emits it on every
visit with a monotonic visit_index."""

from yt.yt.flow.library.python.companion.computation import RowFunction


class VisitTester(RowFunction):
    def on_message(self, message, output, ctx):
        state = ctx.state("user-state", message)
        data = state.get_or_default({"payload": "", "visit_index": 0})
        data["payload"] = message.payload["payload"]
        state.set(data)

    def on_visit(self, visit, output, ctx):
        state = ctx.state("user-state", visit)
        data = state.get()
        if data is None:
            return
        data["visit_index"] = int(data.get("visit_index", 0)) + 1
        state.set(data)

        out = ctx.message_builder("visits")
        out.set("key", visit.key["key"])
        out.set("payload", data["payload"])
        out.set("visit_index", data["visit_index"])
        output.add_message(out.finish())
