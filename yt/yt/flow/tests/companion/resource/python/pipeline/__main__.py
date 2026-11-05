"""Companion binary for the Python companion-resource e2e test.

Hosts a greeting resource that consumes a dependency resource through a
resource-local alias and a mapper that copies the resource state into every
output row, making init parameters, applied dynamic parameters and the serving
process observable.
"""

import os

from yt.yt.flow.library.python.companion import FlowResource, Pipeline


class GreetingDependencyResource(FlowResource):
    def load(self, context):
        self._context = context

    def reconfigure(self, context):
        # Pick up the refreshed dynamic parameters delivered by the worker.
        self._context = context

    def get_value(self):
        return self._context.dynamic_parameters.get("value", "")


class GreetingResource(FlowResource):
    """Load records the pid of the serving companion process and the dependency
    value; a reconfigure delivered by the worker is observable as a fresh
    suffix value, a dependency change as a fresh dependency value after the
    worker re-inits this resource with the advanced dependency reference.
    """

    flow_resource_class = "GreetingResource"

    def load(self, context):
        self._context = context
        self._dependency_value = context.dependencies["dependency"].get_value()
        self._init_pid = os.getpid()

    def reconfigure(self, context):
        self._context = context

    def get_greeting(self):
        return self._context.parameters.get("greeting", "")

    def get_suffix(self):
        return self._context.dynamic_parameters.get("suffix", "")

    def get_dependency_value(self):
        return self._dependency_value

    def get_init_pid(self):
        return self._init_pid


def map_row(message, output, ctx):
    greeting = ctx.get_resource("greeting_view")
    builder = ctx.message_builder("mapped")
    builder.set("key", message.payload["key"])
    builder.set("greeting", greeting.get_greeting())
    builder.set("suffix", greeting.get_suffix())
    builder.set("dependency_value", greeting.get_dependency_value())
    builder.set("pid", greeting.get_init_pid())
    output.add_message(builder.finish())


def main():
    pipeline = Pipeline()
    pipeline.add("mapper", map_row)
    pipeline.add_resource(GreetingDependencyResource)
    pipeline.add_resource(GreetingResource)
    pipeline.run()


if __name__ == "__main__":
    main()
