## See also

- [Quick start (C++)](../../../../flow/cpp/getting-started.md)
- [Timers](../../../../flow/concepts/timers.md)
- [Stateful processing](../../../../flow/concepts/stateful.md)

You must create all these objects in {{product-name}} before you [start the pipeline](../../../../flow/release/basic-rules.md#launch-flow).{% if audience == "internal" %} You can use the [YtSync]({{yt-sync-docs}}/) library to create the objects. It lets you concisely describe the objects and their differences across various [environments](../../../../flow/concepts/glossary.md#environment) and perform create, update, and [migration](../../../../flow/concepts/glossary.md#migration) operations (in some cases).{% endif %}

{% if audience == "internal" %}This example demonstrates the use of easy mode. You can find detailed documentation on it [here]({{yt-sync-docs}}/stages_specification).{% endif %}

{% if audience == "internal" %}The example source code that uses `YtSync` is in [tools/yt_sync]({{source-root}}/yt/yt/flow/examples/cpp/wait_click_join/tools/yt_sync):

- [queues.py]({{source-root}}/yt/yt/flow/examples/cpp/wait_click_join/tools/yt_sync/queues.py) — description of queues, consumers, and producers.
- [tables.py]({{source-root}}/yt/yt/flow/examples/cpp/wait_click_join/tools/yt_sync/tables.py) — table specifications. The example declares none (`TABLES = {}`); this is where you would describe any dynamic tables of your own.
- [pipelines.py]({{source-root}}/yt/yt/flow/examples/cpp/wait_click_join/tools/yt_sync/pipelines.py) — description of the pipeline.
- [stages.py]({{source-root}}/yt/yt/flow/examples/cpp/wait_click_join/tools/yt_sync/stages.py) — global settings for environments.
- [__main__.py]({{source-root}}/yt/yt/flow/examples/cpp/wait_click_join/tools/yt_sync/__main__.py) — the program’s main entry point, which boils down to calling a single library function.{% endif %}