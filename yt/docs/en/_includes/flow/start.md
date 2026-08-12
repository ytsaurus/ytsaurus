# Getting started with {{product-name}} Flow

This section walks you through the steps to implement and run your own pipeline in Flow.

{% include [Language choice](language-choice.md) %}

## General plan

No matter which language you choose, creating a pipeline involves the following steps:

1. **Try the [Quick start](../../flow/quickstart.md)** — run a minimal NoOp pipeline to get familiar with the Flow infrastructure.

2. **Review the basic concepts**. Read the [glossary](../../flow/concepts/glossary.md) to understand the Flow model: pipelines, streams, computations, and messages.

3. **Study the concepts**. Get to know [Computation](../../flow/concepts/computation.md), [Watermarks and Timers](../../flow/concepts/watermarks.md), and [Stateful processing](../../flow/concepts/stateful.md), as well as the [guarantees](../../flow/concepts/guarantees.md) provided by the system.

4. **Explore examples** in your chosen language:
   - C++: [WordCount](../../flow/cpp/examples/word_count.md), [Shuffle](../../flow/cpp/examples/shuffle.md), [WaitClickJoin](../../flow/cpp/examples/wait_click_join.md)
   - Java: [WordCount](../../flow/java/examples/wordcount.md), [Shuffle](../../flow/java/examples/shuffle.md), [WaitClickJoin](../../flow/java/examples/wait_click_join.md)
   - Python: [WordCount](../../flow/python/examples/wordcount.md), [Shuffle](../../flow/python/examples/shuffle.md), [WaitClickJoin](../../flow/python/examples/wait_click_join.md)
   - YQL: [Quick start](../../flow/yql/getting-started.md)

5. **Check out the available [connectors](../../flow/connectors/about.md)** — queues, static tables{% if audience == "internal" %}, Logbroker{% endif %}, and others.

6. **Describe the pipeline spec** in YSON format. In addition to the examples, the [Spec & DynamicSpec](../../flow/concepts/spec.md) section will help you.

7. **Implement your business logic** in the language you’ve chosen, following the relevant quick start guide.

8. **Create the necessary objects in {{product-name}}** — tables, queues, and the pipeline{% if audience == "internal" %} — using the [YtSync]({{yt-sync-docs}}/) utility (the pipeline specification is described [here]({{yt-sync-docs}}/pipeline_specification)){% endif %}.{% if audience == "internal" %} If needed, do the same in third-party systems like [Logbroker](../../yandex-specific/flow/extensions/logbroker.md).{% endif %}

9. **Write tests**. Follow the instructions for your programming language:
   - [C++](../../flow/cpp/testing.md)
   - [Java](../../flow/java/testing.md)
   - [Python](../../flow/python/testing.md)

10. **Run the pipeline** and monitor it via the {{product-name}} UI. For details on releases, read [Releases and pipeline management](../../flow/release/basic-rules.md).

## See also

- [About Flow](../../flow/about.md)
- [Quick start](../../flow/quickstart.md)
- [Basic concepts](../../flow/concepts/glossary.md)
- [Connectors](../../flow/connectors/about.md)
{% if audience == "internal" %}- [Comparison with alternative technologies](../../yandex-specific/flow/other/comparison.md){% endif %}