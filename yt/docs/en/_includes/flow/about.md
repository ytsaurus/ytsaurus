# What is {{product-name}} Flow?

{{product-name}} Flow is a framework for streaming cross-DC event processing with exactly-once guarantees within the {{product-name}} ecosystem. It offers APIs for [C++](../../flow/cpp/getting-started.md), [Java and Kotlin](../../flow/java/getting-started.md), and [Python](../../flow/python/getting-started.md), and supports declarative pipeline descriptions in [YQL](../../flow/yql/getting-started.md).{% if audience == "internal" %} The system is a logical evolution of the [BigRT](https://docs.yandex-team.ru/big_rt/) framework.{% endif %}

Its closest external counterparts are [Google Cloud Dataflow](https://cloud.google.com/products/dataflow?skip_cache=true%22%22) and [Apache Flink](https://flink.apache.org/).{% if audience == "internal" %} You can read a [detailed comparison with alternative technologies](../../yandex-specific/flow/other/comparison.md) in a separate article.{% endif %}

The system is under active development, but more than ten teams have already built their production processes on it.{% if audience == "internal" %} These teams come from different parts of Yandex, including Advertising, Market, Alice, OPK, and Video Search.{% endif %} The system can reliably:

- Handle loads exceeding 100 GB/s or 1 million events per second.{% if audience == "internal" %} ([colibri](../../yandex-specific/flow/other/framework_users.md#colibri)){% endif %}
- Support 150+ logical pipeline nodes.{% if audience == "internal" %} ([limbert](../../yandex-specific/flow/other/framework_users.md#limbert)){% endif %}

## Contacts {#contact}

{% if audience == "internal" %}
For any questions, reach out to the chat in Yandex Messenger: [YT Flow Public](https://nda.ya.ru/t/MBW0Jgy-7bH78f).

If you have a question with a complex context or found a bug, create a ticket in the [YTFLOWSUPPORT](https://nda.ya.ru/t/X7imi95a7gKE5Y) queue.
{% endif %}

## System properties {#properties}

<!-- Supported order of properties: product-relevant, technical guarantees, infrastructure. -->

- Native support for multi-stage [pipelines](../../flow/concepts/glossary.md#pipeline). As a result, you get simpler deployment and system management.
- Support for [watermarks](../../flow/concepts/watermarks.md) and [timers](../../flow/concepts/timers.md).
- [Exactly-once semantics](../../flow/concepts/guarantees.md) for event processing by default.
- Typical event processing latency under stable operation: 1s–10s.
- Automatic balancing of [partitions](../../flow/concepts/glossary.md#partition) across machines.
- Fault tolerance: the pipeline survives the failure of individual machines and data centers.
- Ability to implement business logic in [C++](../../flow/cpp/getting-started.md), [Java and Kotlin](../../flow/java/getting-started.md), [Python](../../flow/python/getting-started.md), and [YQL](../../flow/yql/getting-started.md).
- Support for [stateful processing](../../flow/concepts/stateful.md) with persistent state in {{product-name}} dynamic tables.
- Support for running in {{product-name}}{% if audience == "internal"%}, and in [Deploy](https://docs.yandex-team.ru/deploy){% endif %}.

{% include [Language choice](language-choice.md) %}

## Target system properties {#target-properties}

- Smart planning of the entire pipeline, taking into account CPU/RAM consumption of individual pipeline nodes and shared resources (common caches, databases, etc.) used by multiple nodes.
- Ability to run pipelines on clusters with thousands of nodes or more.
- Minimal downtime when nodes, DCs, or clusters fail, as well as during updates.

## See also {#see-also}

- [Getting started](../../flow/start.md)
- [Quick start](../../flow/quickstart.md)
- [Basic concepts](../../flow/concepts/glossary.md)
- [Task examples](../../flow/tasks.md)
{% if audience == "internal" %}- [Who uses {{product-name}} Flow](../../yandex-specific/flow/other/framework_users.md){% endif %}