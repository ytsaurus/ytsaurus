# {{product-name}} Flow Connectors

A [connector](../../../flow/concepts/glossary.md#connector) is a component that links a pipeline to {{product-name}} objects (a queue, a table, and so on). Each connector provides a [source](../../../flow/concepts/glossary.md#source) for reading messages and/or a [sink](../../../flow/concepts/glossary.md#sink) for writing.

{% note info %}

The connectors used by a pipeline directly affect the message processing guarantees it provides. Before you choose connectors, review the [Processing Guarantees](../../../flow/concepts/guarantees.md) section.

{% endnote %}

{% if audience == "internal" %}Integrations with external (non-{{product-name}}) systems are described in the [Extensions](../../../yandex-specific/flow/extensions/about.md) section.{% endif %}

## List of connectors {#list}

#|
|| **Connector** | **Has source** | **Has sink** | **Description** ||
|| [Queue](../../../flow/connectors/queue.md) | &#10003; | &#10003; |
Reading from and writing to an ordered dynamic table using the [Queue API](../../../user-guide/dynamic-tables/queues.md) ||
|| [Static Table](../../../flow/connectors/static-table.md) | &#10003; | &#10003; |
Reading from static tables: a fixed set or an unlimited sequence from a directory. Writing a continuous sequence of static tables in message arrival order ||
|| Random | &#10003; | &#65794; |
Reading random data generated on the fly. Used for testing ||
|| [Service Log](../../../flow/connectors/servicelog.md) | &#10003; | &#65794; |
Generating a service log based on an external [state](../../../flow/concepts/glossary.md#state) table. A service log is the generation of all keys from a dynamic table at a specified frequency; essentially, it’s a way to re-scan all states once per the configured time ||
|| [Sorted Dynamic Table](../../../flow/connectors/sorted-dynamic-table.md) | &#65794; | &#10003; |
Writing to a sorted dynamic table ||
|#

## Changing the source {#source-change}

The source identifies its [partitions](../../../flow/concepts/glossary.md#partition) by parameters that describe the physical source; the specific set of these parameters is listed in the documentation for the corresponding connector. These parameters are part of the partition key, so when you change them, the source describes a different set of partitions.

When you change the source, the partitions from the old source disappear from the set and are **completed** (`Completing` ⇒ `Completed`): the associated [state](../../../flow/concepts/glossary.md#state), including stored offsets, is deleted and not saved with the expectation that the same partition will return (as during an interrupt during repartitioning). The partitions for the new source are created from scratch.

{% note warning %}

The parameters that identify the source are part of the static [`Spec`](../../../flow/concepts/spec.md), so you can change them only when the pipeline is stopped. The new source is read independently of what the old source has already processed: if it contains the same data, the data will be written to the output streams again.

{% endnote %}