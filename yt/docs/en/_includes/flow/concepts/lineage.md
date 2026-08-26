# Lineage in {{product-name}} Flow

Lineage (from English, “genealogy”) is information about which input [messages](../../../flow/concepts/glossary.md#message) and [timers](../../../flow/concepts/glossary.md#timer) a specific output result of a [computation](../../../flow/concepts/glossary.md#computation) was derived from. You use this information during processing so the framework can compute metadata and ensure ordering guarantees. Note that this information isn’t stored in the output message itself.

## Why you need lineage {#why-lineage}

The framework uses lineage for two purposes:

1. **Computing meta-fields.** Based on parent messages, Flow automatically populates the meta-fields of output messages, such as `EventTimestamp`, `AlignmentTimestamp`, and others. For Swift computations and [passthrough](../../../flow/concepts/glossary.md#passthrough), `AlignmentTimestamp` is inherited from the parents unchanged. This ensures correct [prioritization](../../../flow/concepts/ordering.md) of messages in downstream computations.
2. **Guaranteeing the order of derived messages.** If two messages share the same [grouping keys](../../../flow/concepts/glossary.md#key) across the entire lineage chain from the source to the current computation, their relative processing order is preserved. For more details, see the section [Message Processing Order](../../../flow/concepts/ordering.md#ordering-guarantees).

## Default behavior {#default-behavior}

In most cases, you don’t need to manage lineage explicitly—the framework automatically sets the parents:

| Function type | Parent of the output message |
| --- | --- |
| `RowFunction` / `DoProcessMessage` | The current input message |
| `BatchFunction` / `DoProcess` | All messages in the current batch |
| Timer handler | The current timer |

## When to set lineage explicitly {#explicit-lineage}

The parents to specify are the input messages of the **current call** of the batch function that the given output was actually derived from (in companion SDKs, their `message_id`); messages from other batches cannot be parents.

Whether this is mandatory depends on the computation type:

- **Swift**: every output message must have **exactly one parent**, so in a batch function with more than one message in the batch setting lineage is **mandatory** — narrow the parents down to a single input message. With the "whole batch" default, processing fails with the error `Message should have exactly one parent message`. Multiple parents per output message are allowed only with the [`allow_batching_with_relaxed_guarantees`](../../../flow/concepts/guarantees.md#swift-allow-batching-with-relaxed-guarantees) parameter enabled.
- **Transform**: setting lineage is optional, but with the "whole batch" default the `EventTimestamp` of every output message (unless set explicitly when building it) equals the minimum over the whole batch, which skews event time and [watermarks](../../../flow/concepts/watermarks.md). Narrow the parents if your pipeline relies on event time.

In row functions (`RowFunction` / `DoProcessMessage`) explicit lineage is not needed: the framework sets the current input message as the parent automatically.

## API {#api}

You set lineage using the `SetParents` / `set_parent_ids` / `setParentIds` / `WithParentIDs` method on the `OutputCollector` object. The method returns a **new** collector with the lineage context attached. All calls to `AddMessage` / `add_message` / `addMessage` on this collector will carry that lineage.

For more details on how to use this in each language:
- [C++](../../../flow/cpp/computation.md#output-collector)
- [Java](../../../flow/java/computation.md#output-collector)
- [Python](../../../flow/python/computation.md#output-collector)
- [Go](../../../flow/go/computation.md#output-collector)

## See also

- [Message Processing Order](../../../flow/concepts/ordering.md)
- [Computation](../../../flow/concepts/computation.md)
- [Core Concepts (Glossary)](../../../flow/concepts/glossary.md)
- [Computation (Go)](../../../flow/go/computation.md)