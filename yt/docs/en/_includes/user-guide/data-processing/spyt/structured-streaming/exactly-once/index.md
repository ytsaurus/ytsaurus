# Exactly-once guarantee

By default, streaming in SPYT provides an `at-least-once` guarantee — writing to the output table and committing the consumer offset are performed independently. Therefore, duplicate records may appear in the table if a micro‑batch is reprocessed after a failure.

If duplicates are not acceptable, SPYT offers two ways to ensure `exactly‑once` delivery:

#|
|| **Approach** | **How it works** | **When to use** ||
|| [Transactional mode](../../../../../../user-guide/data-processing/spyt/structured-streaming/exactly-once/transactional-mode.md) (SPYT 2.10+) | `exactly-once` for any transformations, including `join` and stateful ones. Data writing and offset committing are performed atomically in a single transaction | When data accuracy is critical: financial analytics, ML features, incremental data mart construction ||
|| [Idempotent receiver](../../../../../../user-guide/data-processing/spyt/structured-streaming/exactly-once/idempotent-receiver.md) | `exactly-once` only for stateless 1:1 transformations. Re‑writing rows using a unique composite key does not create duplicates (upsert) | When you want to avoid the additional RPC proxy load caused by the transactional mode, or when maintaining legacy code ||
|#

{% note warning %}

Both approaches guarantee `exactly‑once` only within Spark jobs in {{product-name}}: the guarantee covers writing to the output table and committing the consumer offset. It does not apply to external systems — such as writing to an external database, sending messages to other queues, or calling external APIs. For such scenarios, you need additional measures at the application level.

{% endnote %}

If duplicate output data is acceptable, no additional configuration is required — non‑transactional streaming is enabled by default.

## Performance { #performance }

Below are the throughput measurement results for the transactional mode compared to other approaches: non‑transactional streaming and the idempotent receiver.

Conditions: synthetic NEXMark‑style loads, a queue of 5 million rows (16 tablets, average row size ~70 bytes), 8 executors × 2 cores, shared RPC proxy in both modes for a fair comparison.

#|
|| **Load type** | **Mode** | **Throughput** ||
|| Stream‑static `join` with key‑based shuffle | Non‑transactional streaming | ~22 700 rows/s ||
|| ^ | Transactional streaming | ~26 200 rows/s (+13 %) ||
|| Stateful aggregation in 1‑minute event windows | Non‑transactional streaming | ~13 900 rows/s ||
|| ^ | Transactional streaming | ~14 000 rows/s (within noise) ||
|| Comparison with idempotent receiver (passthrough, 1 million rows, 1 executor × 1 core) | Idempotent receiver (sorted table) | ~2 000 rows/s ||
|| ^ | Transactional streaming | ~2 600 rows/s (+23 %) ||
|#

The transactional mode performs better when there are many small writes or shuffle operations (`join`, `groupBy`, aggregations): a single transaction instead of dozens reduces the overhead for commits. For stateful operations with a small output volume, there is little to amortize, so there is practically no difference.

Compared to the idempotent receiver (which requires creating a sorted table), the transactional mode is consistently faster: data is written to an ordered table without maintaining a sorted index.

## See also { #see-also }

- [Transactional mode](../../../../../../user-guide/data-processing/spyt/structured-streaming/exactly-once/transactional-mode.md) — setup instructions
- [Idempotent receiver](../../../../../../user-guide/data-processing/spyt/structured-streaming/exactly-once/idempotent-receiver.md) — an alternative for stateless 1:1 transformations
- [Structured Streaming](../../../../../../user-guide/data-processing/spyt/structured-streaming/index.md) — overview and key use cases
- [Streaming options](../../../../../../user-guide/data-processing/spyt/thesaurus/streaming-options.md) — options reference
