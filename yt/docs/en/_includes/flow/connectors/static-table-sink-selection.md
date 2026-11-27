{{product-name}} Flow offers two ways to write to static tables. Choose based on where the messages come from and how you want to group them into tables.

#|
|| **Criterion** | [**Direct sink**](../../../flow/connectors/static-table.md#arrival-order-sink) | [**Export extension**](../../../flow/extensions/static-table-export.md#configuration) ||
|| **Input** | Output of a `Transform` or `SwiftOrderedSource` computation. | An ordered source with write timestamps, such as a queue, read by a dedicated passthrough computation. ||
|| **Tables** | One per batch. Batches close on time or at a row or data weight limit. | One per source write-time window across all partitions. Publication normally waits for the watermark to close the window. ||
|| **Write cost** | A separate master transaction per batch, tied to computation commits. | Worker writers can append in parallel to a shared staging table for each window. ||
|#

Use direct writes when output usually fits in one batch per `table_period`. If batches regularly hit size limits early, [write to a queue](../../../flow/connectors/queue.md) and export. Frequent rollovers create more tables and can delay partially filled batches.
