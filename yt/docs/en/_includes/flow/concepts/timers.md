# Timers in {{product-name}} Flow

## Why you need timers {#why-timers}

Many stream processing tasks require waiting. For example, you might need to treat a conversion as failed if no click arrives within 30 minutes after an impression. The standard message exchange between computations isn’t suitable for this: a message is either present or not, and you can’t wait for an event’s “absence”.

Timers solve this problem. A process function in transform mode can create a timer — tell the system, “Wake me up when time X arrives.” When the moment comes, the function receives a `ProcessTimer` / `on_timer` / `onTimer` call and can make a decision based on the accumulated [state](../../../flow/concepts/glossary.md#state).

Typical use cases:
- **Join with wait**: correlate an ad impression with a click that might arrive with a delay.
- **Timeout**: handle a situation where the expected event never arrives.
- **Windowed aggregations**: close a time window and output the result when the accumulated data becomes stale.

## How timers work {#how-timers-work}

### Lifecycle {#lifecycle}

1. **Registration**. The computation calls `output.add_timer` / `output.addTimer` / `output->AddTimer`, passing `TriggerTimestamp` and `EventTimestamp`.
2. **Storage**. The timer is reliably stored in {{product-name}} and additionally cached in the process memory.
3. **Triggering**. When the [EventWatermark](../../../flow/concepts/watermarks.md#event-watermark) (or another configured watermark) exceeds `TriggerTimestamp`, the timer is delivered to the computation.
4. **Processing and deletion**. The processing result and the timer deletion are recorded in a single transaction — this ensures exactly-once guarantees (see [Deduplication](../../../flow/concepts/glossary.md#deduplication)).

### Timer fields {#timer-fields}

Each timer includes two timestamp fields:

- `TriggerTimestamp` — the moment when the timer should trigger. The time scale (`event_time`, `system_time`, `real_time`) is set in the [configuration](#configuration).
- `EventTimestamp` — the business time of the original event, held in the timer. It lets you know when the triggering event occurred while processing the timer.

The timer is also bound to the **[grouping key](../../../flow/concepts/glossary.md#key)** (the same key that `Computation` uses), so it automatically lands in the correct partition and stays isolated from other keys.

## Which computations support timers {#supported-computations}

| Process-function adapter | Timer support |
|---|---|
| `TProcessFunctionComputation` | ✓ |
| `TProcessFunctionSwiftMapComputation` | ✗ |
| `TProcessFunctionSourceComputation` | ✗ |
| `TProcessFunctionTransformOrderedSourceComputation` | ✗ |

## Configuration {#configuration}

To enable timers in a computation, you must fill the `timers` field in its spec. Each array element describes a single timer stream with the following parameters:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TTimerSpec.md) %}

Explanations:

- `time_type` defines the scale that the system uses to compare `TriggerTimestamp` with the current watermark:
  - `event_time` (default) — compared with `EventWatermark` across all `input` streams (or the streams listed in `streams`).
  - `system_time` — compared with `SystemWatermark`.
  - `real_time` — compared with real astronomical time.

- `streams` / `streams_with_delays` — let you limit which input streams contribute to the watermark calculation for this timer. `streams_with_delays` also lets you set an individual delay for each stream. These parameters apply only to `event_time` and `system_time` timers. A `current_time` timer triggers when the current time reaches `TriggerTimestamp`; `streams` and `streams_with_delays` are ignored for it.

- `deduplicate_equal_timestamps` — merges timers in the same stream with the same key and `TriggerTimestamp`, keeping the smallest `EventTimestamp`. Deduplication is not guaranteed: duplicates are possible. Enabled by default.

### Timer store {#timer-store-backend}

The static `timer_store_backend` parameter in each computation's spec selects the timer store:

- `in_memory` (default) — all active timers of a partition are held in the job's memory and persisted in the `timers` table.
- `paged` — timers are stored in {{product-name}} tables in pages of about `timer_page_size` timers. Only timers closest to firing and page metadata are loaded into memory. Timers with the same trigger timestamp are kept in one page and loaded together.

The dynamic `timer_store_count_limit` and `timer_store_byte_size_limit` parameters limit the number and size of timers in memory for both stores. The `paged` store does not support a `watermark_percentile` below 100 for computations with timer streams.

For `paged`, the dynamic `timer_store.timer_write_multiplier` parameter limits timer row writes per iteration. The allowance is a multiple of the mandatory writes of new and replacement timers. Its default is 2. The value must be finite and at least 1. The remaining allowance is used to move timers after repartitioning. Deletions and metadata writes do not count toward the allowance.

To change `timer_store_backend`, stop the pipeline and update its spec. At the next start, jobs move their timers to the selected store before processing begins. An interrupted move resumes at the next start. A change to `deduplicate_equal_timestamps` also takes effect when jobs start. A new `timer_page_size` is applied gradually during processing.

Timers of computations and timer streams removed from the spec are not deleted automatically.

#### Rolling back to a version without paged support {#timer-store-rollback}

A rollback is allowed only when all computations use `timer_store_backend: in_memory` and the transfer of timers out of pages has completed.

If the pipeline has used `paged`, stop it and switch the affected computations to `in_memory`. Start the pipeline on a version that supports `paged`. Wait until all jobs finish initialization and begin processing. Only then proceed with the rollback. Changing the spec alone is not enough: the older version cannot read timers left in pages.

## Processing order {#timer-ordering}

Within one timer stream, timers for the same key are processed in `TriggerTimestamp` order.

Processing order across timer streams is not guaranteed. For `event_time` and `system_time` timers, to make timer A fire after timer B, add stream B to the `streams` field in timer A's spec. Timer A will then wait for stream B's watermark to advance.

## Blocking related timer streams {#timer-stream-blocking}

When output limits block a timer stream, delivery of ready timers pauses for its entire group, even if the blocked stream has no registered timers. Other groups can continue processing.

Timer streams belong to the same group if they use the same `time_type` (`event_time` or `system_time`) and their watermark input sets overlap. Grouping is transitive: streams that wait for `{a, b}`, `{b, c}`, and `{c, d}` form one group. Differences in `streams_with_delays` values do not split a group. All `real_time` timer streams form a separate group, regardless of their configured inputs and delays.

Groups include all timer streams configured for the computation, not just those with registered timers. Grouping does not add ordering guarantees across streams.

## Monitoring timer lag {#monitoring}

For every stream, including timer streams and united streams (`__united__`, `__united_timer__`, and others), the controller publishes `yt.flow.controller.streams.*` metrics with the `stream_id` tag:

- `time_lag`, `count_lag`, `byte_size_lag` — include all registered timers. A timer's lag starts at its creation, so timers scheduled far in the future produce a constant lag "shelf" even when processing keeps up.
- `ready_time_lag` — the longest time an unprocessed timer has been due: time since the earliest due moment among timers that are not processed yet. For timers with `time_type` `current_time`, the due moment is `TriggerTimestamp` itself. For `event_time` and `system_time`, the controller finds the moment when the watermarks of the streams the timer waits for first reached `TriggerTimestamp` plus their delays, using a persisted history of these watermarks. The history is thinned over time, so for timers that became due long ago the lag can be over-reported by up to `1 / watermark_history_thinning_factor` (25% by default), but it is never under-reported.
- `ready_count_lag`, `ready_byte_size_lag` — the number and size of timers that have triggered but are not processed yet. Timers that have not triggered are excluded.

For streams without timers, `ready_*` equal `time_lag`, `count_lag`, and `byte_size_lag`. The `ready_*` metrics never exceed the corresponding regular lags.

{% note warning %}

- If the watermark that timers wait for does not advance, the timers never become due and `ready_time_lag` stays near zero. Use `time_lag` and the lags of input streams as a backstop for alerts.
- A timer registered with a `TriggerTimestamp` in the past can briefly (usually for 1–2 job reports, until it is processed) raise `ready_time_lag` up to `time_lag`.
- If an `event_time` or `system_time` timer became due before the watermark history starts, `ready_time_lag` is not published for its stream and the united streams that include it until that timer is processed. This happens right after the first rollout or a controller rollback, and for timers with a `TriggerTimestamp` far in the past. `ready_count_lag` and `ready_byte_size_lag` are still published. Configure alerts on `ready_time_lag` to treat missing data as OK, and use `time_lag` as a backstop.

{% endnote %}

The `ready_count_lag` metric is not the same as the `ready_count` lifecycle metric. For timer streams their values match. For other streams, `ready_count_lag` equals `count_lag`, while `ready_count` counts only messages that are ready to be delivered but not yet handed over for processing.

## Timer structure {#timer-structure}

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TTimerSerializer.md) %}

## API by language {#api}

### C++ {#api-cpp}

```cpp
void ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& /*context*/) override
{
    output->AddTimer(
        TSystemTimestamp(message->EventTimestamp.Underlying() + TDuration::Minutes(30).Seconds()),
        message->EventTimestamp);
}

void ProcessTimer(
    const TInputTimerConstPtr& timer,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context) override
{
    // timer->Key, timer->EventTimestamp, timer->TriggerTimestamp
    auto builder = context->MakeOutputMessageBuilder();
    // ...
    output->AddMessage(builder.Finish());
}
```

For more details, see [Process functions (C++)](../../../flow/cpp/process-functions.md).

### Java {#api-java}

```java
// Create a timer in onMessage:
output.addTimer(message.getEventTimestamp() + 30 * 60_000_000_000L, message.getEventTimestamp());

// Process the triggered timer (RowFunction):
@Override
public void onTimer(Timer timer, OutputCollector output, RuntimeContext ctx) {
    // timer.getKey(), timer.getEventTimestamp(), timer.getTriggerTimestamp()
}

// For BatchFunction:
@Override
public void onTimers(List<Timer> timers, OutputCollector output, RuntimeContext ctx) { ... }
```

For more details, see the [Computation (Java)](../../../flow/java/computation.md) section.

### Python {#api-python}

```python
# Create a timer in on_message:
output.add_timer(trigger_timestamp=message.event_timestamp + 30 * 60_000_000_000, event_timestamp=message.event_timestamp)

# Process the triggered timer (RowFunction):
def on_timer(self, timer, output, ctx):
    # timer.key, timer.event_timestamp, timer.trigger_timestamp, timer.stream_id

# For BatchFunction:
def on_timers(self, timers, output, ctx):
    for timer in timers:
        ...
```

For more details, see the [Computation (Python)](../../../flow/python/computation.md) section.

### Go {#api-go}

```go
// Create a timer in OnMessage:
out.AddTimer(flow.TimerRequest{
    TriggerTimestamp: msg.EventTimestamp + 30*60_000_000_000,
    EventTimestamp:   msg.EventTimestamp,
})

// Process the triggered timer (RowFunction):
func (f myFunction) OnTimer(
    ctx context.Context,
    rt flow.Runtime,
    timer flow.Timer,
    out flow.OutputCollector,
) error {
    // timer.Key, timer.EventTimestamp, timer.TriggerTimestamp, timer.StreamID
    return nil
}

// For BatchFunction:
func (f myFunction) OnTimers(ctx context.Context, rt flow.Runtime, timers []flow.Timer, out flow.OutputCollector) error { ... }
```

For more details, see the [Computation (Go)](../../../flow/go/computation.md) section.

## Examples {#examples}

Example implementation of a join with wait (impression + click, 30-minute timeout):

- [C++](../../../flow/cpp/examples/wait_click_join.md)
- [Java](../../../flow/java/examples/wait_click_join.md)
- [Python](../../../flow/python/examples/wait_click_join.md)
- [Go](../../../flow/go/examples/wait_click_join.md)

## Limitations and known issues {#limitations}

{% note warning %}

**Process memory**. In the `in_memory` store, all active timers are additionally stored in the process memory, on top of {{product-name}}. With a large number of timers, this can lead to Out of Memory errors when jobs start; the `paged` store exists for such computations (see [Timer store](#timer-store-backend)).

{% endnote %}

- Swift-map mode doesn’t support timers. If you need timer functionality, run the process function under `TProcessFunctionComputation`.
- Timestamps are transmitted in nanoseconds (uint64).

## See also

- [Watermarks and Timers](../../../flow/concepts/watermarks.md)
- [Stateful processing](../../../flow/concepts/stateful.md)
- [Computation (C++)](../../../flow/cpp/computation.md)
- [Computation (Java)](../../../flow/java/computation.md)
- [Computation (Python)](../../../flow/python/computation.md)
- [Computation (Go)](../../../flow/go/computation.md)
