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

Where a computation's timers live is set by the static `timer_store_backend` field of the computation spec:

- `in_memory` (default) — all active timers of a partition are held in the job's memory and mirrored in the `timers` table. The dynamic `timer_store_count_limit` / `timer_store_byte_size_limit` limits apply to the number and size of the timers.
- `paged` — the timers live in pages of `timer_page_size` timers in the `timer_pages` / `timer_page_meta` / `timer_page_journal` tables, ordered by trigger time; the job keeps in memory only the head of every page family (the few batches closest to firing) and the page metadata, about one entry per `timer_page_size` timers. The `timer_store_count_limit` / `timer_store_byte_size_limit` limits apply to the resident timers. The exception is timers sharing one trigger timestamp (for example, the end of a window for every key): they stay in one page that cannot be split and is loaded whole. Shared-page exploration grows geometrically when few timers belong to the partition. Reading completes the known nearest-timer window before firing. The write allowance is configured by `timer_store.timer_write_multiplier` in the dynamic spec.

The dynamic `timer_write_multiplier` limits timer row writes per iteration. Its default is 2. The value must be finite and at least 1. Let `R` be the mandatory writes for new or replacement timers, after deduplication. At most `floor(timer_write_multiplier * R)` timer rows may be written. Repartitioning transfers use the remaining allowance. Without mandatory writes, existing timers remain in shared or nested pages. They fire from the resident portion belonging to the partition. Deletions and page metadata do not count. Unused allowance does not carry over.

Changing `timer_store_backend` is a static spec change: the pipeline is stopped, and at the next start every job moves the timers of its partition between the `timers` table and the pages before its first iteration, in batches of separate transactions; an interrupted move is continued at the next start. A change of `deduplicate_equal_timestamps` is applied at a start the same way: the pages of the partition are rewritten into the new form, duplicates within a page are collapsed, and duplicates in different pages are removed when they fire. A page shared with a neighbouring partition after repartitioning is rewritten by the partition that owns the beginning of its key range; the other one waits for it to start. A change of `timer_page_size` applies gradually: pages are split and merged over several iterations without moving timer rows. A limitation of the first stage: `paged` is incompatible with a `watermark_percentile` below 100 on a computation with timers; such a spec is rejected by the validation.

The `paged` store must be enabled on the controller with the `enable_paged_timer_store` parameter (the `controller_service` section of the controller config). The current release ships with it disabled. A spec with `timer_store_backend: paged` is rejected. The flag is checked when a spec is set: a pipeline whose stored spec already selects `paged` keeps running. This keeps a rollback safe. The previous release cannot read the page format yet. It must never meet that format in pipeline data. Before a downgrade to a release without `paged` support, switch such computations to `in_memory` in the spec and wait for the conversion. Timers move from the pages back to the `timers` table at the next job start.

Every timer page has a format version (`format_version` in `timer_page_meta`). A job refuses to start when it meets a page with a version newer than it supports. Such a page was written by a newer binary. The only safe moves are to upgrade or to roll back. A new format version will be rolled out in two releases. First comes a binary that reads both versions and writes the old one. Then comes a binary that writes the new one and converts old pages when a job starts.

The timers of a computation or timer stream removed from the spec stay in the tables: they are never removed automatically.

## Processing order {#timer-ordering}

Within one timer stream, timers for the same key are processed in `TriggerTimestamp` order.

Processing order across timer streams is not guaranteed. For `event_time` and `system_time` timers, to make timer A fire after timer B, add stream B to the `streams` field in timer A's spec. Timer A will then wait for stream B's watermark to advance.

## Blocking related timer streams {#timer-stream-blocking}

When output limits block a timer stream, delivery of ready timers pauses for its entire group, even if the blocked stream has no registered timers. Other groups can continue processing.

Timer streams belong to the same group if they use the same `time_type` (`event_time` or `system_time`) and their watermark input sets overlap. Grouping is transitive: streams that wait for `{a, b}`, `{b, c}`, and `{c, d}` form one group. Differences in `streams_with_delays` values do not split a group. All `real_time` timer streams form a separate group, regardless of their configured inputs and delays.

Groups include all timer streams configured for the computation, not just those with registered timers. Grouping does not add ordering guarantees across streams.

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
