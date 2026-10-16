# Watermarks in {{product-name}} Flow

## Why time matters in stream processing {#why-time-matters}

Imagine you’re sorting letters by their writing date, but they arrive out of order. One letter dated March 1 arrives today, and another dated March 5 arrived yesterday. How do you know that you’ve received all the March letters and can start summarizing the data? A watermark is exactly that signal: “all events older than moment X have been received and processed.”

In real systems, the situation is even more interesting. Suppose an ad impression is recorded in the system at 10:05, but the user saw the ad at 10:02. Which time should you consider the “real” one? The time recorded in the system (10:05) or the time of the event itself (10:02)? The answer depends on your task, and each case requires its own time scale.

Flow supports both time scales—`SystemTimestamp` (record time) and `EventTimestamp` (event time)—as well as the Watermarks mechanism to track processing progress. Below, you’ll find an overview of each of these elements.

Currently, Flow doesn’t include the `Window` concept that exists in [The Dataflow Model](https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/43864.pdf) and [Apache Beam](https://beam.apache.org/). However, it does provide basic building blocks that let you define `EventTimestamp`, calculate `EventWatermark`, and set up `Timers`.

## Two time scales: SystemTimestamp and EventTimestamp {#two-timescales}

### SystemTimestamp

All messages in Flow must include the `SystemTimestamp` field, which stores the event creation time. For `source` streams, this is the time when the event was created in the external system. For all other streams, it’s strictly the `YT` time.

### EventTimestamp

All messages in `Flow` must have the `EventTimestamp` field populated. This is the time of the event itself in business logic terms: for example, the moment a user clicks or the time a log entry is written on the client side. You set `EventTimestamp` using `WatermarkStrategy` in `SourceComputation`.

## SystemWatermark

All public messages (of the `input` and `output` types) have `SystemTimestamp`. `SystemWatermark` takes a value such that any possible message with `SystemTimestamp < SystemWatermark` can be considered processed. The exception is `source` streams. Since they come from an external system, `SystemWatermark` for them may be inaccurate.

You use `SystemWatermark` to estimate the time lag across all streams.

Because `Flow` assigns all `SystemTimestamp` values itself (and knows the current {{product-name}} time) and also maintains information about the current `inflight` data, it can reliably keep the `SystemWatermark` value up to date. Currently, you can expect a delay of up to a few minutes.

You can assume that `SystemWatermark` for `input` and `output` messages is completely accurate. That’s why you can use it to clean up the input message deduplication table (`input_messages` or `compact_input_messages`) containing already processed `message_id` values.

## EventWatermark {#event-watermark}

You use the configured `WatermarkStrategy` in `SourceComputation` to estimate `EventWatermark` at different points in the system.

`WatermarkStrategy` includes three main modules:

- `EventTimestampAssigner` — to automatically populate `EventTimestamp` based on a specific column in `output` streams.
- `WatermarkGenerator` — settings for the `EventWatermark` generator. It lets you configure `out_of_orderness_bound` and also handle `idle` and `unavailable` partitions.
- `WatermarkAlignment` — a special module for aligning reads.

For more details, see the [spec](../../../flow/concepts/spec.md#watermark-strategy).

## Late Data

Late Data is the term for messages that arrive in violation of `EventWatermark`. This concept was introduced in the article about [The Dataflow Model](https://static.googleusercontent.com/media/research.google.com/ru//pubs/archive/43864.pdf).

You can read more about how to handle these events in the article.

## WatermarkGenerator

This module for source [computations](../../../flow/concepts/glossary.md#stream-and-computation) tries to provide a lower-bound estimate for the `EventTimestamp` of future events across all `output` streams. The algorithm works as follows:

1. You estimate `EventWatermark` for each [partition](../../../flow/concepts/glossary.md#partition) of the input queue independently.
2. You take the minimum value across all partitions.
3. To estimate `EventWatermark` for a specific partition, you use `max(MaxTimestamp – OutOfOrdernessBound, min(MinAheadTimestamp, MaxAheadTimestamp - OutOfOrdernessBound))`, where:

   * `MaxTimestamp` — the maximum time encountered in that partition.
   * `OutOfOrdernessBound` — a parameter defined in the spec that characterizes the delays in writing data to the queue.
   * `MinAheadTimestamp` — the minimum time in the next unprocessed data batch.
   * `MaxAheadTimestamp` — the maximum time in the next unprocessed data batch.
4. Unavailable partitions, as well as partitions that aren’t receiving writes, can be ignored to a limited extent. You can find the settings for these heuristics in the [spec description](../../../flow/concepts/spec.md#watermark-generator).

This approach isn’t optimal. In the future, we plan to:

- Calculate `MaxTimestamp` across multiple dimensions{% if audience == "internal" %} (ticket [YTFLOW-64](https://nda.ya.ru/t/cHIENl-i7gKZHC)){% endif %} — to protect against cases like “impressions happen, but clicks don’t.”
- Maintain the `MinTimestamp` value for the last N messages from a partition{% if audience == "internal" %} (ticket [YTFLOW-65](https://nda.ya.ru/t/DuDC3wCs7gKZHJ)){% endif %} — to protect against cases where multiple providers write to a partition and one of them lags significantly more than `OutOfOrdernessBound`.

{% note info %}

We also plan to create a protocol for passing `Watermark` information between different [pipelines](../../../flow/concepts/glossary.md#pipeline).

{% endnote %}

## WatermarkState

Flow maintains `EventWatermark` and `SystemWatermark` values for each stream and each `alignment` group.

## Timers {#timers}

A timer lets `TransformComputation` say, “wake me up when `EventWatermark` reaches time X.” This is the main tool for delayed processing, joins with waiting, and windowed aggregations.

Timers are closely tied to watermarks: by default, `EventWatermark` determines when a timer fires. For more details on how timers work, their configuration, and the API, see the [Timers](../../../flow/concepts/timers.md) section.

## Configuration {#configuration}

You configure `WatermarkStrategy` (`EventTimestampAssigner`, `WatermarkGenerator`, `WatermarkAlignment`) in the [Spec](../../../flow/concepts/spec.md#watermark-strategy) section.

## See also

- [Timers](../../../flow/concepts/timers.md)
- [Message processing order](../../../flow/concepts/ordering.md)
- [Spec and DynamicSpec](../../../flow/concepts/spec.md)