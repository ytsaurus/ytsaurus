# Distributed Throttler in {{product-name}} Flow

Use the distributed throttler to globally limit the rate across the entire [pipeline](../../../flow/concepts/glossary.md#pipeline). The Controller holds a [token bucket](https://en.wikipedia.org/wiki/Token_bucket) for each named throttler; [Jobs](../../../flow/concepts/glossary.md#job) request a quota from the Controller before processing messages or performing any custom user action.

You can apply quotas to either the number of processed messages or the total size of their payload in bytes — separately or together.

Typical use cases:

- Limit the total load on an external API from all Jobs.
- Evenly distribute bandwidth across [partitions](../../../flow/concepts/glossary.md#partition) of a single [`Computation`](../../../flow/concepts/computation.md).
- Slow down reading from a source.

## Priority by lag {#priority}

Each Job attaches its timestamp to the quota request — the same timestamp that determines lag in the input buffer. The server takes the minimum value across all `stabilized_event_timestamp + stream_delay` from [input streams](../../../flow/concepts/glossary.md#stream) and `read_alignment_timestamp` from [source streams](../../../flow/concepts/glossary.md#source). The server ranks requests by this timestamp in ascending order: it grants quota first to those that are lagging more. If there’s a load imbalance across [partitions](../../../flow/concepts/glossary.md#partition) of a single [`Computation`](../../../flow/concepts/computation.md), the lagging ones automatically get more quota, while the leading ones slow down.

## Weighted quota classes {#quota-classes}

A named throttler can declare classes with weights. Quota is distributed among the classes that have pending requests in proportion to their weights, and in a work-conserving way. If a class goes idle, its share is immediately redistributed among the active classes. A weight sets the long-run share of the bandwidth, not a strict priority.

Priority by lag keeps working inside each class. An idle class banks no credit, but once a request appears it gets a chance to be served before the next long pass over another class's continuous backlog.

If `classes` is absent, the previous single-queue behavior and the previous token bucket semantics are preserved. The reserved `default` class always has weight `1.0`. A missing, empty, or unknown `quota_class_id` falls into `default` on the server; that is how a throttler with no class configured is served. The class covers every request the Computation makes to that throttler: if the same id is also obtained manually via `GetThrottlerOrThrow`, those requests carry the same class rather than `default`. The class is set per automatic throttler — `input_rows_throttler_class_id` and `input_bytes_throttler_class_id` — and each is checked against the classes its own throttler declares: an unknown class is rejected by validation before the pipeline starts.

```yson
"dynamic_spec" = {
    "throttlers" = {
        "output_quota" = {
            "limit" = 1000.0;
            "period" = "1s";
            "request_period" = "500ms";
            "max_grant_amount" = 50;
            "classes" = {
                "vip" = {"weight" = 5.0;};
                "regular" = {"weight" = 3.0;};
                "bulk" = {"weight" = 1.0;};
            };
        };
    };
    "computations" = {
        "ReaderVip" = {
            "input_rows_throttler_id" = "output_quota";
            "input_rows_throttler_class_id" = "vip";
        };
    };
}
```

### Class switching latency

The scheduler reconsiders the active classes after the current server chunk completes. `max_grant_amount` sets the maximum chunk size in absolute quota units; leaving it unset lets a single request hold the token bucket for as long as its whole prefetch window takes, which delays every other class by that much. The observed latency also includes `drain_period`, the RPC, the local prefetch, the source's `empty_batch_backoff`, and the Flow commit.

### Weights and reader throughput

Observed shares match the configured weights only while every measured class stays backlogged **and** each reader can actually consume its entitlement. A reader iteration carries a fixed overhead independent of the granted rate, so with a small `max_rows_per_batch` a high-weight class can hit its own throughput ceiling before it reaches its share; work conservation then hands the surplus to the lower-weight classes and the observed split drifts toward equality. Scale `max_rows_per_batch` together with `limit` so the per-iteration overhead stays a small fraction of the time each class spends waiting for quota.

## Live reconfiguration {#reconfigure}

All throttler parameters (`limit`, `period`, `request_period`, `retrying_channel`, `rpc_timeout`, `classes`, `max_grant_amount`) are applied without restarting the pipeline — just update `dynamic_spec`. The `IThroughputThrottlerPtr` cached in your user code remains valid after the config change.

Weights and the server chunk size change without rebuilding the prefetch client. Changing `input_rows_throttler_class_id` or `input_bytes_throttler_class_id` affects subsequent RPCs; quota already fetched locally or still in flight stays accounted to the previous class.

## Quota classes and watermark alignment {#watermark-alignment}

Quota availability does not override watermark alignment: reading is allowed only when the output limits, the alignment, and the read window restriction are all satisfied at once. So a heavier class will not speed reading up while alignment is holding the source back — weight distributes quota only. Configuring alignment itself is covered in [watermark strategy](../../../flow/concepts/watermarks.md).

## What happens if the Controller is unavailable {#controller-unavailable}

The throttler client reopens the connection via `retrying_channel`. While the Controller doesn’t respond, the local prefetch cache is consumed, and `Throttle()` blocks while waiting for a quota — the Job makes no progress. When the Controller comes back, the wait resolves automatically. If the Controller stays unresponsive longer than the total retry budget of `retrying_channel`, the throttler operation throws an exception and the Job fails.

## What happens if there isn’t enough quota {#quota-insufficient}

If the total demand from Jobs far exceeds the possible supply, individual `RequestQuota` calls may wait a long time in the server queue and eventually miss their `rpc_timeout` × `retry_attempts` deadline. In this case, `Throttle()` also throws an exception, which causes the Job to fail.

Fallback to a local limit isn’t supported in either of these cases right now.

## Configuration {#configuration}

All settings are in `DynamicSpec`. You declare throttlers at the root. In the example below, `external_api_quota` is a user-defined throttler ID, and `RequestEnricher` is the Computation name:

```yson
"dynamic_spec" = {
    "throttlers" = {
        "external_api_quota" = {
            "limit" = 100.0;
        };
    };
    "computations" = {
        "RequestEnricher" = {
            "input_rows_throttler_id" = "external_api_quota";
        };
    };
}
```

Fields of `TDynamicThrottlerSpec`:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicThrottlerSpec.md) %}

## Usage {#usage}

You have two options — automatic and manual. They don’t conflict: you can use one or both at the same time.

### Automatic: limit the input batch rate {#auto}

`TDynamicComputationSpec` includes two fields — `input_rows_throttler_id` and `input_bytes_throttler_id`. If you set them, the Computation waits for a quota before each `Process` iteration:

- By the number of messages in the input batch (`input_rows_throttler_id`).
- By the sum of the `byte_size` of the batch messages — this is the system size of the serialized representation, excluding network-level compression or encryption (`input_bytes_throttler_id`).

The wait is recorded as a separate `Input.Throttle` span in the Computation’s tracing (visible in Jaeger and in the “Epoch parts time” charts in the UI).

The ID must be declared in `dynamic_spec/throttlers`. Both fields may be set at once — the iteration then waits for both quotas — but they must name **different** throttlers: a throttler meters either a message count or a byte size, and a shared token bucket would be summing counts with bytes. Such a spec is rejected by validation.

### Manual: `GetThrottlerOrThrow(id)` from user code {#manual}

If automatic throttling on the input batch isn’t enough — for example, if you need a rate limit for each external request — you get the throttler directly from your user `Computation`. The base class has a `GetThrottlerOrThrow(id)` method that returns `IThroughputThrottlerPtr` — the standard YT throttler. You call `Throttle(amount)` on it and wait for the result. `GetThrottlerOrThrow` throws if the id is not declared in `throttlers`; `TryGetThrottler(id)` returns `nullptr` instead — handy when declaring a throttler in `dynamic_spec` is what turns throttling on for the matching entity (say, one throttler per model or per external service).

The returned pointer stays stable for the entire life of the Job: the factory replaces the internal client during `Reconfigure`, so you can store it in `DoInit` and keep using it without re-fetching. This does not apply to a `nullptr` from `TryGetThrottler`: it only means "not declared right now", and once the throttler is added to `dynamic_spec` you have to query it again.

Right now, this mechanism is supported only in C++.

## Monitoring {#monitoring}

The Flow dashboard has a separate tab called **Distributed throttler**. Key sensors include:

- Server-side (Controller): `value.rate` — the actual quota issued, `queue_size` — how many requests are waiting, `wait_time.max` — the tail of the wait time.
- Client-side (Worker, on each `Computation`): `consumed.rate` / `released.rate`, `wait_time.max` — the time the Job spent blocked in `Throttle()`, `request_count.rate` — the prefetch-RPC frequency.

## See also {#see-also}

- [Spec, DynamicSpec and Config](../../../flow/concepts/spec.md)
- [Computation](../../../flow/concepts/computation.md)