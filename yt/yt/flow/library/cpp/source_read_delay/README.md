# Delayed source Offered and Ready

Swift ordered source computations estimate Offered and eligible unread Ready whenever
`watermark_alignment.read_delays` is nonempty. Parameters live in
`source_read_delay_estimator` in the **dynamic computation spec** and can be changed
without stopping the pipeline. Omitting the section uses defaults; it does not disable
estimation. Publication and replay rules are unchanged.

`RegisterInput` observes original input count/bytes, original queue SystemTimestamp
and the maximum EventTimestamp of the corresponding output batch. Swift captures
input work before expression filtering and pairs timestamps before rewriting output
SystemTimestamp. Filtered or output-free input contributes to density without
inventing a timestamp offset. IDs are registered in source order.

The estimator measures historical producer density in data time: all work at a queue
timestamp is accumulated until the next timestamp, divided by the elapsed queue
time, then smoothed. Reading the same interval faster during catch-up does not
increase its estimated production rate. System/event offset is smoothed using
input-count weights normalized by the learned count rate.

With `offset = mean(system_time - event_time)`:

```
C = event_threshold + offset
F = learned read_system_time
U = observation_time
alpha = clamp((C - F) / (U - F), 0, 1)
Offered = ((1 - alpha) * historical_density + alpha * EMA(New)) * EMA(gate_speed)
```

Counts and bytes use separate densities. At or beyond the current tail, Offered is
New. Read-ahead uses the nearest learned density. Gate speed is measured in event
time; changes to the estimated offset do not count as gate advancement.

Ready retains the source metric's unread-work meaning. It excludes already-read
batches, including delayed and published-but-unacknowledged batches:

```
Ready = source_unread * clamp((C - current_read_system_time) / (U - current_read_system_time), 0, 1)
```

This assumes uniform average density over unread work. Offered interpolates endpoint
rates; Ready interpolates cumulative volume. They are separate approximations. A
stationary gate can have Ready backlog while Offered approaches zero. Ready is bounded
by source unread volume, and empty unread is known to be zero even before training.

The estimator state has bounded size independent of ReadDelay and backlog. Learned density, pending timestamp-group
work, timestamp offset and the last observed message ID are stored under the source
key in `$timestamp_memory`, atomically with source state. Replayed IDs do not train
again. The transient read frontier is rebuilt during replay because the source reads
again from its persisted offset; nonempty Ready is unknown until the first read.
Status-only New/gate smoothing is updated after commit and saved in the following
epoch. Restart can lose its latest sample, but does not require relearning density.

`Estimate` returns independent optional Offered and Ready values. Missing observations,
cold or stale density, and the first status after a long gap can leave Offered unknown;
the computation does not substitute New or zero. The default smoothing period is one
minute; density expires after five minutes without new data-time progress. `Reset`
explicitly discards learning. Dynamic tuning preserves existing learning.

The model assumes recent queue writes track wall time and that historical timestamp
skew remains representative near the target gate. Nonlinear rate changes between
observed endpoints and changing delivery latency cause error. Without connector trim
signals, sparse input and dropped unread input cannot be distinguished. The module
makes no exact trim-detection claim and does not change TrueRate's catch-up demand.

The benchmark under `yandex/benchmarks/source_read_delay` runs this compiled module
against captured write rates with simulated reading. It measures interpolation error,
not runtime throughput, ACK behavior or actual Colibri partition telemetry.


## Exact delayed-input status and estimate warnings

The shared `computation/read_delay` component defines the dependency threshold,
maximum candidate-output timestamp, contiguous publication prefix and original-input
observations. Swift publishes an immutable `partition_status.read_delay` snapshot:
`held_count`, `held_byte_size`, `held_batch_count`, `event_threshold`,
`first_blocking_message_id` and `required_event_threshold`. Held totals count original
split input before filtering and exclude published batches even while awaiting ACK.
A fully filtered batch is completed immediately and is not held. During drain,
`draining` is true and admission uses saved timestamp-memory progress; an event
threshold cannot unblock it, so `required_event_threshold` is absent.

Offered and Ready for unread input remain **estimates**. A retryable status warning
at `read_delay_estimate` preserves their numerical values while reporting observed
instability. Count and byte density mismatches are checked separately: the larger
of historical density and smoothed New must not exceed `max_density_ratio` (default
4) times the smaller. A batch system/event offset differing from its previously
smoothed value by more than `max_event_offset_deviation` (default 1 minute) retains
a warning for `warning_retention` (default 5 minutes) after the last such observation.
These controls affect warnings only; they do not change the interpolation or replace
an available estimate with unknown. Existing missing-data and freshness rules remain.

A warning is evidence of observed instability, not an error bound. No warning is
not proof of accuracy: changes wholly inside unread data are unobservable here.
For example, at 5-hour reader lag and a 1-hour delay, a count-rate change from 100
to 10,000 records/s at constant byte rate can make Offered reach 8,020 while the
actual eligible rate is still 100. Ready can be 29,952,000 versus 1,440,000 records;
the bytes remain accurate in this example. Five-minute alternation also produces
large errors when the gate and reader do not align with the writer's period.
With mixed timestamps, one fresh message can hold its entire raw batch and every
following batch; a mean offset cannot reconstruct those unread publication barriers.
