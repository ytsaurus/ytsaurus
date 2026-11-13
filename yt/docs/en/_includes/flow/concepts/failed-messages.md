# Saving failed messages

If a message cannot be processed now and your application allows deferring its processing, save it with `output->AddFailedMessage(message, error)` and return from its handler. Flow writes it to the `<pipeline_path>/failed_messages` system table and continues processing other messages. This avoids repeated job failures caused by the same invalid data.

The mechanism is available for C++ functions running under the `NYT::NFlow::TProcessFunctionComputation` adapter inside the worker. Source, Swift, [process function graphs](../../../flow/cpp/process-functions.md#function-graph), and companions are not supported yet.

1. Enable failed-message capture at the root of the pipeline's [static spec](../../../flow/concepts/spec.md). It is disabled by default:

   ```yson
   {
       failure_handler = {
           enabled = %true;
       };
   }
   ```

2. In your process function, explicitly select the messages whose processing can be deferred. For example, for streams with an `int64` column named `value`, this handler saves messages with a negative value and forwards the rest to the `output` stream:

   ```cpp
   void ProcessMessage(
       const TInputMessageConstPtr& message,
       const IOutputCollectorPtr& output,
       const IRuntimeContextPtr& context) override
   {
       if (GetColumnValue<i64>(*message, "value") < 0) {
           output->AddFailedMessage(message, TError("value must be non-negative"));
           return;
       }

       output->AddMessage(context->ConvertToOutputMessage(*message, TStreamId("output")));
   }
   ```

Pass the original `message` received by `ProcessMessage`. Exceptions are not caught automatically: an unhandled exception still fails the epoch.

Writes to `failed_messages` commit atomically with the epoch's state and outputs. After a successful commit, the failed input is considered processed; if the write fails, the epoch does not commit. The table stores the payload, stream spec version ID (`stream_spec_id`), key, message and stream IDs, event timestamp, and the supplied error in `diagnostic`. The payload schema is stored in the pipeline's persistent stream spec history and is identified by `stream_spec_id`. The `failed_at` field contains the batch time in milliseconds.

`AddFailedMessage` does not undo earlier state changes, emitted messages, or timers. Validate the input before these actions if you need to defer them for a failed message.

Currently, the mechanism saves messages for later investigation. Repair and replay will be added separately; Flow cannot automatically return saved messages to processing yet.

## Metrics

The `yt.flow.worker.computation.tables.captured_rows` counter tracks messages accepted for capture into `failed_messages` after removing duplicates within the batch. It increments when preparing the write, before the epoch commits. Transaction retries do not increment it again.

The metric is exported with the `table=failed_messages`, `computation_id`, and `tag=<computation_id>:failed_messages` labels. Monitoring exposes the message capture rate as `yt.flow.worker.computation.tables.captured_rows.rate`.
