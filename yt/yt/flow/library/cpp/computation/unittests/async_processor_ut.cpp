#include "computation_test_harness.h"

#include <yt/yt/flow/library/cpp/computation/job_state/job_init_context.h>
#include <yt/yt/flow/library/cpp/computation/stores/timer_store.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/common/async_processor.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/output_collector.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/common/traverse.h>

#include <yt/yt/flow/library/cpp/distributed_throttler/config.h>
#include <yt/yt/flow/library/cpp/distributed_throttler/factory.h>
#include <yt/yt/flow/library/cpp/distributed_throttler/server.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/core/rpc/local_server.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/profiling/solomon/registry.h>

#include <algorithm>
#include <atomic>
#include <memory>

namespace NYT::NFlow {
namespace {

using namespace NComputationTesting;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;
using namespace testing;

////////////////////////////////////////////////////////////////////////////////

const TStreamId InputStreamId("input");
const TStreamId FetchTimerStreamId("fetch_timer");
const TStreamId PlainTimerStreamId("plain_timer");
const TStreamId OutputStreamId("output");
//! An output stream that does not depend on the timers, see #TAsyncProbe::BlockedOutputStreamId.
const TStreamId SideOutputStreamId("side_output");

//! The key of input #i (0-based) is |FirstInputKey + i|; the tasks of an input have its key as the event id.
constexpr ui64 FirstInputKey = 7;

const std::string InputRowsThrottlerId = "input_rows";
const std::string InputBytesThrottlerId = "input_bytes";

//! Long enough for the task to span several epochs.
constexpr auto TaskDuration = TDuration::MilliSeconds(200);
constexpr i64 ResultFactor = 10;
constexpr int MaxEpochs = 1000;

struct TFetchTask
    : public TYsonMessage
{
    i64 EventId = 0;
    i64 Attempt = 0;

    REGISTER_YSON_STRUCT(TFetchTask);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("event_id", &TThis::EventId)
            .Default();
        registrar.Parameter("attempt", &TThis::Attempt)
            .Default();
    }
};

struct TFetchResult
    : public TYsonMessage
{
    i64 Value = 0;
    std::string Padding;

    REGISTER_YSON_STRUCT(TFetchResult);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("value", &TThis::Value)
            .Default();
        registrar.Parameter("padding", &TThis::Padding)
            .Default();
    }
};

using TFetchResultPtr = TIntrusivePtr<TFetchResult>;

//! The byte size of |result| as the async processor manager measures it: the size of its binary YSON.
i64 GetResultByteSize(const TErrorOr<TFetchResultPtr>& result)
{
    auto yson = result.IsOK()
        ? ConvertToYsonString(*result.Value(), EYsonFormat::Binary)
        : ConvertToYsonString(TError(result), EYsonFormat::Binary);
    return std::ssize(yson.AsStringBuf());
}

//! What the computation does with a successful result.
DEFINE_ENUM(EResultAction,
    (Emit)
    (Ignore)
    (AddTwoTasks)
);

//! What a step that adds a task emits along with it; a restart replays such a step, so it may not emit.
DEFINE_ENUM(EOutputWithTask,
    (None)
    (Message)
    (Timer)
);

//! Settings and observations of the computation under test. Unless atomic, the fields are touched
//! from the serialized invoker of the computation only.
struct TAsyncProbe
{
    // Settings.
    //! Input #i (0-based) is sent once #i tasks have started.
    int InputCount = 1;
    EResultAction ResultAction = EResultAction::Emit;
    //! Whether the first task run fails; the computation retries a failed task.
    bool FailFirstRun = false;
    //! Whether a failed result adds a swift timer that starts the retry, rather than a task.
    bool RetryFromNewTimer = false;
    //! Whether the input adds a timer of the non-swift stream rather than of the swift one.
    bool UsePlainTimer = false;
    TSystemTimestamp TriggerTimestamp = TSystemTimestamp(1);
    //! If positive, the tasks wait until that many waiting epochs, see #WaitingEpochs.
    int GateEpochs = 0;
    //! If positive, the waiting epoch with this number raises the limit of running tasks to two
    //! in the dynamic spec; the change takes effect from the next epoch.
    int RaiseMaxRunningTasksAtWaitingEpoch = 0;
    IInvokerPtr TaskInvoker;
    //! What the timer emits along with its task.
    EOutputWithTask OutputWithTimerTask = EOutputWithTask::None;
    //! What a failed result emits along with the task that retries it.
    EOutputWithTask OutputWithRetryTask = EOutputWithTask::None;
    //! If set, the consumption of the input throttlers is read from it once the test finishes.
    NProfiling::TSolomonRegistryPtr ThrottlerRegistry;
    //! If positive, every task waits until that many tasks wait; the epoch that lets them go waits for their results.
    int GateTasks = 0;
    //! The length of the padding of every successful result.
    i64 ResultPadding = 0;
    //! If not empty, every input adds two swift timers of its key. The grouped step that gets both adds a task
    //! from the second one and emits a message whose parents are the timers of these indexes in the pair.
    std::vector<int> SiblingMessageParents;
    //! If positive, the controller blocks #BlockedOutputStreamId once the first task starts, the task waits for
    //! the block, and the controller unblocks the stream once the result has been ready for that many epochs.
    int OutputBlockedEpochsWithReadyResult = 0;
    TStreamId BlockedOutputStreamId = OutputStreamId;

    // Observations.
    int SentInputs = 0;
    int Epochs = 0;
    //! Calls of #DoProcessTimer().
    int Firings = 0;
    //! The attempts of the tasks in the order the start callback of the async processor gets them.
    std::vector<i64> TaskAttempts;
    //! The futures that the start callback returned.
    std::vector<TFuture<TFetchResultPtr>> TaskFutures;
    //! Decremented by a task once it is done, before its future is set.
    const std::shared_ptr<std::atomic<int>> TasksInFlight = std::make_shared<std::atomic<int>>(0);
    int MaxTasksInFlight = 0;
    int TimersTakenWhileTaskInFlight = 0;
    //! Epochs that saw a task in flight with every input sent and every timer registered, until the gate opened.
    int WaitingEpochs = 0;
    //! The minimum trigger timestamp of the swift timer stream in each waiting epoch.
    std::vector<std::optional<TSystemTimestamp>> WaitingMinTriggerTimestamps;
    const TPromise<void> Gate = NewPromise<void>();
    int ErrorResults = 0;
    //! Results processed by a step that added no task.
    int FinishedChains = 0;
    int Outputs = 0;
    TInputTimerConstPtr FirstFiredTimer;
    //! The root timer of the first result.
    TInputTimerConstPtr FirstResultRootTimer;
    //! The largest blocked time share that the status charged to the processor for the swift stream.
    double MaxBlockedTimeShare = 0;
    //! The limit types of the input limits that the status had in some epoch.
    THashSet<std::string> InputLimitTypes;
    //! The byte size of the messages, timers and results that the computation processed.
    i64 ProcessedByteSize = 0;
    //! What the input throttlers granted by the end of the test, see #ThrottlerRegistry.
    i64 ConsumedRows = 0;
    i64 ConsumedBytes = 0;
    //! The results and timers of every epoch: an "R" per result, then a "T" per timer.
    std::vector<std::string> Batches;
    //! The results that the computation processed in the current epoch.
    int EpochResults = 0;
    //! The byte size of the results that the computation processed.
    i64 ResultByteSize = 0;
    //! What the inflight metrics of the swift timer stream counted beyond its timers when the first result came.
    TInflightMetricsPtr ResultInflight;
    //! The results that the computation processed.
    int ProcessedResults = 0;
    //! The results that the computation processed while the output stream was blocked.
    int ResultsTakenWhileOutputBlocked = 0;
    //! The count of the timers in each epoch with the output stream blocked and a result ready.
    std::vector<i64> TimersWhileResultBlocked;

    int GetExpectedOutputs() const
    {
        return (ResultAction == EResultAction::Emit ? InputCount : 0) + (SiblingMessageParents.empty() ? 0 : InputCount);
    }

    //! Calls of the start callback of the async processor.
    int GetTaskRuns() const
    {
        return std::ssize(TaskAttempts);
    }
};

//! Returns what the throttlers with |throttlerIds| granted, as their sensors in |registry| tell.
THashMap<std::string, i64> ReadConsumedQuotas(
    const NProfiling::TSolomonRegistryPtr& registry,
    const std::vector<std::string>& throttlerIds)
{
    registry->ProcessRegistrations();
    registry->Collect();
    const auto dump = registry->DumpSensors();
    THashMap<std::string, i64> result;
    for (const auto& cube : dump.cubes()) {
        if (cube.name() != "yt/test/throttlers/consumed") {
            continue;
        }
        for (const auto& projection : cube.projections()) {
            if (!projection.has_counter()) {
                continue;
            }
            for (auto tagId : projection.tag_ids()) {
                const auto& tag = dump.tags(static_cast<int>(tagId));
                const std::string value(tag.value().data(), tag.value().size());
                if (tag.key() == "throttler_id" && std::ranges::find(throttlerIds, value) != throttlerIds.end()) {
                    auto& consumed = result[value];
                    consumed = std::max(consumed, projection.counter());
                }
            }
        }
    }
    return result;
}

//! Sets a swift timer for every input, runs a fetch task from the timer and handles its result as the probe says.
class TAsyncFetchTransform
    : public TTransformComputation
{
public:
    using TTransformComputation::DoProcessMessage;
    using TTransformComputation::DoProcessTimer;
    using TTransformComputation::TTransformComputation;

    TAsyncProbe* Probe = nullptr;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitAsyncProcessor(
            Fetcher_,
            "fetcher",
            BIND([probe = Probe] (TIntrusivePtr<TFetchTask> task) {
                return StartFetch(probe, std::move(task));
            }),
            BIND(&TAsyncFetchTransform::OnFetched, Unretained(this)));
    }

    void DoProcess(IInputContextPtr input, IOutputCollectorPtr output) override
    {
        auto& probe = *Probe;
        THROW_ERROR_EXCEPTION_IF(++probe.Epochs > MaxEpochs, "Too many epochs");
        // The results of the epoch are processed before this call.
        const int results = std::exchange(probe.EpochResults, 0);
        probe.Batches.push_back(std::string(results, 'R') + std::string(input->GetTimers().size(), 'T'));
        if (probe.OutputBlockedEpochsWithReadyResult > 0) {
            UpdateOutputBlock(results);
        }
        if (results > 0 && !probe.ResultInflight) {
            probe.ResultInflight = GetResultInflight();
        }
        auto status = GetStatus();
        for (const auto& [limitType, streamLimits] : status->InputLimits) {
            probe.InputLimitTypes.insert(limitType);
            if (limitType == MakeAsyncProcessorLimitType("fetcher")) {
                probe.MaxBlockedTimeShare = std::max(
                    probe.MaxBlockedTimeShare,
                    GetOrCrash(streamLimits, FetchTimerStreamId).BlockedTimeShare);
            }
        }
        if (probe.FinishedChains == probe.InputCount &&
            probe.Outputs == probe.GetExpectedOutputs() &&
            TimerStore_->GetCount() == 0)
        {
            // The sensors of the throttlers go away with the computation.
            if (probe.ThrottlerRegistry) {
                auto consumed = ReadConsumedQuotas(probe.ThrottlerRegistry, {InputRowsThrottlerId, InputBytesThrottlerId});
                probe.ConsumedRows = consumed[InputRowsThrottlerId];
                probe.ConsumedBytes = consumed[InputBytesThrottlerId];
            }
            THROW_ERROR_EXCEPTION("Test finished");
        }
        if (*probe.TasksInFlight > 0 &&
            !probe.Gate.IsSet() &&
            probe.SentInputs == probe.InputCount &&
            TimerStore_->GetCount() == probe.InputCount)
        {
            ++probe.WaitingEpochs;
            if (probe.WaitingEpochs == probe.RaiseMaxRunningTasksAtWaitingEpoch) {
                RequestMaxRunningTasks(2);
            }
            auto inflights = TimerStore_->BuildInflight();
            auto it = inflights.find(FetchTimerStreamId);
            probe.WaitingMinTriggerTimestamps.push_back(it == inflights.end() ? std::nullopt : it->second->MinTriggerTimestamp);
            if (probe.GateEpochs > 0 && probe.WaitingEpochs >= probe.GateEpochs) {
                probe.Gate.Set();
            }
        }
        if (probe.GateTasks > 0 && !probe.Gate.IsSet() && *probe.TasksInFlight == probe.GateTasks) {
            probe.Gate.Set();
            // The results come back to the manager while the epoch waits: the manager subscribed to the tasks
            // first, so its completion handlers are queued before the epoch resumes or yields.
            WaitFor(AllSet(probe.TaskFutures)).ThrowOnError();
            Yield();
        }
        TTransformComputation::DoProcess(std::move(input), std::move(output));
    }

    void DoProcessKey(IInputContextPtr input, IOutputCollectorPtr output) override
    {
        const auto& timers = input->GetTimers();
        if (Probe->SiblingMessageParents.empty() || timers.size() != 2) {
            TTransformComputation::DoProcessKey(std::move(input), std::move(output));
            return;
        }
        Probe->Firings += 2;
        const auto eventId = static_cast<i64>(GetColumnValue<ui64>(timers[1], "key"));
        output->SetParents({}, {timers[1]})->Add(Fetcher_, MakeTask(eventId, /*attempt*/ 0));
        std::vector<TInputTimerConstPtr> parents;
        for (int index : Probe->SiblingMessageParents) {
            parents.push_back(timers[index]);
        }
        auto builder = MakeOutputMessageBuilder(OutputStreamId);
        builder.Payload().SetValue(MakeUnversionedInt64Value(0), "value");
        output->SetParents({}, parents)->AddMessage(builder.Finish());
    }

    void DoProcessMessage(const TInputMessageConstPtr& message, IOutputCollectorPtr output) override
    {
        Probe->ProcessedByteSize += message->ByteSize;
        output->AddTimer(Probe->UsePlainTimer ? PlainTimerStreamId : FetchTimerStreamId, Probe->TriggerTimestamp);
        if (!Probe->SiblingMessageParents.empty()) {
            output->AddTimer(FetchTimerStreamId, Probe->TriggerTimestamp);
        }
    }

    void DoProcessTimer(const TInputTimerConstPtr& timer, IOutputCollectorPtr output) override
    {
        auto& probe = *Probe;
        ++probe.Firings;
        probe.ProcessedByteSize += timer->ByteSize;
        if (*probe.TasksInFlight > 0) {
            ++probe.TimersTakenWhileTaskInFlight;
        }
        if (!probe.FirstFiredTimer) {
            probe.FirstFiredTimer = timer;
        }
        const auto eventId = static_cast<i64>(GetColumnValue<ui64>(timer, "key"));
        output->Add(Fetcher_, MakeTask(eventId, /*attempt*/ 0));
        EmitAlongWithTask(probe.OutputWithTimerTask, output);
    }

    void OnFetched(const TCompletedAsyncTask<TFetchTask, TFetchResult>& completedTask, IOutputCollectorPtr output)
    {
        auto& probe = *Probe;
        if (!probe.FirstResultRootTimer) {
            probe.FirstResultRootTimer = completedTask.GetRootTimer();
        }
        ++probe.EpochResults;
        ++probe.ProcessedResults;
        const auto& task = completedTask.GetTask();
        const auto& result = completedTask.GetResult();
        const auto byteSize = GetResultByteSize(result);
        probe.ProcessedByteSize += byteSize;
        probe.ResultByteSize += byteSize;
        if (!result.IsOK()) {
            ++probe.ErrorResults;
            if (probe.RetryFromNewTimer) {
                output->AddTimer(FetchTimerStreamId, probe.TriggerTimestamp);
                return;
            }
            output->Add(Fetcher_, MakeTask(task.EventId, task.Attempt + 1));
            EmitAlongWithTask(probe.OutputWithRetryTask, output);
            return;
        }
        switch (probe.ResultAction) {
            case EResultAction::Emit: {
                auto builder = MakeOutputMessageBuilder(OutputStreamId);
                builder.Payload().SetValue(MakeUnversionedInt64Value(result.Value()->Value), "value");
                output->AddMessage(builder.Finish());
                break;
            }
            case EResultAction::Ignore:
                break;
            case EResultAction::AddTwoTasks:
                output->Add(Fetcher_, MakeTask(task.EventId, task.Attempt + 1));
                output->Add(Fetcher_, MakeTask(task.EventId, task.Attempt + 2));
                return;
        }
        ++probe.FinishedChains;
    }

private:
    TAsyncProcessorHandle<TFetchTask, TFetchResult> Fetcher_;
    bool OutputBlockRequested_ = false;
    bool OutputUnblockRequested_ = false;

    //! The inflight metrics of the swift timer stream in the last traverse minus those of its timers now.
    TInflightMetricsPtr GetResultInflight()
    {
        const auto& stream = *GetOrCrash(GetNodeTraverse()->Streams, FetchTimerStreamId)->InflightMetrics;
        const auto& timers = *GetOrCrash(TimerStore_->BuildInflight(), FetchTimerStreamId)->InflightMetrics;
        auto result = New<TInflightMetrics>();
        result->ReadyCount = stream.ReadyCount.value_or(0) - timers.ReadyCount.value_or(0);
        result->ReadyByteSize = stream.ReadyByteSize.value_or(0) - timers.ReadyByteSize.value_or(0);
        result->ByteSize = stream.ByteSize.value_or(0) - timers.ByteSize.value_or(0);
        return result;
    }

    //! Drives the block of the output stream, see #TAsyncProbe::OutputBlockedEpochsWithReadyResult; a request
    //! takes effect from the next epoch.
    void UpdateOutputBlock(int resultCount)
    {
        auto& probe = *Probe;
        if (!OutputBlockRequested_ && probe.GetTaskRuns() > 0) {
            OutputBlockRequested_ = true;
            RequestOutputBlock(/*block*/ true);
            return;
        }
        if (!GetDynamicPartitionSpec()->BlockedOutputStreams.contains(probe.BlockedOutputStreamId)) {
            return;
        }
        probe.ResultsTakenWhileOutputBlocked += resultCount;
        // The task finishes once the block is in effect, so its result comes while the stream is blocked.
        if (!probe.Gate.IsSet()) {
            probe.Gate.Set();
        }
        if (OutputUnblockRequested_ || GetResultInflight()->ReadyCount.value_or(0) == 0) {
            return;
        }
        probe.TimersWhileResultBlocked.push_back(TimerStore_->GetCount());
        if (std::ssize(probe.TimersWhileResultBlocked) >= probe.OutputBlockedEpochsWithReadyResult) {
            OutputUnblockRequested_ = true;
            RequestOutputBlock(/*block*/ false);
        }
    }

    //! Returns a copy of the dynamic context for the controller to change.
    TDynamicComputationContextPtr CloneDynamicContext() const
    {
        auto dynamicContext = New<TDynamicComputationContext>();
        static_cast<TDynamicComputationContextBase&>(*dynamicContext) = *GetDynamicContext();
        return dynamicContext;
    }

    //! Makes the controller block the output stream if |block| and unblock it otherwise.
    void RequestOutputBlock(bool block)
    {
        auto dynamicContext = CloneDynamicContext();
        auto blockedStreams = block ? TStringBuf(Probe->BlockedOutputStreamId.Underlying()) : TStringBuf();
        auto spec = Format("{computation_partition_spec={blocked_output_streams=[%v];};}", blockedStreams);
        dynamicContext->DynamicPartitionSpec = ConvertTo<NFlow::TDynamicPartitionSpecPtr>(TYsonStringBuf(spec));
        Reconfigure(std::move(dynamicContext));
    }

    //! Makes the controller set the limit of running tasks of the processor in the dynamic spec.
    void RequestMaxRunningTasks(i64 maxRunningTasks)
    {
        auto dynamicContext = CloneDynamicContext();
        auto dynamicSpec = CloneYsonStruct(GetDynamicSpec());
        auto processorSpec = New<TDynamicAsyncProcessorSpec>();
        processorSpec->MaxRunningTasks = NYTree::TSize(maxRunningTasks);
        dynamicSpec->AsyncProcessors["fetcher"] = std::move(processorSpec);
        dynamicContext->DynamicComputationSpec = std::move(dynamicSpec);
        Reconfigure(std::move(dynamicContext));
    }

    void EmitAlongWithTask(EOutputWithTask outputWithTask, const IOutputCollectorPtr& output) const
    {
        switch (outputWithTask) {
            case EOutputWithTask::None:
                break;
            case EOutputWithTask::Message: {
                auto builder = MakeOutputMessageBuilder(OutputStreamId);
                builder.Payload().SetValue(MakeUnversionedInt64Value(0), "value");
                output->AddMessage(builder.Finish());
                break;
            }
            case EOutputWithTask::Timer:
                output->AddTimer(PlainTimerStreamId, Probe->TriggerTimestamp);
                break;
        }
    }

    static TIntrusivePtr<TFetchTask> MakeTask(i64 eventId, i64 attempt)
    {
        auto task = New<TFetchTask>();
        task->EventId = eventId;
        task->Attempt = attempt;
        return task;
    }

    static TFuture<TFetchResultPtr> StartFetch(TAsyncProbe* probePtr, TIntrusivePtr<TFetchTask> task)
    {
        auto& probe = *probePtr;
        probe.TaskAttempts.push_back(task->Attempt);
        probe.MaxTasksInFlight = std::max(probe.MaxTasksInFlight, ++*probe.TasksInFlight);
        const bool fail = probe.FailFirstRun && probe.GetTaskRuns() == 1;
        const bool gated = probe.GateTasks > 0 || probe.GateEpochs > 0 || probe.OutputBlockedEpochsWithReadyResult > 0;
        auto gate = gated ? probe.Gate.ToFuture() : OKFuture;
        auto future = BIND([task = std::move(task), fail, gate = std::move(gate), tasksInFlight = probe.TasksInFlight, padding = probe.ResultPadding] {
            TDelayedExecutor::WaitForDuration(TaskDuration);
            WaitFor(gate).ThrowOnError();
            --*tasksInFlight;
            THROW_ERROR_EXCEPTION_IF(fail, "Fetch of event %v failed", task->EventId);
            auto result = New<TFetchResult>();
            result->Value = task->EventId * ResultFactor;
            result->Padding = std::string(padding, 'x');
            return result;
        })
            .AsyncVia(probe.TaskInvoker)
            .Run();
        probe.TaskFutures.push_back(future);
        return future;
    }
};

YT_FLOW_DEFINE_COMPUTATION(TAsyncFetchTransform);

//! An in-process distributed throttler service whose throttlers are unlimited.
class TUnlimitedThrottlerService
{
public:
    explicit TUnlimitedThrottlerService(const std::vector<std::string>& throttlerIds)
    {
        auto config = New<NDistributedThrottler::TDistributedThrottlerServiceConfig>();
        config->DrainPeriod = TDuration::MilliSeconds(10);
        for (const auto& throttlerId : throttlerIds) {
            config->Throttlers[throttlerId] = New<NDistributedThrottler::TDistributedThrottlerBucketConfig>();
        }
        Service_ = NDistributedThrottler::CreateDistributedThrottlerService(
            std::move(config),
            Queue_->GetInvoker(),
            NLogging::TLogger("ThrottlerService"));
        Server_->RegisterService(Service_->GetRpcService());
        Server_->Start();
    }

    ~TUnlimitedThrottlerService()
    {
        YT_UNUSED_FUTURE(Server_->Stop());
        Queue_->Shutdown();
    }

    NRpc::IChannelPtr CreateChannel() const
    {
        return NRpc::CreateLocalChannel(Server_);
    }

private:
    const TActionQueuePtr Queue_ = New<TActionQueue>("ThrottlerService");
    const NRpc::IServerPtr Server_ = NRpc::CreateLocalServer();
    NDistributedThrottler::IDistributedThrottlerServicePtr Service_;
};

class TAsyncProcessorTest
    : public ::testing::Test
{
protected:
    const TComputationSpecPtr Spec = New<TComputationSpec>();
    TAsyncProbe Probe;
    //! Overrides the byte limit of the batches of the computation.
    std::optional<i64> MaxBytesPerBatch;
    //! The limit of the async processor in the dynamic spec.
    std::optional<i64> MaxRunningTasks;
    //! Limits of a processor that the computation does not register.
    bool UnknownProcessorInDynamicSpec = false;
    //! Whether the input batches draw from unlimited rows and bytes throttlers of an in-process service;
    //! the probe reads what they granted, see #TAsyncProbe::ConsumedRows.
    bool ThrottleInput = false;

    TAsyncProcessorTest()
    {
        Probe.TaskInvoker = TaskQueue_->GetInvoker();

        Spec->ComputationClassName = TypeName<TAsyncFetchTransform>();
        Spec->GroupBySchema = GetTestKeySchema();
        Spec->InputStreamIds.insert(InputStreamId);
        Spec->OutputStreamIds.insert(OutputStreamId);
        auto swiftTimerSpec = New<TTimerSpec>();
        swiftTimerSpec->TimeType = ETimeType::CurrentTime;
        swiftTimerSpec->DeduplicateEqualTimestamps = false;
        swiftTimerSpec->Swift = true;
        Spec->TimerStreams[FetchTimerStreamId] = swiftTimerSpec;
        // The spec validation keeps a swift stream alone in its timer group, but the computation does not check
        // it: the plain current time stream shares the group of the swift one to fire right away.
        auto plainTimerSpec = New<TTimerSpec>();
        plainTimerSpec->TimeType = ETimeType::CurrentTime;
        plainTimerSpec->DeduplicateEqualTimestamps = false;
        Spec->TimerStreams[PlainTimerStreamId] = plainTimerSpec;
        // A failed result may add the swift timer that retries it, see #TAsyncProbe::RetryFromNewTimer.
        Spec->StreamsDependency[FetchTimerStreamId] = {InputStreamId, FetchTimerStreamId};
        Spec->AllowTimerSelfDependency = true;
        // A step of a chain may add a plain timer, see #EOutputWithTask.
        Spec->StreamsDependency[PlainTimerStreamId] = {InputStreamId, FetchTimerStreamId};
        // An output comes from the result of a task, whose stream is the one of its root timer.
        Spec->StreamsDependency[OutputStreamId] = {InputStreamId, FetchTimerStreamId};
        Spec->Parameters = ConvertTo<IMapNodePtr>(TYsonStringBuf("{}"));
    }

    ~TAsyncProcessorTest() override
    {
        TaskQueue_->Shutdown();
    }

    //! Runs the computation until it throws and returns the error.
    std::string Run()
    {
        if (ThrottleInput) {
            ThrottlerService_ = std::make_unique<TUnlimitedThrottlerService>(std::vector{InputRowsThrottlerId, InputBytesThrottlerId});
            Probe.ThrottlerRegistry = New<NProfiling::TSolomonRegistry>();
            Probe.ThrottlerRegistry->SetWindowSize(12);
        }
        return ToString(RunComputation(
            {
                .Spec = Spec,
                .OutputSchema = New<TTableSchema>(std::vector{TColumnSchema("value", EValueType::Int64)}),
                .OutputSchemaOverrides = {
                    {SideOutputStreamId, New<TTableSchema>(std::vector{TColumnSchema("side_value", EValueType::Int64)})},
                },
                .GetNextBatch = [this] {
                    std::vector<TInputMessageConstPtr> messages;
                    if (Probe.SentInputs < Probe.InputCount && Probe.GetTaskRuns() >= Probe.SentInputs) {
                        auto index = Probe.SentInputs++;
                        messages.push_back(MakeTestInputMessage(
                            InputStreamId,
                            TMessageId(Format("input-%v", index)),
                            MakeKey(FirstInputKey + index)));
                    }
                    return messages;
                },
                .OnRegisterOutputMessages = [this] (std::span<const TOutputMessageConstPtr> messages) {
                    for (const auto& message : messages) {
                        if (message->StreamId == OutputStreamId) {
                            ++Probe.Outputs;
                        }
                    }
                },
            },
            [this] (TComputationContextPtr context, TDynamicComputationContextPtr dynamicContext) {
                const auto& dynamicSpec = dynamicContext->DynamicComputationSpec;
                if (MaxBytesPerBatch) {
                    dynamicSpec->MaxBytesPerBatch = NYTree::TSize(*MaxBytesPerBatch);
                }
                if (MaxRunningTasks) {
                    auto processorSpec = New<TDynamicAsyncProcessorSpec>();
                    processorSpec->MaxRunningTasks = NYTree::TSize(*MaxRunningTasks);
                    dynamicSpec->AsyncProcessors["fetcher"] = std::move(processorSpec);
                }
                if (UnknownProcessorInDynamicSpec) {
                    dynamicSpec->AsyncProcessors["unknown"] = New<TDynamicAsyncProcessorSpec>();
                }
                if (ThrottlerService_) {
                    context->Profiler = NProfiling::TProfiler(Probe.ThrottlerRegistry, "/test");
                    THashMap<TThrottlerId, TDynamicThrottlerSpecPtr> throttlers;
                    for (const auto& throttlerId : {InputRowsThrottlerId, InputBytesThrottlerId}) {
                        throttlers.emplace(TThrottlerId(throttlerId), New<TDynamicThrottlerSpec>());
                    }
                    auto workerFactory = NDistributedThrottler::CreateWorkerThrottlerFactory(
                        throttlers,
                        ThrottlerService_->CreateChannel());
                    context->ThrottlerFactory = workerFactory->CreateFactory(
                        "test",
                        context->StatusProfiler,
                        context->Logger,
                        context->Profiler.WithPrefix("/throttlers"));
                    dynamicSpec->InputRowsThrottlerId = TThrottlerId(InputRowsThrottlerId);
                    dynamicSpec->InputBytesThrottlerId = TThrottlerId(InputBytesThrottlerId);
                }
                auto computation = New<TAsyncFetchTransform>(std::move(context), std::move(dynamicContext));
                computation->Probe = &Probe;
                return computation;
            }));
    }

    //! Runs the computation whose timer, or failed result if |retry|, emits |outputWithTask| along with its task
    //! and expects the epoch to fail.
    void ExpectHeldRootTimerOutputError(bool retry, EOutputWithTask outputWithTask)
    {
        Probe.FailFirstRun = retry;
        (retry ? Probe.OutputWithRetryTask : Probe.OutputWithTimerTask) = outputWithTask;
        auto error = Run();
        EXPECT_THAT(error, HasSubstr("outputs are allowed only at the step that acknowledges the timer"));
        EXPECT_THAT(error, HasSubstr(outputWithTask == EOutputWithTask::Message ? "an output message" : "a timer"));

        EXPECT_EQ(Probe.Firings, 1);
        EXPECT_EQ(Probe.ErrorResults, retry ? 1 : 0);
        // The retry is not started.
        EXPECT_EQ(Probe.GetTaskRuns(), retry ? 1 : 0);
        EXPECT_EQ(Probe.Outputs, 0);
    }

    //! Runs the computation, see #TAsyncProbe::SiblingMessageParents, and expects the epoch to fail.
    void ExpectSiblingMessageError()
    {
        auto error = Run();
        EXPECT_THAT(error, HasSubstr("adds a task and an output message in one epoch"));
        EXPECT_EQ(Probe.Firings, 2);
        EXPECT_EQ(Probe.GetTaskRuns(), 0);
        EXPECT_EQ(Probe.Outputs, 0);
    }

    //! The batches with results or timers since the first one with a result.
    std::vector<std::string> GetBatchesSinceFirstResult() const
    {
        auto it = std::ranges::find_if(Probe.Batches, [] (const std::string& batch) {
            return batch.starts_with('R');
        });
        std::vector<std::string> result;
        for (; it != Probe.Batches.end(); ++it) {
            if (!it->empty()) {
                result.push_back(*it);
            }
        }
        return result;
    }

private:
    const TActionQueuePtr TaskQueue_ = New<TActionQueue>("AsyncProcessorTask");
    std::unique_ptr<TUnlimitedThrottlerService> ThrottlerService_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TAsyncProcessorTest, ResultOfTaskOfSwiftTimerProducesOutputAndUnregistersRootTimer)
{
    EXPECT_THAT(Run(), HasSubstr("Test finished"));

    EXPECT_EQ(Probe.Firings, 1);
    EXPECT_EQ(Probe.GetTaskRuns(), 1);
    EXPECT_EQ(Probe.ErrorResults, 0);
    EXPECT_EQ(Probe.FinishedChains, 1);
    EXPECT_EQ(Probe.Outputs, 1);
    // The root timer of the result is the timer that added the task.
    ASSERT_TRUE(Probe.FirstFiredTimer);
    EXPECT_EQ(Probe.FirstResultRootTimer.Get(), Probe.FirstFiredTimer.Get());
    EXPECT_TRUE(Probe.FirstResultRootTimer->Key == MakeKey(FirstInputKey));
}

TEST_F(TAsyncProcessorTest, FailedTaskIsRetriedFromItsResult)
{
    Probe.FailFirstRun = true;
    EXPECT_THAT(Run(), HasSubstr("Test finished"));

    EXPECT_EQ(Probe.Firings, 1);
    EXPECT_EQ(Probe.GetTaskRuns(), 2);
    EXPECT_THAT(Probe.TaskAttempts, ElementsAre(0, 1));
    EXPECT_EQ(Probe.ErrorResults, 1);
    EXPECT_EQ(Probe.FinishedChains, 1);
    EXPECT_EQ(Probe.Outputs, 1);
}

TEST_F(TAsyncProcessorTest, FailedTaskIsRetriedFromNewTimerOfItsResult)
{
    Probe.FailFirstRun = true;
    Probe.RetryFromNewTimer = true;
    EXPECT_THAT(Run(), HasSubstr("Test finished"));

    // The failed result acknowledges its root timer and adds the timer that starts the retry.
    EXPECT_EQ(Probe.Firings, 2);
    EXPECT_EQ(Probe.GetTaskRuns(), 2);
    EXPECT_THAT(Probe.TaskAttempts, ElementsAre(0, 0));
    EXPECT_EQ(Probe.ErrorResults, 1);
    EXPECT_EQ(Probe.FinishedChains, 1);
    EXPECT_EQ(Probe.Outputs, 1);
}

TEST_F(TAsyncProcessorTest, HeldSwiftTimerIsNotOfferedAgainAndKeepsInflightWhileTaskRuns)
{
    Probe.GateEpochs = 3;
    EXPECT_THAT(Run(), HasSubstr("Test finished"));

    // The epochs go on while the task runs.
    EXPECT_EQ(Probe.WaitingEpochs, 3);
    EXPECT_THAT(Probe.WaitingMinTriggerTimestamps, Each(Optional(TSystemTimestamp(1))));
    EXPECT_EQ(Probe.Firings, 1);
    EXPECT_EQ(Probe.TimersTakenWhileTaskInFlight, 0);
    EXPECT_EQ(Probe.GetTaskRuns(), 1);
    EXPECT_EQ(Probe.Outputs, 1);
}

TEST_F(TAsyncProcessorTest, TwoTasksForOneRootTimerInOneEpochAreRejected)
{
    Probe.ResultAction = EResultAction::AddTwoTasks;
    auto error = Run();
    EXPECT_THAT(error, HasSubstr("linear"));
    EXPECT_THAT(error, Not(HasSubstr("Test finished")));
    EXPECT_EQ(Probe.GetTaskRuns(), 1);
}

TEST_F(TAsyncProcessorTest, TaskOfNonSwiftTimerIsRejected)
{
    Probe.UsePlainTimer = true;
    EXPECT_THAT(Run(), HasSubstr("is not swift"));
    EXPECT_EQ(Probe.Firings, 1);
    EXPECT_EQ(Probe.GetTaskRuns(), 0);
}

TEST_F(TAsyncProcessorTest, SwiftTimerWaitsWhileRunningTasksReachMaxRunningTasks)
{
    Probe.InputCount = 2;
    MaxRunningTasks = 1;
    Probe.GateEpochs = 3;
    EXPECT_THAT(Run(), HasSubstr("Test finished"));

    // The second timer was due in the waiting epochs but was not taken.
    EXPECT_EQ(Probe.WaitingEpochs, 3);
    // The wait is charged to the processor by its name.
    EXPECT_GT(Probe.MaxBlockedTimeShare, 0);
    EXPECT_THAT(Probe.InputLimitTypes, ElementsAre("async_processor/fetcher"));
    EXPECT_EQ(Probe.TimersTakenWhileTaskInFlight, 0);
    EXPECT_EQ(Probe.MaxTasksInFlight, 1);
    EXPECT_EQ(Probe.Firings, 2);
    EXPECT_EQ(Probe.GetTaskRuns(), 2);
    EXPECT_EQ(Probe.FinishedChains, 2);
    EXPECT_EQ(Probe.Outputs, 2);
}

TEST_F(TAsyncProcessorTest, RaisedMaxRunningTasksInDynamicSpecLetsWaitingSwiftTimerGo)
{
    Probe.InputCount = 2;
    MaxRunningTasks = 1;
    Probe.RaiseMaxRunningTasksAtWaitingEpoch = 1;
    Probe.GateEpochs = 3;
    EXPECT_THAT(Run(), HasSubstr("Test finished"));

    // The second timer was taken while the first task was running, once the raised limit took effect.
    EXPECT_EQ(Probe.TimersTakenWhileTaskInFlight, 1);
    EXPECT_EQ(Probe.MaxTasksInFlight, 2);
    EXPECT_EQ(Probe.Firings, 2);
    EXPECT_EQ(Probe.GetTaskRuns(), 2);
    EXPECT_EQ(Probe.FinishedChains, 2);
    EXPECT_EQ(Probe.Outputs, 2);
}

TEST_F(TAsyncProcessorTest, DynamicSpecOfUnknownProcessorFailsJob)
{
    UnknownProcessorInDynamicSpec = true;
    EXPECT_THAT(Run(), HasSubstr("does not register"));
    EXPECT_EQ(Probe.Firings, 0);
    EXPECT_EQ(Probe.GetTaskRuns(), 0);
}

TEST_F(TAsyncProcessorTest, SwiftTimerBeyondCurrentTimestampIsRejected)
{
    Probe.TriggerTimestamp = TSystemTimestamp(1'000'000'000);
    EXPECT_THAT(Run(), HasSubstr("beyond the current timestamp"));
    EXPECT_EQ(Probe.Firings, 0);
    EXPECT_EQ(Probe.GetTaskRuns(), 0);
}

TEST_F(TAsyncProcessorTest, IgnoredResultUnregistersRootTimer)
{
    // A handler that adds nothing ends the chain.
    Probe.ResultAction = EResultAction::Ignore;
    EXPECT_THAT(Run(), HasSubstr("Test finished"));

    EXPECT_EQ(Probe.Firings, 1);
    EXPECT_EQ(Probe.GetTaskRuns(), 1);
    EXPECT_EQ(Probe.FinishedChains, 1);
    EXPECT_EQ(Probe.Outputs, 0);
}

TEST_F(TAsyncProcessorTest, OutputMessageAlongWithTaskOfTimerFailsEpoch)
{
    ExpectHeldRootTimerOutputError(/*retry*/ false, EOutputWithTask::Message);
}

TEST_F(TAsyncProcessorTest, TimerAlongWithTaskOfTimerFailsEpoch)
{
    ExpectHeldRootTimerOutputError(/*retry*/ false, EOutputWithTask::Timer);
}

TEST_F(TAsyncProcessorTest, OutputMessageAlongWithRetryTaskFailsEpoch)
{
    ExpectHeldRootTimerOutputError(/*retry*/ true, EOutputWithTask::Message);
}

TEST_F(TAsyncProcessorTest, TimerAlongWithRetryTaskFailsEpoch)
{
    ExpectHeldRootTimerOutputError(/*retry*/ true, EOutputWithTask::Timer);
}

TEST_F(TAsyncProcessorTest, ResultsTakeTheirShareOfInputThrottlerQuotas)
{
    ThrottleInput = true;
    EXPECT_THAT(Run(), HasSubstr("Test finished"));

    ASSERT_EQ(Probe.FinishedChains, 1);
    // The input message, its timer and the result of the task.
    EXPECT_EQ(Probe.ConsumedRows, 3);
    EXPECT_EQ(Probe.ConsumedBytes, Probe.ProcessedByteSize);
    EXPECT_GT(Probe.ResultByteSize, 0);
}

TEST_F(TAsyncProcessorTest, MessageOfSiblingAndHeldTimerFailsEpoch)
{
    Probe.SiblingMessageParents = {0, 1};
    ExpectSiblingMessageError();
}

TEST_F(TAsyncProcessorTest, MessageOfHeldTimerAndSiblingFailsEpoch)
{
    Probe.SiblingMessageParents = {1, 0};
    ExpectSiblingMessageError();
}

TEST_F(TAsyncProcessorTest, MessageOfSiblingOfHeldTimerCommits)
{
    Probe.SiblingMessageParents = {0};
    EXPECT_THAT(Run(), HasSubstr("Test finished"));

    EXPECT_EQ(Probe.Firings, 2);
    EXPECT_EQ(Probe.GetTaskRuns(), 1);
    EXPECT_EQ(Probe.FinishedChains, 1);
    // The message of the sibling and the one of the result.
    EXPECT_EQ(Probe.Outputs, 2);
}

TEST_F(TAsyncProcessorTest, ResultOverByteLimitLeavesRoomForDueTimer)
{
    MaxBytesPerBatch = 1024;
    Probe.ResultPadding = 2048;
    Probe.InputCount = 2;
    // The second timer waits while the first task runs, so it is due once the first result is ready.
    MaxRunningTasks = 1;
    EXPECT_THAT(Run(), HasSubstr("Test finished"));

    // The results have a batch of their own.
    EXPECT_GT(Probe.ResultByteSize, 2 * *MaxBytesPerBatch);
    EXPECT_THAT(GetBatchesSinceFirstResult(), ElementsAre("RT", "R"));
}

TEST_F(TAsyncProcessorTest, ReadyResultsCountInInflightOfStreamOfTheirRootTimers)
{
    Probe.InputCount = 4;
    // The four results wait for an epoch at once.
    Probe.GateTasks = 4;
    EXPECT_THAT(Run(), HasSubstr("Test finished"));

    ASSERT_TRUE(Probe.ResultInflight);
    EXPECT_EQ(Probe.ResultInflight->ReadyCount, 4);
    EXPECT_EQ(Probe.ResultInflight->ReadyByteSize, Probe.ResultByteSize);
    EXPECT_EQ(Probe.ResultInflight->ByteSize, Probe.ResultByteSize);
}

TEST_F(TAsyncProcessorTest, ReadyResultWaitsWhileOutputIsBlockedByController)
{
    Probe.OutputBlockedEpochsWithReadyResult = 3;
    // The test finishes once the root timer is unregistered.
    EXPECT_THAT(Run(), HasSubstr("Test finished"));

    // While the output is blocked, the result stays ready and its root timer stays held.
    EXPECT_EQ(Probe.ResultsTakenWhileOutputBlocked, 0);
    // The processor is not saturated, so the wait is not charged to it.
    EXPECT_THAT(Probe.InputLimitTypes, ::testing::IsEmpty());
    EXPECT_THAT(Probe.TimersWhileResultBlocked, ElementsAre(1, 1, 1));
    // Once unblocked, the result is processed exactly once.
    EXPECT_EQ(Probe.ProcessedResults, 1);
    EXPECT_EQ(Probe.Firings, 1);
    EXPECT_EQ(Probe.GetTaskRuns(), 1);
    EXPECT_EQ(Probe.FinishedChains, 1);
    EXPECT_EQ(Probe.Outputs, 1);
}

TEST_F(TAsyncProcessorTest, ReadyResultIsProcessedWhileOutputOfAnotherStreamIsBlocked)
{
    // The side output depends on the input stream only, so its block does not hold back the results.
    Spec->OutputStreamIds.insert(SideOutputStreamId);
    Spec->StreamsDependency[SideOutputStreamId] = {InputStreamId};
    Probe.OutputBlockedEpochsWithReadyResult = 1;
    Probe.BlockedOutputStreamId = SideOutputStreamId;
    EXPECT_THAT(Run(), HasSubstr("Test finished"));

    EXPECT_EQ(Probe.ResultsTakenWhileOutputBlocked, 1);
    EXPECT_EQ(Probe.ProcessedResults, 1);
    EXPECT_EQ(Probe.Outputs, 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
