#include <yt/yt/flow/library/cpp/worker/input_buffer.h>
#include <yt/yt/flow/library/cpp/worker/job.h>
#include <yt/yt/flow/library/cpp/worker/job_spec.h>
#include <yt/yt/flow/library/cpp/worker/traced_invoker.h>

#include <yt/yt/flow/library/cpp/computation/universal_controller.h>

#include <yt/yt/flow/library/cpp/common/computation.h>
#include <yt/yt/flow/library/cpp/common/computation_statistics.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource_manager.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/serialized_invoker.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/yt/system/thread_id.h>

#include <atomic>
#include <future>

namespace NYT::NFlow::NWorker {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

struct TKeySnapshotProbe
{
    TPromise<void> Started = NewPromise<void>();
    TPromise<void> Finish = NewPromise<void>();
    bool Enable = true;
};

std::atomic<TKeySnapshotProbe*>& GetActiveKeySnapshotProbe()
{
    static std::atomic<TKeySnapshotProbe*> Probe = nullptr;
    return Probe;
}

class TTerminalKeySnapshotComputation
    : public IComputation
{
public:
    using TComputationController = TUniversalComputationController;

    static constexpr auto InputAcknowledgementPolicy = EInputAcknowledgementPolicy::PersistAfterEpoch;

    TTerminalKeySnapshotComputation(TComputationContextPtr, TDynamicComputationContextPtr)
        : Probe_(GetActiveKeySnapshotProbe().load())
    {
        for (int key = 0; key < 1200; ++key) {
            Keys_.emplace(MakeKey(key), TInstant::Now());
        }
    }

    TParametersPtr GetParametersBase() const override
    {
        return nullptr;
    }

    TDynamicParametersPtr GetDynamicParametersBase() const override
    {
        return nullptr;
    }

    TDynamicPartitionSpecPtr GetDynamicPartitionSpecBase() const override
    {
        return nullptr;
    }

    void Run(const IComputationRunContextPtr&) override
    {
        THROW_ERROR_EXCEPTION(EErrorCode::PartitionKeyLimitExceeded, "Key limit");
    }

    void Reconfigure(TDynamicComputationContextPtr) override
    { }

    void SubscribeOnReconfigure(TCallback<void()>, EWatchReconfigure) override
    { }

    void UpdateWatermarkState(TWatermarkStatePtr) override
    { }

    void SetInputTraverse(THashMap<TStreamId, TStreamTraverseDataPtr>) override
    { }

    TComputationStatusPtr GetStatus() override
    {
        auto status = New<TComputationStatus>();
        if (Probe_->Enable) {
            status->ObservedKeys = Keys_;
        }
        return status;
    }

protected:
    TKeySnapshotProbe* const Probe_;

private:
    THashMap<TKey, TInstant> Keys_;
};

YT_FLOW_DEFINE_COMPUTATION(TTerminalKeySnapshotComputation);

class TOrdinaryFailureKeySnapshotComputation
    : public TTerminalKeySnapshotComputation
{
public:
    using TTerminalKeySnapshotComputation::TTerminalKeySnapshotComputation;

    void Run(const IComputationRunContextPtr&) override
    {
        THROW_ERROR_EXCEPTION("Ordinary failure");
    }
};

class TSuccessfulKeySnapshotComputation
    : public TTerminalKeySnapshotComputation
{
public:
    using TTerminalKeySnapshotComputation::TTerminalKeySnapshotComputation;

    void Run(const IComputationRunContextPtr&) override
    { }
};

YT_FLOW_DEFINE_COMPUTATION(TOrdinaryFailureKeySnapshotComputation);
YT_FLOW_DEFINE_COMPUTATION(TSuccessfulKeySnapshotComputation);

class TRunningKeySnapshotComputation
    : public TTerminalKeySnapshotComputation
{
public:
    using TTerminalKeySnapshotComputation::TTerminalKeySnapshotComputation;

    void Run(const IComputationRunContextPtr&) override
    {
        Probe_->Started.Set();
        WaitFor(Probe_->Finish.ToFuture()).ThrowOnError();
    }
};

YT_FLOW_DEFINE_COMPUTATION(TRunningKeySnapshotComputation);

class TInputBytesComputation
    : public TTerminalKeySnapshotComputation
{
public:
    using TTerminalKeySnapshotComputation::TTerminalKeySnapshotComputation;

    void Run(const IComputationRunContextPtr& context) override
    {
        for (int batchIndex = 0; batchIndex < 2; ++batchIndex) {
            while (WaitFor(context->GetNextBatch({TStreamId("input")})).ValueOrThrow().empty()) {
                TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(1));
            }

            if (batchIndex == 0) {
                Probe_->Started.Set();
            }
        }

        WaitFor(Probe_->Finish.ToFuture()).ThrowOnError();
    }

    TComputationStatusPtr GetStatus() override
    {
        auto status = New<TComputationStatus>();
        status->NodeTraverse = New<TNodeTraverseData>();
        return status;
    }
};

YT_FLOW_DEFINE_COMPUTATION(TInputBytesComputation);

class TKeyCountJobTest
    : public ::testing::Test
{
protected:
    const TActionQueuePtr Queue_ = New<TActionQueue>("KeyCountJobTest");
    const TDynamicJobSpecPtr Dynamic_ = New<TDynamicJobSpec>();
    TKeySnapshotProbe Probe_;
    IJobPtr Job_;
    TActionQueuePtr InputBufferQueue_;
    IInvokerPtr InputBufferPoolInvoker_;
    TJobCpuTimeAccountantPtr CpuTimeAccountant_;

    void SetUp() override
    {
        YT_VERIFY(!GetActiveKeySnapshotProbe().exchange(&Probe_));
        Dynamic_->DynamicComputationSpec = New<TDynamicComputationSpec>();
        Dynamic_->DynamicComputationPartitionSpec = New<TDynamicPartitionSpec>();
    }

    void TearDown() override
    {
        Probe_.Finish.TrySet();
        if (Job_) {
            WaitFor(Job_->Stop()).ThrowOnError();
        }
        Queue_->Shutdown();
        if (InputBufferQueue_) {
            InputBufferQueue_->Shutdown();
        }
        GetActiveKeySnapshotProbe().store(nullptr);
    }

    void Start(std::string className, bool enable = true, bool input = false)
    {
        auto jobContext = New<TJobContext>();
        jobContext->ControlSerializedInvoker = Queue_->GetInvoker();
        jobContext->SerializedInvoker = Queue_->GetInvoker();
        jobContext->PoolInvoker = Queue_->GetInvoker();
        jobContext->InputBufferPoolInvoker = InputBufferPoolInvoker_ ? InputBufferPoolInvoker_ : Queue_->GetInvoker();
        if (CpuTimeAccountant_) {
            jobContext->PoolInvoker = CreateTracedInvoker(
                jobContext->PoolInvoker,
                NTracing::TTraceContext::NewRoot("JobTest"),
                CpuTimeAccountant_);
            jobContext->SerializedInvoker = CreateSerializedInvoker(jobContext->PoolInvoker);
        }
        auto resourceContext = New<TResourceManagerContext>();
        resourceContext->Invoker = Queue_->GetInvoker();
        resourceContext->StatusProfiler = CreateSyncStatusProfiler();
        jobContext->ResourceManager = CreateResourceManager(resourceContext, {}, {});
        auto spec = New<TJobSpec>();
        spec->ComputationSpec = New<TComputationSpec>();
        spec->ComputationSpec->ComputationClassName = std::move(className);
        TJobStreamLimitUsageStates streamLimits;
        if (input) {
            const TStreamId streamId("input");
            spec->ComputationSpec->InputStreamIds.insert(streamId);
            spec->ComputationSpec->HeavyHitters.Window = TDuration::MilliSeconds(10);
            Dynamic_->DynamicComputationSpec->BatchDuration = TDuration::Zero();
            streamLimits.Input.emplace(streamId, New<TStreamLimitUsageState>());
        }
        Probe_.Enable = enable;
        spec->Partition = New<TPartition>();
        spec->Partition->PartitionId = TPartitionId(TGuid::Create());
        spec->Partition->ComputationId = TComputationId("key-count");
        spec->Partition->State = EPartitionState::Executing;
        spec->Job = New<TJob>();
        spec->Job->JobId = TJobId(TGuid::Create());
        spec->Job->PartitionId = spec->Partition->PartitionId;
        Job_ = CreateJob(jobContext, std::move(streamLimits), 1, spec, Dynamic_, New<TToPartitionTraverseData>(), New<TWatermarkState>());
        WaitFor(Job_->Start()).ThrowOnError();
    }

    void WaitForDuration(TDuration duration)
    {
        WaitFor(BIND([duration] {
            TDelayedExecutor::WaitForDuration(duration);
        })
                .AsyncVia(Queue_->GetInvoker())
                .Run())
            .ThrowOnError();
    }

    TJobStatusPtr WaitForFinished()
    {
        for (int iteration = 0; iteration < 100; ++iteration) {
            auto status = WaitFor(Job_->GetStatus()).ValueOrThrow();
            if (status->IsFinished) {
                return status;
            }
            WaitForDuration(TDuration::MilliSeconds(10));
        }
        return WaitFor(Job_->GetStatus()).ValueOrThrow();
    }

    void CheckTerminalSnapshotSurvivesStop(std::string className, int errorCode)
    {
        Start(std::move(className));
        auto before = WaitForFinished();
        ASSERT_TRUE(before->IsFinished);
        EXPECT_EQ(before->Error.GetCode(), errorCode);
        ASSERT_TRUE(before->ObservedKeys);
        WaitFor(Job_->Stop()).ThrowOnError();
        for (int iteration = 0; iteration < 3; ++iteration) {
            auto after = WaitFor(Job_->GetStatus()).ValueOrThrow();
            ASSERT_TRUE(after->ObservedKeys);
            EXPECT_EQ(after->ObservedKeys->size(), 1200u);
            EXPECT_EQ(after->ObservedKeys, before->ObservedKeys);
        }
    }
};

TEST_F(TKeyCountJobTest, TerminalSnapshotSurvivesStopAndRepeatedStatusRequests)
{
    CheckTerminalSnapshotSurvivesStop(TypeName<TTerminalKeySnapshotComputation>(), static_cast<int>(EErrorCode::PartitionKeyLimitExceeded));
}

TEST_F(TKeyCountJobTest, OrdinaryFailureStillReportsLifetimeKeys)
{
    CheckTerminalSnapshotSurvivesStop(TypeName<TOrdinaryFailureKeySnapshotComputation>(), 1);
}

TEST_F(TKeyCountJobTest, SuccessfulFinishStillReportsLifetimeKeys)
{
    CheckTerminalSnapshotSurvivesStop(TypeName<TSuccessfulKeySnapshotComputation>(), 0);
}

TEST_F(TKeyCountJobTest, EveryRunningStatusReportsKeys)
{
    Start(TypeName<TRunningKeySnapshotComputation>());
    WaitFor(Probe_.Started.ToFuture()).ThrowOnError();
    auto first = WaitFor(Job_->GetStatus()).ValueOrThrow();
    ASSERT_TRUE(first->ObservedKeys);
    EXPECT_EQ(first->ObservedKeys->size(), 1200u);
    for (int iteration = 0; iteration < 3; ++iteration) {
        auto status = WaitFor(Job_->GetStatus()).ValueOrThrow();
        EXPECT_FALSE(status->IsFinished);
        EXPECT_EQ(status->ObservedKeys, first->ObservedKeys);
    }
    Probe_.Finish.Set();
    auto finished = WaitForFinished();
    ASSERT_TRUE(finished->IsFinished);
    EXPECT_EQ(finished->ObservedKeys, first->ObservedKeys);
}

TEST_F(TKeyCountJobTest, EqualSizedInputBatchesHavePositiveByteRate)
{
    const TStreamId streamId("input");
    Start(TypeName<TInputBytesComputation>(), /*enable*/ false, /*input*/ true);
    Job_->UpdateMessageTransferingInfo(New<TMessageTransferingInfo>());
    auto schema = New<NTableClient::TTableSchema>();
    auto connectionId = TGuid::Create();
    i64 firstByteSize = 0;
    auto addMessage = [&] (int index) {
        TMessageBuilder builder(streamId, schema);
        builder.SetMessageId(TMessageId(Format("msg-%v", index)));
        builder.SetSystemTimestamp(TSystemTimestamp(100 + index));
        builder.SetAlignmentTimestamp(TSystemTimestamp(100 + index));
        builder.SetEventTimestamp(TSystemTimestamp(100 + index));
        auto message = New<TInputMessage>(builder.Finish(), MakeKey("key"));
        ASSERT_GT(message->ByteSize, 0);
        if (index == 1) {
            firstByteSize = message->ByteSize;
        } else {
            ASSERT_EQ(message->ByteSize, firstByteSize);
        }
        WaitFor(Job_->GetInputBuffer()->AddMessages(connectionId, {std::move(message)}, {})).ThrowOnError();
    };

    addMessage(1);
    WaitFor(Probe_.Started.ToFuture()).ThrowOnError();
    bool firstBatchRegistered = false;
    for (int iteration = 0; iteration < 100; ++iteration) {
        auto status = WaitFor(Job_->GetStatus()).ValueOrThrow();
        ASSERT_TRUE(status->Error.IsOK()) << ToString(status->Error);
        if (status->InputMetrics && status->InputMetrics->Streams.contains(streamId)) {
            firstBatchRegistered = true;
            break;
        }
        WaitForDuration(TDuration::MilliSeconds(10));
    }
    ASSERT_TRUE(firstBatchRegistered);
    WaitForDuration(TDuration::MilliSeconds(20));
    addMessage(2);

    TNodeInputMetricsPtr metrics;
    for (int iteration = 0; iteration < 100; ++iteration) {
        auto status = WaitFor(Job_->GetStatus()).ValueOrThrow();
        ASSERT_TRUE(status->Error.IsOK()) << ToString(status->Error);
        metrics = status->InputMetrics;
        if (metrics && metrics->Global.BytesPerSecond > 0) {
            break;
        }
        WaitForDuration(TDuration::MilliSeconds(10));
    }
    ASSERT_TRUE(metrics);
    EXPECT_GT(metrics->Global.BytesPerSecond, 0);
    EXPECT_GT(GetOrCrash(metrics->Streams, streamId).BytesPerSecond, 0);
}

TEST_F(TKeyCountJobTest, InputBufferRunsIndependentlyAndChargesJobCpu)
{
    InputBufferQueue_ = New<TActionQueue>("MsgServiceTest");
    const auto& serviceQueue = InputBufferQueue_;
    CpuTimeAccountant_ = New<TJobCpuTimeAccountant>();
    InputBufferPoolInvoker_ = CreateTracedInvoker(
        serviceQueue->GetInvoker(),
        NTracing::TTraceContext::NewRoot("InputBufferTest"),
        CpuTimeAccountant_);
    constexpr int messageCount = 4096;
    Dynamic_->DynamicComputationSpec = New<TDynamicComputationSpec>();
    Dynamic_->DynamicComputationSpec->MaxRowsPerBatch = NYTree::TSize(messageCount);
    Start(TypeName<TRunningKeySnapshotComputation>(), /*enable*/ false, /*input*/ true);
    WaitFor(Probe_.Started.ToFuture()).ThrowOnError();
    Job_->UpdateMessageTransferingInfo(New<TMessageTransferingInfo>());
    auto buffer = Job_->GetInputBuffer();
    WaitFor(buffer->GetInflightMetrics()).ThrowOnError();

    auto release = std::make_shared<std::promise<void>>();
    auto gate = release->get_future().share();
    auto releaseGuard = Finally([release] {
        release->set_value();
    });
    auto started = NewPromise<void>();
    Queue_->GetInvoker()->Invoke(BIND([started, gate] {
        started.Set();
        // Hold the job thread to verify that buffer operations do not need it.
        gate.wait();
    }));
    WaitFor(started.ToFuture().WithTimeout(TDuration::Seconds(5))).ThrowOnError();
    auto cpuBefore = CpuTimeAccountant_->GetCpuTime();

    const TStreamId streamId("input");
    auto schema = New<NTableClient::TTableSchema>();
    auto connectionId = TGuid::Create();
    buffer->AddConnectionOffer(connectionId, {{streamId, {{TSystemTimestamp(100), 1'000'000}}}});
    auto limits = WaitFor(buffer->GetConnectionLimits(connectionId).WithTimeout(TDuration::Seconds(5))).ValueOrThrow();
    EXPECT_GT(limits.at(streamId), 0);
    std::vector<TInputMessageConstPtr> messages;
    for (int index = 0; index < messageCount; ++index) {
        TMessageBuilder builder(streamId, schema);
        builder.SetMessageId(TMessageId(Format("msg-%v", index)));
        builder.SetSystemTimestamp(TSystemTimestamp(100));
        builder.SetAlignmentTimestamp(TSystemTimestamp(100));
        builder.SetEventTimestamp(TSystemTimestamp(100));
        messages.push_back(New<TInputMessage>(builder.Finish(), MakeKey("key")));
    }
    auto processed = NewPromise<std::pair<int, bool>>();
    auto onProcessed = BIND([processed, threadId = serviceQueue->GetInvoker()->GetThreadId()] (TJobId, std::vector<TMessageId> ids) {
        processed.Set({std::ssize(ids), GetSystemThreadId() == threadId});
    });
    auto states = WaitFor(buffer->AddMessages(connectionId, std::move(messages), onProcessed)
            .WithTimeout(TDuration::Seconds(5)))
        .ValueOrThrow();
    EXPECT_EQ(std::count(states.begin(), states.end(), EMessageDeliveryState::Accepted), messageCount);
    auto batch = WaitFor(buffer->GetInputBatch({streamId}).WithTimeout(TDuration::Seconds(5))).ValueOrThrow();
    ASSERT_EQ(std::ssize(batch), messageCount);
    std::deque<TMessageId> ids;
    for (const auto& message : batch) {
        ids.push_back(message->MessageId);
    }
    buffer->MarkPersisted(std::move(ids));
    auto [processedCount, onServiceThread] = WaitFor(processed.ToFuture().WithTimeout(TDuration::Seconds(5))).ValueOrThrow();
    EXPECT_EQ(processedCount, messageCount);
    EXPECT_TRUE(onServiceThread);
    auto metrics = WaitFor(buffer->GetInflightMetrics().WithTimeout(TDuration::Seconds(5))).ValueOrThrow();
    EXPECT_EQ(metrics.at(streamId)->Count, 0);
    // Drain the service queue so the traced callbacks have finished CPU accounting.
    auto flush = BIND([] {
    });
    WaitFor(flush.AsyncVia(serviceQueue->GetInvoker()).Run()).ThrowOnError();
    EXPECT_GT(CpuTimeAccountant_->GetCpuTime(), cpuBefore);
}

TEST_F(TKeyCountJobTest, DisabledComputationOmitsKeys)
{
    Start(TypeName<TSuccessfulKeySnapshotComputation>(), /*enable*/ false);
    auto finished = WaitForFinished();
    ASSERT_TRUE(finished->IsFinished);
    EXPECT_FALSE(finished->ObservedKeys);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NWorker
