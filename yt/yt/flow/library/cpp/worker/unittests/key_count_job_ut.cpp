#include <yt/yt/flow/library/cpp/worker/job.h>
#include <yt/yt/flow/library/cpp/worker/job_spec.h>

#include <yt/yt/flow/library/cpp/computation/universal_controller.h>

#include <yt/yt/flow/library/cpp/common/computation.h>
#include <yt/yt/flow/library/cpp/common/computation_statistics.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource_manager.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/test_framework/framework.h>

#include <atomic>

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

class TKeyCountJobTest
    : public ::testing::Test
{
protected:
    const TActionQueuePtr Queue_ = New<TActionQueue>("KeyCountJobTest");
    const TDynamicJobSpecPtr Dynamic_ = New<TDynamicJobSpec>();
    TKeySnapshotProbe Probe_;
    IJobPtr Job_;

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
        GetActiveKeySnapshotProbe().store(nullptr);
    }

    void Start(std::string className, bool enable = true)
    {
        auto jobContext = New<TJobContext>();
        jobContext->ControlSerializedInvoker = Queue_->GetInvoker();
        jobContext->SerializedInvoker = Queue_->GetInvoker();
        jobContext->PoolInvoker = Queue_->GetInvoker();
        auto resourceContext = New<TResourceManagerContext>();
        resourceContext->Invoker = Queue_->GetInvoker();
        resourceContext->StatusProfiler = CreateSyncStatusProfiler();
        jobContext->ResourceManager = CreateResourceManager(resourceContext, {}, {});
        auto spec = New<TJobSpec>();
        spec->ComputationSpec = New<TComputationSpec>();
        spec->ComputationSpec->ComputationClassName = std::move(className);
        Probe_.Enable = enable;
        spec->Partition = New<TPartition>();
        spec->Partition->PartitionId = TPartitionId(TGuid::Create());
        spec->Partition->ComputationId = TComputationId("key-count");
        spec->Partition->State = EPartitionState::Executing;
        spec->Job = New<TJob>();
        spec->Job->JobId = TJobId(TGuid::Create());
        spec->Job->PartitionId = spec->Partition->PartitionId;
        Job_ = CreateJob(jobContext, {}, 1, spec, Dynamic_, New<TToPartitionTraverseData>(), New<TWatermarkState>());
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
