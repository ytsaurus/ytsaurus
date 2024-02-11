#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/scheduler/fair_share_strategy_operation_controller.h>
#include <yt/yt/server/scheduler/operation.h>
#include <yt/yt/server/scheduler/operation_controller.h>

#include <yt/yt/server/lib/scheduler/config.h>

#include <yt/yt/ytlib/chunk_client/medium_directory.h>

#include <yt/yt/ytlib/chunk_client/proto/medium_directory.pb.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

class TOperationControllerStrategyHostMock
    : public IOperationControllerStrategyHost
{
public:
    explicit TOperationControllerStrategyHostMock(TJobResourcesWithQuotaList jobResourcesList)
        : JobResourcesList_(std::move(jobResourcesList))
    { }

    TControllerEpoch GetEpoch() const override
    {
        return TControllerEpoch(0);
    }

    MOCK_METHOD(TFuture<TControllerScheduleAllocationResultPtr>, ScheduleAllocation, (
        const ISchedulingContextPtr& context,
        const TJobResources& jobLimits,
        const TDiskResources& diskResourceLimits,
        const TString& treeId,
        const TString& poolPath,
        const TFairShareStrategyTreeConfigPtr& treeConfig), (override));

    MOCK_METHOD(void, OnNonscheduledAllocationAborted, (TAllocationId, EAbortReason, TControllerEpoch), (override));

    TCompositeNeededResources GetNeededResources() const override
    {
        TJobResources totalResources;
        for (const auto& resources : JobResourcesList_) {
            totalResources += resources.ToJobResources();
        }
        return TCompositeNeededResources{.DefaultResources = totalResources};
    }

    void UpdateMinNeededAllocationResources() override
    { }

    TJobResourcesWithQuotaList GetMinNeededAllocationResources() const override
    {
        TJobResourcesWithQuotaList minNeededResourcesList;
        for (const auto& resources : JobResourcesList_) {
            bool dominated = false;
            for (const auto& minNeededResourcesElement : minNeededResourcesList) {
                if (Dominates(resources.ToJobResources(), minNeededResourcesElement.ToJobResources())) {
                    dominated = true;
                    break;
                }
            }
            if (!dominated) {
                minNeededResourcesList.push_back(resources);
            }
        }
        return minNeededResourcesList;
    }

    TJobResourcesWithQuotaList GetInitialMinNeededAllocationResources() const override
    {
        return GetMinNeededAllocationResources();
    }

    EPreemptionMode PreemptionMode = EPreemptionMode::Normal;

    EPreemptionMode GetPreemptionMode() const override
    {
        return PreemptionMode;
    }

private:
    TJobResourcesWithQuotaList JobResourcesList_;
};

using TOperationControllerStrategyHostMockPtr = TIntrusivePtr<TOperationControllerStrategyHostMock>;

////////////////////////////////////////////////////////////////////////////////

class TOperationStrategyHostMock
    : public TRefCounted
    , public IOperationStrategyHost
{
public:
    explicit TOperationStrategyHostMock(const TJobResourcesWithQuotaList& jobResourcesList)
        : StartTime_(TInstant::Now())
        , Id_(TGuid::Create())
        , Controller_(New<TOperationControllerStrategyHostMock>(jobResourcesList))
    { }

    EOperationType GetType() const override
    {
        return EOperationType::Vanilla;
    }

    EOperationState GetState() const override
    {
        YT_UNIMPLEMENTED();
    }

    std::optional<EUnschedulableReason> CheckUnschedulable(const std::optional<TString>& /*treeId*/) const override
    {
        return std::nullopt;
    }

    TInstant GetStartTime() const override
    {
        return StartTime_;
    }

    std::optional<int> FindSlotIndex(const TString& /*treeId*/) const override
    {
        return 0;
    }

    void SetSlotIndex(const TString& /*treeId*/, int /*slotIndex*/) override
    { }

    void ReleaseSlotIndex(const TString& /*treeId*/) override
    { }

    TString GetAuthenticatedUser() const override
    {
        return "root";
    }

    TOperationId GetId() const override
    {
        return Id_;
    }

    IOperationControllerStrategyHostPtr GetControllerStrategyHost() const override
    {
        return Controller_;
    }

    TStrategyOperationSpecPtr GetStrategySpec() const override
    {
        YT_UNIMPLEMENTED();
    }

    TStrategyOperationSpecPtr GetStrategySpecForTree(const TString& /*treeId*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    const NYson::TYsonString& GetSpecString() const override
    {
        YT_UNIMPLEMENTED();
    }

    const NYson::TYsonString& GetTrimmedAnnotations() const override
    {
        return TrimmedAnnotations_;
    }

    TOperationRuntimeParametersPtr GetRuntimeParameters() const override
    {
        YT_UNIMPLEMENTED();
    }

    TOperationControllerStrategyHostMock& GetOperationControllerStrategyHost()
    {
        return *Controller_.Get();
    }

    bool IsTreeErased(const TString& /*treeId*/) const override
    {
        return false;
    }

    void EraseTrees(const std::vector<TString>& /*treeIds*/) override
    { }

    std::optional<TJobResources> GetAggregatedInitialMinNeededResources() const override
    {
        return std::nullopt;
    }

private:
    TInstant StartTime_;
    NYson::TYsonString TrimmedAnnotations_;
    TOperationId Id_;
    TOperationControllerStrategyHostMockPtr Controller_;
};

using TOperationStrategyHostMockPtr = TIntrusivePtr<TOperationStrategyHostMock>;

////////////////////////////////////////////////////////////////////////////////

class TFairShareStrategyOperationControllerTest
    : public testing::Test
{
public:
    TFairShareStrategyOperationControllerTest()
    {
        NChunkClient::NProto::TMediumDirectory protoDirectory;
        auto* item = protoDirectory.add_items();
        item->set_name(NChunkClient::DefaultSlotsMediumName);
        item->set_index(NChunkClient::DefaultSlotsMediumIndex);
        item->set_priority(0);
        MediumDirectory_->LoadFrom(protoDirectory);
    }

    void SetUp() override
    {
        SchedulerConfig_ = New<TSchedulerConfig>();
    }

protected:
    TSchedulerConfigPtr SchedulerConfig_;

    NChunkClient::TMediumDirectoryPtr MediumDirectory_ = New<NChunkClient::TMediumDirectory>();

    NNodeTrackerClient::TNodeId ExecNodeId_ = NNodeTrackerClient::TNodeId(1);

    TFairShareStrategyOperationControllerPtr CreateTestOperationController(
        IOperationStrategyHost* operation,
        int nodeShardCount = 1)
    {
        auto controller = New<TFairShareStrategyOperationController>(
            operation,
            SchedulerConfig_,
            nodeShardCount);

        // Just in case.
        controller->SetDetailedLogsEnabled(true);

        return controller;
    }

    TExecNodePtr CreateTestExecNode(const TJobResourcesWithQuota& nodeResources, TBooleanFormulaTags tags = {})
    {
        auto diskResources = TDiskResources{
            .DiskLocationResources = {
                TDiskResources::TDiskLocationResources{
                    .Usage = 0,
                    .Limit = GetOrDefault(
                        nodeResources.DiskQuota().DiskSpacePerMedium,
                        NChunkClient::DefaultSlotsMediumIndex),
                },
            },
        };

        auto nodeId = ExecNodeId_;
        ExecNodeId_ = NNodeTrackerClient::TNodeId(nodeId.Underlying() + 1);
        auto execNode = New<TExecNode>(nodeId, NNodeTrackerClient::TNodeDescriptor(), ENodeState::Online);
        execNode->SetResourceLimits(nodeResources.ToJobResources());
        execNode->SetDiskResources(std::move(diskResources));

        execNode->SetTags(std::move(tags));

        return execNode;
    }

    ISchedulingContextPtr CreateTestSchedulingContext(TExecNodePtr execNode, int nodeShardId = 0)
    {
        return CreateSchedulingContext(
            nodeShardId,
            SchedulerConfig_,
            std::move(execNode),
            /*runningJobs*/ {},
            MediumDirectory_);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TFairShareStrategyOperationControllerTest, TestConcurrentScheduleAllocationCallsThrottling)
{
    const int JobCount = 10;
    SchedulerConfig_->MaxConcurrentControllerScheduleAllocationCalls = JobCount;
    SchedulerConfig_->ConcurrentControllerScheduleAllocationCallsRegularization = 1.0;

    auto operation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList());
    auto controller = CreateTestOperationController(operation.Get());

    auto readyToGo = NewPromise<void>();
    std::atomic<int> concurrentScheduleAllocationCalls = 0;
    EXPECT_CALL(
        operation->GetOperationControllerStrategyHost(),
        ScheduleAllocation(testing::_, testing::_, testing::_, testing::_, testing::_, testing::_))
        .Times(JobCount)
        .WillRepeatedly(testing::Invoke([&] (auto /*context*/, auto /*jobLimits*/, auto /*diskResourceLimits*/, auto /*treeId*/, auto /*poolPath*/, auto /*treeConfig*/) {
            ++concurrentScheduleAllocationCalls;
            EXPECT_TRUE(NConcurrency::WaitFor(readyToGo.ToFuture()).IsOK());
            return MakeFuture<TControllerScheduleAllocationResultPtr>(
                TErrorOr<TControllerScheduleAllocationResultPtr>(New<TControllerScheduleAllocationResult>()));
        }));

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(10);
    nodeResources.SetCpu(10);
    nodeResources.SetMemory(100_MB);
    auto execNode = CreateTestExecNode(nodeResources);

    auto actionQueue = New<NConcurrency::TActionQueue>();
    std::vector<TFuture<TControllerScheduleAllocationResultPtr>> futures;
    std::vector<ISchedulingContextPtr> contexts;
    int concurrentCallsThrottlingCount = 0;
    int concurrentExecDurationThrottlingCount = 0;
    for (int i = 0; i < 2 * JobCount; ++i) {
        auto context = CreateTestSchedulingContext(execNode);

        if (controller->IsMaxConcurrentScheduleAllocationCallsPerNodeShardViolated(context)) {
            ++concurrentCallsThrottlingCount;
            continue;
        }
        if (controller->IsMaxConcurrentScheduleAllocationExecDurationPerNodeShardViolated(context)) {
            ++concurrentExecDurationThrottlingCount;
            continue;
        }

        contexts.push_back(context);

        controller->OnScheduleAllocationStarted(context);

        auto future = BIND([&, context] {
            return controller->ScheduleAllocation(
                context,
                nodeResources,
                context->DiskResources(),
                /*timeLimit*/ TDuration::Days(1),
                /*treeId*/ "tree",
                /*poolPath*/ "/pool",
                /*treeConfig*/ {});
        })
            .AsyncVia(actionQueue->GetInvoker())
            .Run();
        futures.push_back(future);
    }

    EXPECT_EQ(JobCount, std::ssize(futures));
    EXPECT_EQ(JobCount, std::ssize(contexts));
    EXPECT_EQ(JobCount, concurrentCallsThrottlingCount);
    EXPECT_EQ(0, concurrentExecDurationThrottlingCount);

    while (concurrentScheduleAllocationCalls != JobCount) {
        // Actively waiting.
    }

    readyToGo.Set();
    EXPECT_TRUE(AllSucceeded(futures).WithTimeout(TDuration::Seconds(2)).Get().IsOK());

    for (const auto& context : contexts) {
        controller->OnScheduleAllocationFinished(context);
    }
}

TEST_F(TFairShareStrategyOperationControllerTest, TestConcurrentScheduleAllocationExecDurationThrottling)
{
    const int JobCount = 10;
    SchedulerConfig_->MaxConcurrentControllerScheduleAllocationExecDuration = TDuration::Seconds(1);
    SchedulerConfig_->EnableConcurrentScheduleAllocationExecDurationThrottling = true;
    SchedulerConfig_->ConcurrentControllerScheduleAllocationCallsRegularization = 1.0;

    auto operation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList());
    auto controller = CreateTestOperationController(operation.Get());

    auto readyToGo = NewPromise<void>();
    std::atomic<int> concurrentScheduleAllocationCalls = 0;
    EXPECT_CALL(
        operation->GetOperationControllerStrategyHost(),
        ScheduleAllocation(testing::_, testing::_, testing::_, testing::_, testing::_, testing::_))
        .Times(JobCount + 1)
        .WillOnce(testing::Invoke([&] (auto /*context*/, auto /*jobLimits*/, auto /*diskResourceLimits*/, auto /*treeId*/, auto /*poolPath*/, auto /*treeConfig*/) {
            auto result = New<TControllerScheduleAllocationResult>();
            result->NextDurationEstimate = TDuration::MilliSeconds(100);
            return MakeFuture<TControllerScheduleAllocationResultPtr>(
                TErrorOr<TControllerScheduleAllocationResultPtr>(result));
        }))
        .WillRepeatedly(testing::Invoke([&] (auto /*context*/, auto /*jobLimits*/, auto /*diskResourceLimits*/, auto /*treeId*/, auto /*poolPath*/, auto /*treeConfig*/) {
            ++concurrentScheduleAllocationCalls;
            EXPECT_TRUE(NConcurrency::WaitFor(readyToGo.ToFuture()).IsOK());
            auto result = New<TControllerScheduleAllocationResult>();
            result->NextDurationEstimate = TDuration::MilliSeconds(100);
            return MakeFuture<TControllerScheduleAllocationResultPtr>(
                TErrorOr<TControllerScheduleAllocationResultPtr>(result));
        }));

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(10);
    nodeResources.SetCpu(10);
    nodeResources.SetMemory(100_MB);
    auto execNode = CreateTestExecNode(nodeResources);

    // Execute one schedule job to get an estimate.
    {
        auto context = CreateTestSchedulingContext(execNode);

        EXPECT_FALSE(controller->IsMaxConcurrentScheduleAllocationCallsPerNodeShardViolated(context));
        EXPECT_FALSE(controller->IsMaxConcurrentScheduleAllocationExecDurationPerNodeShardViolated(context));

        controller->OnScheduleAllocationStarted(context);

        controller->ScheduleAllocation(
            context,
            nodeResources,
            context->DiskResources(),
            /*timeLimit*/ TDuration::Days(1),
            /*treeId*/ "tree",
            /*poolPath*/ "/pool",
            /*treeConfig*/ {});

        controller->OnScheduleAllocationFinished(context);
    }

    auto actionQueue = New<NConcurrency::TActionQueue>();
    std::vector<TFuture<TControllerScheduleAllocationResultPtr>> futures;
    std::vector<ISchedulingContextPtr> contexts;
    int concurrentCallsThrottlingCount = 0;
    int concurrentExecDurationThrottlingCount = 0;
    for (int i = 0; i < 2 * JobCount; ++i) {
        auto context = CreateTestSchedulingContext(execNode);

        if (controller->IsMaxConcurrentScheduleAllocationCallsPerNodeShardViolated(context)) {
            ++concurrentCallsThrottlingCount;
            continue;
        }
        if (controller->IsMaxConcurrentScheduleAllocationExecDurationPerNodeShardViolated(context)) {
            ++concurrentExecDurationThrottlingCount;
            continue;
        }

        contexts.push_back(context);

        controller->OnScheduleAllocationStarted(context);

        auto future = BIND([&, context] {
            return controller->ScheduleAllocation(
                context,
                nodeResources,
                context->DiskResources(),
                /*timeLimit*/ TDuration::Days(1),
                /*treeId*/ "tree",
                /*poolPath*/ "/pool",
                /*treeConfig*/ {});
        })
            .AsyncVia(actionQueue->GetInvoker())
            .Run();
        futures.push_back(future);
    }

    EXPECT_EQ(JobCount, std::ssize(futures));
    EXPECT_EQ(JobCount, std::ssize(contexts));
    EXPECT_EQ(0, concurrentCallsThrottlingCount);
    EXPECT_EQ(JobCount, concurrentExecDurationThrottlingCount);

    while (concurrentScheduleAllocationCalls != JobCount) {
        // Actively waiting.
    }

    readyToGo.Set();
    EXPECT_TRUE(AllSucceeded(futures).WithTimeout(TDuration::Seconds(2)).Get().IsOK());

    for (const auto& context : contexts) {
        controller->OnScheduleAllocationFinished(context);
    }
}

TEST_F(TFairShareStrategyOperationControllerTest, TestConcurrentControllerScheduleAllocationCallsRegularization)
{
    const int JobCount = 10;
    SchedulerConfig_->MaxConcurrentControllerScheduleAllocationCalls = JobCount;
    SchedulerConfig_->ConcurrentControllerScheduleAllocationCallsRegularization = 2.0;

    const int NodeShardCount = 2;
    auto operation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList());
    auto controller = CreateTestOperationController(operation.Get(), NodeShardCount);

    auto readyToGo = NewPromise<void>();
    std::atomic<int> concurrentScheduleAllocationCalls = 0;
    EXPECT_CALL(
        operation->GetOperationControllerStrategyHost(),
        ScheduleAllocation(testing::_, testing::_, testing::_, testing::_, testing::_, testing::_))
        .Times(2 * JobCount)
        .WillRepeatedly(testing::Invoke([&] (auto /*context*/, auto /*jobLimits*/, auto /*diskResourceLimits*/, auto /*treeId*/, auto /*poolPath*/, auto /*treeConfig*/) {
            ++concurrentScheduleAllocationCalls;
            EXPECT_TRUE(NConcurrency::WaitFor(readyToGo.ToFuture()).IsOK());
            return MakeFuture<TControllerScheduleAllocationResultPtr>(
                TErrorOr<TControllerScheduleAllocationResultPtr>(New<TControllerScheduleAllocationResult>()));
        }));

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(10);
    nodeResources.SetCpu(10);
    nodeResources.SetMemory(100_MB);
    auto execNode = CreateTestExecNode(nodeResources);

    auto actionQueue = New<NConcurrency::TActionQueue>();
    std::vector<TFuture<TControllerScheduleAllocationResultPtr>> futures;
    std::vector<ISchedulingContextPtr> contexts;
    int concurrentCallsThrottlingCount = 0;
    int concurrentExecDurationThrottlingCount = 0;
    for (int i = 0; i < 2 * JobCount; ++i) {
        int nodeShardId = i % NodeShardCount;
        auto context = CreateTestSchedulingContext(execNode, nodeShardId);

        if (controller->IsMaxConcurrentScheduleAllocationCallsPerNodeShardViolated(context)) {
            ++concurrentCallsThrottlingCount;
            continue;
        }
        if (controller->IsMaxConcurrentScheduleAllocationExecDurationPerNodeShardViolated(context)) {
            ++concurrentExecDurationThrottlingCount;
            continue;
        }

        contexts.push_back(context);

        controller->OnScheduleAllocationStarted(context);

        auto future = BIND([&, context] {
            return controller->ScheduleAllocation(
                context,
                nodeResources,
                context->DiskResources(),
                /*timeLimit*/ TDuration::Days(1),
                /*treeId*/ "tree",
                /*poolPath*/ "/pool",
                /*treeConfig*/ {});
        })
            .AsyncVia(actionQueue->GetInvoker())
            .Run();
        futures.push_back(future);
    }

    EXPECT_EQ(2 * JobCount, std::ssize(futures));
    EXPECT_EQ(2 * JobCount, std::ssize(contexts));
    EXPECT_EQ(0, concurrentCallsThrottlingCount);
    EXPECT_EQ(0, concurrentExecDurationThrottlingCount);

    while (concurrentScheduleAllocationCalls != 2 * JobCount) {
        // Actively waiting.
    }

    readyToGo.Set();
    EXPECT_TRUE(AllSucceeded(futures).WithTimeout(TDuration::Seconds(2)).Get().IsOK());

    for (const auto& context : contexts) {
        controller->OnScheduleAllocationFinished(context);
    }
}

TEST_F(TFairShareStrategyOperationControllerTest, TestScheduleAllocationTimeout)
{
    auto operation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList());
    auto controller = CreateTestOperationController(operation.Get());

    auto actionQueue = New<NConcurrency::TActionQueue>();

    auto firstAllocationId = TAllocationId(TGuid::Create());
    auto secondAllocationId = TAllocationId(TGuid::Create());
    EXPECT_CALL(
        operation->GetOperationControllerStrategyHost(),
        ScheduleAllocation(testing::_, testing::_, testing::_, testing::_, testing::_, testing::_))
        .WillOnce(testing::Invoke([&] (auto /*context*/, auto /*jobLimits*/, auto /*diskResourceLimits*/, auto /*treeId*/, auto /*poolPath*/, auto /*treeConfig*/) {
            return BIND([&] {
                Sleep(TDuration::Seconds(2));

                return New<TControllerScheduleAllocationResult>();
            })
                .AsyncVia(actionQueue->GetInvoker())
                .Run();
        }))
        .WillOnce(testing::Invoke([&] (auto /*context*/, auto /*jobLimits*/, auto /*diskResourceLimits*/, auto /*treeId*/, auto /*poolPath*/, auto /*treeConfig*/) {
            return BIND([&] {
                Sleep(TDuration::Seconds(2));

                auto result = New<TControllerScheduleAllocationResult>();
                result->StartDescriptor.emplace(firstAllocationId, /*resourceLimits*/ TJobResources());
                return result;
            })
                .AsyncVia(actionQueue->GetInvoker())
                .Run();
        }))
        .WillOnce(testing::Invoke([&] (auto /*context*/, auto /*jobLimits*/, auto /*diskResourceLimits*/, auto /*treeId*/, auto /*poolPath*/, auto /*treeConfig*/) {
            return BIND([&] {
                Sleep(TDuration::MilliSeconds(10));

                auto result = New<TControllerScheduleAllocationResult>();
                result->StartDescriptor.emplace(secondAllocationId, /*resourceLimits*/ TJobResources());
                return result;
            })
                .AsyncVia(actionQueue->GetInvoker())
                .Run();
        }));
    EXPECT_CALL(
        operation->GetOperationControllerStrategyHost(),
        OnNonscheduledAllocationAborted(firstAllocationId, EAbortReason::SchedulingTimeout, testing::_))
        .Times(1);

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(10);
    nodeResources.SetCpu(10);
    nodeResources.SetMemory(100_MB);
    auto execNode = CreateTestExecNode(nodeResources);

    for (int i = 0; i < 2; ++i) {
        auto context = CreateTestSchedulingContext(execNode);
        auto result = controller->ScheduleAllocation(
            context,
            nodeResources,
            context->DiskResources(),
            /*timeLimit*/ TDuration::Seconds(1),
            /*treeId*/ "tree",
            /*poolPath*/ "/pool",
            /*treeConfig*/ {});

        EXPECT_FALSE(result->StartDescriptor);
        EXPECT_EQ(1, result->Failed[EScheduleAllocationFailReason::Timeout]);
    }

    {
        auto context = CreateTestSchedulingContext(execNode);
        auto result = controller->ScheduleAllocation(
            context,
            nodeResources,
            context->DiskResources(),
            /*timeLimit*/ TDuration::Seconds(10),
            /*treeId*/ "tree",
            /*poolPath*/ "/pool",
            /*treeConfig*/ {});

        ASSERT_TRUE(result->StartDescriptor);
        EXPECT_EQ(secondAllocationId, result->StartDescriptor->Id);
    }

    auto finished = NewPromise<void>();
    actionQueue->GetInvoker()->Invoke(BIND([finished] {
        finished.Set();
    }));
    EXPECT_TRUE(finished.ToFuture().Get().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NScheduler

