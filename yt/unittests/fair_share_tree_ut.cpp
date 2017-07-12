#include <yt/core/test_framework/framework.h>

#include <yt/server/scheduler/fair_share_tree.h>

#include <yt/server/controller_agent/operation_controller.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/yson/null_consumer.h>

namespace NYT {
namespace NScheduler {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerStrategyHostMock
    : public TRefCounted
    , public ISchedulerStrategyHost
    , public TEventLogHostBase
{
    virtual TJobResources GetTotalResourceLimits() override
    {
        return GetMainNodesResourceLimits();
    }

    virtual TJobResources GetMainNodesResourceLimits() override
    {
        TJobResources resources;
        resources.SetUserSlots(100);
        resources.SetCpu(100);
        resources.SetMemory(1000);
        return resources;
    }

    virtual TJobResources GetResourceLimits(const TSchedulingTagFilter& filter)
    {
        if (!filter.IsEmpty()) {
            return ZeroJobResources();
        }
        return GetMainNodesResourceLimits();
    }

    virtual void ActivateOperation(const TOperationId& operationId) override
    { }

    virtual int GetExecNodeCount() const override
    {
        return 10;
    }

    virtual void ValidatePoolPermission(
        const NYPath::TYPath& path,
        const TString& user,
        NYTree::EPermission permission) const override
    { }

    virtual void SetSchedulerAlert(ESchedulerAlertType alertType, const TError& alert) override
    { }

    virtual void SetOperationAlert(
        const TOperationId& operationId,
        EOperationAlertType alertType,
        const TError& alert) override
    { }

    virtual NYson::IYsonConsumer* GetEventLogConsumer() override
    {
        return NYson::GetNullYsonConsumer();
    }

};

DEFINE_REFCOUNTED_TYPE(TSchedulerStrategyHostMock)


class TOperationControllerStrategyHostMock
    : public IOperationControllerStrategyHost
{
public:
    virtual TScheduleJobResultPtr ScheduleJob(
        ISchedulingContextPtr context,
        const TJobResources& jobLimits) override
    {
        Y_UNREACHABLE();
    }

    virtual IInvokerPtr GetCancelableInvoker() const
    {
        Y_UNREACHABLE();
    }

    virtual void OnJobAborted(std::unique_ptr<TAbortedJobSummary> jobSummary)
    {
        Y_UNREACHABLE();
    }

    virtual TJobResources GetNeededResources() const
    {
        TJobResources resources;
        resources.SetUserSlots(10);
        resources.SetCpu(10);
        resources.SetMemory(100);
        return resources;
    }

    virtual std::vector<TJobResources> GetMinNeededJobResources() const
    {
        TJobResources resources;
        resources.SetUserSlots(1);
        resources.SetCpu(1);
        resources.SetMemory(10);
        return std::vector<TJobResources>({resources});
    }

    virtual int GetPendingJobCount() const
    {
        return 10;
    }
};

DEFINE_REFCOUNTED_TYPE(TOperationControllerStrategyHostMock)


class TOperationStrategyHostMock
    : public TRefCounted
    , public IOperationStrategyHost
{
public:
    TOperationStrategyHostMock()
        : StartTime_(TInstant::Now())
        , Id_(TGuid::Create())
        , Controller_(New<TOperationControllerStrategyHostMock>())
    { }

    virtual bool IsSchedulable() const override
    {
        return true;
    }

    virtual TInstant GetStartTime() const override
    {
        return StartTime_;
    }

    virtual int GetSlotIndex() const
    {
        return 0;
    }

    virtual TOperationId GetId() const
    {
        return Id_;
    }

    virtual IOperationControllerStrategyHostPtr GetControllerStrategyHost() const override
    {
        return Controller_;
    }

private:
    TInstant StartTime_;
    TOperationId Id_;
    IOperationControllerStrategyHostPtr Controller_;
};

DEFINE_REFCOUNTED_TYPE(TOperationStrategyHostMock)


TEST(FairShareTree, TestAttributes)
{
    auto config = New<TFairShareStrategyConfig>();
    auto host = New<TSchedulerStrategyHostMock>();

    auto rootElement = New<TRootElement>(
        host.Get(),
        config,
        // TODO(ignat): eliminate profiling from test.
        NProfiling::TProfileManager::Get()->RegisterTag("pool", RootPoolName));

    auto poolA = New<TPool>(
        host.Get(),
        "A",
        config,
        NProfiling::TProfileManager::Get()->RegisterTag("pool", "A"));

    auto poolB = New<TPool>(
        host.Get(),
        "B",
        config,
        NProfiling::TProfileManager::Get()->RegisterTag("pool", "B"));

    rootElement->AddChild(poolA);
    poolA->SetParent(rootElement.Get());

    rootElement->AddChild(poolB);
    poolB->SetParent(rootElement.Get());

    auto operationX = New<TOperationStrategyHostMock>();
    auto operationElementX = New<TOperationElement>(
        config,
        New<TStrategyOperationSpec>(),
        New<TOperationRuntimeParams>(),
        host.Get(),
        operationX.Get());

    poolA->AddChild(operationElementX);
    operationElementX->SetParent(poolA.Get());

    auto dynamicAttributes = TDynamicAttributesList(4);
    rootElement->Update(dynamicAttributes);

    EXPECT_EQ(rootElement->Attributes().DemandRatio, 0.1);
    EXPECT_EQ(poolA->Attributes().DemandRatio, 0.1);
    EXPECT_EQ(poolB->Attributes().DemandRatio, 0.0);
    EXPECT_EQ(operationElementX->Attributes().DemandRatio, 0.1);

    EXPECT_EQ(rootElement->Attributes().FairShareRatio, 1.0);
    EXPECT_EQ(rootElement->Attributes().DemandRatio, 0.1);
    EXPECT_EQ(poolB->Attributes().FairShareRatio, 0.0);
    EXPECT_EQ(operationElementX->Attributes().FairShareRatio, 0.1);
}

TEST(FairShareTree, TestUpdatePreemptableJobsList)
{
    auto config = New<TFairShareStrategyConfig>();
    auto host = New<TSchedulerStrategyHostMock>();

    auto rootElement = New<TRootElement>(
        host.Get(),
        config,
        // TODO(ignat): eliminate profiling from test.
        NProfiling::TProfileManager::Get()->RegisterTag("pool", RootPoolName));

    auto operationX = New<TOperationStrategyHostMock>();
    auto operationElementX = New<TOperationElement>(
        config,
        New<TStrategyOperationSpec>(),
        New<TOperationRuntimeParams>(),
        host.Get(),
        operationX.Get());

    rootElement->AddChild(operationElementX);
    operationElementX->SetParent(rootElement.Get());

    TJobResources jobResources;
    jobResources.SetUserSlots(1);
    jobResources.SetCpu(1);
    jobResources.SetMemory(10);

    std::vector<TJobId> jobIds;
    for (int i = 0; i < 150; ++i) {
        auto jobId = TGuid::Create();
        jobIds.push_back(jobId);
        operationElementX->OnJobStarted(jobId, jobResources);
    }

    auto dynamicAttributes = TDynamicAttributesList(2);
    rootElement->Update(dynamicAttributes);

    EXPECT_EQ(operationElementX->Attributes().DemandRatio, 1.6);
    EXPECT_EQ(operationElementX->Attributes().FairShareRatio, 1.0);

    for (int i = 0; i < 50; ++i) {
        EXPECT_FALSE(operationElementX->IsJobPreemptable(jobIds[i], true));
    }
    for (int i = 50; i < 100; ++i) {
        EXPECT_FALSE(operationElementX->IsJobPreemptable(jobIds[i], false));
        EXPECT_TRUE(operationElementX->IsJobPreemptable(jobIds[i], true));
    }
    for (int i = 100; i < 150; ++i) {
        EXPECT_TRUE(operationElementX->IsJobPreemptable(jobIds[i], false));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NScheduler
} // namespace NYT
