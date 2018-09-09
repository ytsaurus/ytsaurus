#include <yt/core/test_framework/framework.h>

#include <yt/server/scheduler/fair_share_tree_element.h>
#include <yt/server/scheduler/operation_controller.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/yson/null_consumer.h>

#include <contrib/libs/gmock/gmock/gmock.h>
#include <contrib/libs/gmock/gmock/gmock-matchers.h>
#include <contrib/libs/gmock/gmock/gmock-actions.h>

namespace NYT {
namespace NScheduler {
namespace {

using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerStrategyHostMock
    : public TRefCounted
    , public ISchedulerStrategyHost
    , public TEventLogHostBase
{
    explicit TSchedulerStrategyHostMock(const TJobResourcesWithQuotaList& nodeResourceLimitsList)
        : NodeResourceLimitsList(nodeResourceLimitsList)
    { }

    TSchedulerStrategyHostMock()
        : TSchedulerStrategyHostMock(TJobResourcesWithQuotaList{})
    { }

    virtual TJobResources GetResourceLimits(const TSchedulingTagFilter& filter)
    {
        if (!filter.IsEmpty()) {
            return ZeroJobResources();
        }

        TJobResources totalResources;
        for (const auto& resources : NodeResourceLimitsList) {
            totalResources += resources.ToJobResources();
        }
        return totalResources;
    }

    virtual TInstant GetConnectionTime() const override
    {
        return TInstant();
    }

    virtual void ActivateOperation(const TOperationId& operationId) override
    { }

    virtual void AbortOperation(const TOperationId& /* operationId */, const TError& /* error */) override
    { }

    virtual TMemoryDistribution GetExecNodeMemoryDistribution(const TSchedulingTagFilter& filter) const override
    {
        TMemoryDistribution result;
        for (const auto& resources : NodeResourceLimitsList) {
            ++result[resources.GetMemory()];
        }
        return result;
    }

    virtual TRefCountedExecNodeDescriptorMapPtr CalculateExecNodeDescriptors(
        const TSchedulingTagFilter& /* filter */) const override
    {
        Y_UNREACHABLE();
    }

    virtual std::vector<NNodeTrackerClient::TNodeId> GetExecNodeIds(
        const TSchedulingTagFilter& /* filter */) const override
    {
        return {};
    }

    virtual void ValidatePoolPermission(
        const NYPath::TYPath& path,
        const TString& user,
        NYTree::EPermission permission) const override
    { }

    virtual void SetSchedulerAlert(ESchedulerAlertType alertType, const TError& alert) override
    { }

    virtual TFuture<void> SetOperationAlert(
        const TOperationId& operationId,
        EOperationAlertType alertType,
        const TError& alert,
        TNullable<TDuration> timeout) override
    {
        return VoidFuture;
    }

    virtual NYson::IYsonConsumer* GetEventLogConsumer() override
    {
        return NYson::GetNullYsonConsumer();
    }

    TJobResourcesWithQuotaList NodeResourceLimitsList;
};

DEFINE_REFCOUNTED_TYPE(TSchedulerStrategyHostMock)


class TOperationControllerStrategyHostMock
    : public IOperationControllerStrategyHost
{
public:
    explicit TOperationControllerStrategyHostMock(const TJobResourcesWithQuotaList& jobResourcesList)
        : JobResourcesList(jobResourcesList)
    { }

    MOCK_METHOD3(ScheduleJob, TFuture<TScheduleJobResultPtr>(
        const ISchedulingContextPtr& context,
        const TJobResourcesWithQuota& jobLimits,
        const TString& treeId));

    MOCK_METHOD2(OnNonscheduledJobAborted, void(const TJobId&, EAbortReason));

    virtual TJobResources GetNeededResources() const override
    {
        TJobResources totalResources;
        for (const auto& resources : JobResourcesList) {
            totalResources += resources.ToJobResources();
        }
        return totalResources;
    }

    virtual void UpdateMinNeededJobResources() override
    { }

    virtual TJobResourcesWithQuotaList GetMinNeededJobResources() const override
    {
        TJobResourcesWithQuotaList minNeededResourcesList;
        for (const auto& resources : JobResourcesList) {
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

    virtual int GetPendingJobCount() const override
    {
        return JobResourcesList.size();
    }

private:
    TJobResourcesWithQuotaList JobResourcesList;
};

DEFINE_REFCOUNTED_TYPE(TOperationControllerStrategyHostMock)
DECLARE_REFCOUNTED_TYPE(TOperationControllerStrategyHostMock)

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

    virtual EOperationType GetType() const override
    {
        Y_UNREACHABLE();
    }

    virtual bool IsSchedulable() const override
    {
        return true;
    }

    virtual TInstant GetStartTime() const override
    {
        return StartTime_;
    }

    virtual TNullable<int> FindSlotIndex(const TString& /* treeId */) const override
    {
        return 0;
    }

    virtual int GetSlotIndex(const TString& treeId) const override
    {
        return 0;
    }

    virtual void SetSlotIndex(const TString& /* treeId */, int /* slotIndex */) override
    { }

    virtual TString GetAuthenticatedUser() const
    {
        return "root";
    }

    virtual const TOperationId& GetId() const
    {
        return Id_;
    }

    virtual IOperationControllerStrategyHostPtr GetControllerStrategyHost() const override
    {
        return Controller_;
    }

    virtual NYTree::IMapNodePtr GetSpec() const override
    {
        Y_UNREACHABLE();
    }

    virtual TOperationRuntimeParametersPtr GetRuntimeParameters() const override
    {
        Y_UNREACHABLE();
    }

    TOperationControllerStrategyHostMock& GetOperationControllerStrategyHost()
    {
        return *Controller_.Get();
    }

private:
    TInstant StartTime_;
    TOperationId Id_;
    TOperationControllerStrategyHostMockPtr Controller_;
};

DEFINE_REFCOUNTED_TYPE(TOperationStrategyHostMock)

class TFairShareTreeTest
    : public testing::Test
{
protected:
    TSchedulerConfigPtr SchedulerConfig_ = New<TSchedulerConfig>();
    TFairShareStrategyTreeConfigPtr TreeConfig_ = New<TFairShareStrategyTreeConfig>();

    TRootElementPtr CreateTestRootElement(ISchedulerStrategyHost* host)
    {
        return New<TRootElement>(
            host,
            TreeConfig_,
            // TODO(ignat): eliminate profiling from test.
            NProfiling::TProfileManager::Get()->RegisterTag("pool", RootPoolName),
            "default");
    }

    TPoolPtr CreateTestPool(ISchedulerStrategyHost* host, const TString& name)
    {
        return New<TPool>(
            host,
            name,
            New<TPoolConfig>(),
            /*defaultConfigured*/ true,
            TreeConfig_,
            // TODO(ignat): eliminate profiling from test.
            NProfiling::TProfileManager::Get()->RegisterTag("pool", name),
            "default");
    }

    TOperationElementPtr CreateTestOperationElement(
        ISchedulerStrategyHost* host,
        const TOperationFairShareTreeRuntimeParametersPtr& operationOptions,
        IOperationStrategyHost* operation)
    {
        auto operationController = New<TFairShareStrategyOperationController>(operation);
        return New<TOperationElement>(
            TreeConfig_,
            New<TStrategyOperationSpec>(),
            operationOptions,
            operationController,
            SchedulerConfig_,
            host,
            operation,
            "default");
    }

    TExecNodePtr CreateTestExecNode(NNodeTrackerClient::TNodeId id, const TJobResourcesWithQuota& nodeResources)
    {
        NNodeTrackerClient::NProto::TDiskResources diskResources;
        diskResources.mutable_disk_reports()->Add();
        diskResources.mutable_disk_reports(0)->set_limit(nodeResources.GetDiskQuota());

        auto execNode = New<TExecNode>(id, NNodeTrackerClient::TNodeDescriptor());
        execNode->SetResourceLimits(nodeResources.ToJobResources());
        execNode->SetDiskInfo(diskResources);

        return execNode;
    }

    void DoTestSchedule(
        const TRootElementPtr& rootElement,
        const TOperationElementPtr& operationElement,
        const TExecNodePtr& execNode)
    {
        auto schedulingContext = CreateSchedulingContext(SchedulerConfig_, execNode, /*runningJobs*/ {});
        TFairShareContext context(schedulingContext);
        TDynamicAttributesList dynamicAttributes;
        PrepareForTestScheduling(rootElement, &context, &dynamicAttributes);
        operationElement->ScheduleJob(&context);
    }

private:
    void PrepareForTestScheduling(
        const TRootElementPtr& rootElement,
        TFairShareContext* context,
        TDynamicAttributesList* dynamicAttributesList)
    {
        rootElement->Update(*dynamicAttributesList);
        context->Initialize(rootElement->GetTreeSize(), /*registeredSchedulingTagFilters*/ {});
        rootElement->PrescheduleJob(context, /*starvingOnly*/ false, /*aggressiveStarvationEnabled*/ false);
        context->PrescheduledCalled = true;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TFairShareTreeTest, TestAttributes)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(10);
    nodeResources.SetCpu(10);
    nodeResources.SetMemory(100);

    TJobResourcesWithQuota jobResources;
    jobResources.SetUserSlots(1);
    jobResources.SetCpu(1);
    jobResources.SetMemory(10);

    auto operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions->Weight = 1.0;

    auto host = New<TSchedulerStrategyHostMock>(TJobResourcesWithQuotaList(10, nodeResources));

    auto rootElement = CreateTestRootElement(host.Get());

    auto poolA = CreateTestPool(host.Get(), "A");
    auto poolB = CreateTestPool(host.Get(), "B");

    rootElement->AddChild(poolA);
    poolA->SetParent(rootElement.Get());

    rootElement->AddChild(poolB);
    poolB->SetParent(rootElement.Get());

    auto operationX = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(10, jobResources));
    auto operationElementX = CreateTestOperationElement(host.Get(), operationOptions, operationX.Get());

    poolA->AddChild(operationElementX);
    operationElementX->SetParent(poolA.Get());
    operationElementX->Enable();

    auto dynamicAttributes = TDynamicAttributesList(4);
    rootElement->Update(dynamicAttributes);

    EXPECT_EQ(0.1, rootElement->Attributes().DemandRatio);
    EXPECT_EQ(0.1, poolA->Attributes().DemandRatio);
    EXPECT_EQ(0.0, poolB->Attributes().DemandRatio);
    EXPECT_EQ(0.1, operationElementX->Attributes().DemandRatio);

    EXPECT_EQ(1.0, rootElement->Attributes().FairShareRatio);
    EXPECT_EQ(0.1, rootElement->Attributes().DemandRatio);
    EXPECT_EQ(0.0, poolB->Attributes().FairShareRatio);
    EXPECT_EQ(0.1, operationElementX->Attributes().FairShareRatio);
}

TEST_F(TFairShareTreeTest, TestUpdatePreemptableJobsList)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(10);
    nodeResources.SetCpu(10);
    nodeResources.SetMemory(100);

    TJobResourcesWithQuota jobResources;
    jobResources.SetUserSlots(1);
    jobResources.SetCpu(1);
    jobResources.SetMemory(10);

    auto operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions->Weight = 1.0;

    auto host = New<TSchedulerStrategyHostMock>(TJobResourcesWithQuotaList(10, nodeResources));

    auto rootElement = CreateTestRootElement(host.Get());

    auto operationX = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(10, jobResources));
    auto operationElementX = CreateTestOperationElement(host.Get(), operationOptions, operationX.Get());

    rootElement->AddChild(operationElementX);
    operationElementX->SetParent(rootElement.Get());
    operationElementX->Enable();

    std::vector<TJobId> jobIds;
    for (int i = 0; i < 150; ++i) {
        auto jobId = TGuid::Create();
        jobIds.push_back(jobId);
        operationElementX->OnJobStarted(jobId, jobResources.ToJobResources());
    }

    auto dynamicAttributes = TDynamicAttributesList(2);
    rootElement->Update(dynamicAttributes);

    EXPECT_EQ(1.6, operationElementX->Attributes().DemandRatio);
    EXPECT_EQ(1.0, operationElementX->Attributes().FairShareRatio);

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

TEST_F(TFairShareTreeTest, TestBestAllocationRatio)
{
    TJobResourcesWithQuota nodeResourcesA;
    nodeResourcesA.SetUserSlots(10);
    nodeResourcesA.SetCpu(10);
    nodeResourcesA.SetMemory(100);

    TJobResourcesWithQuota nodeResourcesB;
    nodeResourcesB.SetUserSlots(10);
    nodeResourcesB.SetCpu(10);
    nodeResourcesB.SetMemory(200);

    TJobResourcesWithQuota jobResources;
    jobResources.SetUserSlots(1);
    jobResources.SetCpu(1);
    jobResources.SetMemory(150);

    auto operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions->Weight = 1.0;

    auto host = New<TSchedulerStrategyHostMock>(TJobResourcesWithQuotaList({nodeResourcesA, nodeResourcesA, nodeResourcesB}));

    auto rootElement = CreateTestRootElement(host.Get());

    auto operationX = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(3, jobResources));
    auto operationElementX = CreateTestOperationElement(host.Get(), operationOptions, operationX.Get());

    rootElement->AddChild(operationElementX);
    operationElementX->SetParent(rootElement.Get());
    operationElementX->Enable();

    auto dynamicAttributes = TDynamicAttributesList(4);
    rootElement->Update(dynamicAttributes);

    EXPECT_EQ(1.125, operationElementX->Attributes().DemandRatio);
    EXPECT_EQ(0.375, operationElementX->Attributes().BestAllocationRatio);
    EXPECT_EQ(0.375, operationElementX->Attributes().FairShareRatio);
}

TEST_F(TFairShareTreeTest, TestOperationCountLimits)
{
    auto host = New<TSchedulerStrategyHostMock>();
    auto rootElement = CreateTestRootElement(host.Get());

    TPoolPtr pools[3];
    for (int i = 0; i < 3; ++i) {
        pools[i] = CreateTestPool(host.Get(), "pool" + ToString(i));
    }

    rootElement->AddChild(pools[0], /* enabled */ true);
    rootElement->AddChild(pools[1], /* enabled */ true);
    pools[0]->SetParent(rootElement.Get());
    pools[1]->SetParent(rootElement.Get());

    pools[1]->AddChild(pools[2], /* enabled */ true);
    pools[2]->SetParent(pools[1].Get());

    pools[2]->IncreaseOperationCount(1);
    pools[2]->IncreaseRunningOperationCount(1);

    EXPECT_EQ(1, rootElement->OperationCount());
    EXPECT_EQ(1, rootElement->RunningOperationCount());

    EXPECT_EQ(1, pools[1]->OperationCount());
    EXPECT_EQ(1, pools[1]->RunningOperationCount());

    pools[1]->IncreaseOperationCount(5);
    EXPECT_EQ(6, rootElement->OperationCount());
    for (int i = 0; i < 5; ++i) {
        pools[1]->IncreaseOperationCount(-1);
    }
    EXPECT_EQ(1, rootElement->OperationCount());

    pools[2]->IncreaseOperationCount(-1);
    pools[2]->IncreaseRunningOperationCount(-1);

    EXPECT_EQ(0, rootElement->OperationCount());
    EXPECT_EQ(0, rootElement->RunningOperationCount());
}

TEST_F(TFairShareTreeTest, TestMaxPossibleUsageRatioWithoutLimit)
{
    auto operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions->Weight = 1.0;

    // Total resource vector is <100, 100>.
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(100);
    auto host = New<TSchedulerStrategyHostMock>(TJobResourcesWithQuotaList({nodeResources}));

    // First operation with demand <5, 5>.
    TJobResourcesWithQuota firstOperationJobResources;
    firstOperationJobResources.SetCpu(5);
    firstOperationJobResources.SetMemory(5);

    auto firstOperation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(1, firstOperationJobResources));
    auto firstOperationElement = CreateTestOperationElement(host.Get(), operationOptions, firstOperation.Get());

    // Second operation with demand <5, 10>.
    TJobResourcesWithQuota secondOperationJobResources;
    secondOperationJobResources.SetCpu(5);
    secondOperationJobResources.SetMemory(10);

    auto secondOperation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(1, secondOperationJobResources));
    auto secondOperationElement = CreateTestOperationElement(host.Get(), operationOptions, secondOperation.Get());

    // Pool with total demand <10, 15>.
    auto pool = CreateTestPool(host.Get(), "A");

    pool->AddChild(firstOperationElement, true);
    firstOperationElement->SetParent(pool.Get());
    pool->AddChild(secondOperationElement);
    secondOperationElement->SetParent(pool.Get());

    // Root element.
    auto rootElement = CreateTestRootElement(host.Get());

    rootElement->AddChild(pool, true);
    pool->SetParent(rootElement.Get());

    // Сheck MaxPossibleUsageRatio computation.
    auto dynamicAttributes = TDynamicAttributesList(4);
    rootElement->Update(dynamicAttributes);
    EXPECT_EQ(0.15, pool->Attributes().MaxPossibleUsageRatio);
}

TEST_F(TFairShareTreeTest, DontSuggestMoreResourcesThanOperationNeeds)
{
    // Create 3 nodes.
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(100);
    nodeResources.SetDiskQuota(100);

    std::vector<TExecNodePtr> execNodes(3);
    for (int i = 0; i < execNodes.size(); ++i) {
        execNodes[i] = CreateTestExecNode(static_cast<NNodeTrackerClient::TNodeId>(i), nodeResources);
    }

    auto host = New<TSchedulerStrategyHostMock>(TJobResourcesWithQuotaList(execNodes.size(), nodeResources));

    // Create an operation with 2 jobs.
    TJobResourcesWithQuota operationJobResources;
    operationJobResources.SetCpu(10);
    operationJobResources.SetMemory(10);
    operationJobResources.SetDiskQuota(0);

    auto operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions->Weight = 1.0;
    auto operation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(2, operationJobResources));

    auto operationElement = CreateTestOperationElement(host.Get(), operationOptions, operation.Get());

    // Root element.
    auto rootElement = CreateTestRootElement(host.Get());
    rootElement->AddChild(operationElement, true);
    operationElement->SetParent(rootElement.Get());

    // We run operation with 2 jobs and simulate 3 concurrent heartbeats.
    // Two of them must succeed and call controller ScheduleJob,
    // the third one must skip ScheduleJob call since resource usage precommit is limited by operation demand.

    auto readyToGo = NewPromise<void>();
    auto& operationControllerStrategyHost = operation->GetOperationControllerStrategyHost();
    std::atomic<int> heartbeatsInScheduling(0);
    EXPECT_CALL(
        operationControllerStrategyHost,
        ScheduleJob(testing::_, testing::_, testing::_))
        .Times(2)
        .WillRepeatedly(testing::Invoke([&](auto context, auto jobLimits, auto treeId) {
            heartbeatsInScheduling.fetch_add(1);
            EXPECT_TRUE(NConcurrency::WaitFor(readyToGo.ToFuture()).IsOK());
            return MakeFuture<TScheduleJobResultPtr>(TErrorOr<TScheduleJobResultPtr>(New<TScheduleJobResult>()));
        }));

    std::vector<TFuture<void>> futures;
    NConcurrency::TActionQueue actionQueue;
    for (int i = 0; i < 2; ++i) {
        auto future = BIND([&, i]() {
            DoTestSchedule(rootElement, operationElement, execNodes[i]);
        }).AsyncVia(actionQueue.GetInvoker()).Run();
        futures.push_back(std::move(future));
    }

    while (heartbeatsInScheduling.load() != 2) {
        // Actively waiting.
    }
    // Number of expected calls to `operationControllerStrategyHost.ScheduleJob(...)` is set to 2.
    // In this way, the mock object library checks that this heartbeat doesn't get to actual scheduling.
    DoTestSchedule(rootElement, operationElement, execNodes[2]);
    readyToGo.Set();

    EXPECT_TRUE(Combine(futures).WithTimeout(TDuration::Seconds(2)).Get().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NScheduler
} // namespace NYT
