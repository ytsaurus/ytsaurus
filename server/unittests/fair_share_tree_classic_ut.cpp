#include <yt/core/test_framework/framework.h>

#include <yt/server/scheduler/fair_share_tree_element_classic.h>
#include <yt/server/scheduler/operation_controller.h>
#include <yt/server/scheduler/resource_tree.h>

#include <yt/ytlib/chunk_client/proto/medium_directory.pb.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/yson/null_consumer.h>

#include <contrib/libs/gmock/include/gmock/gmock.h>
#include <contrib/libs/gmock/include/gmock/gmock-matchers.h>
#include <contrib/libs/gmock/include/gmock/gmock-actions.h>

namespace NYT::NScheduler::NClassicScheduler {
namespace {

using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerStrategyHostMock
    : public TRefCounted
    , public ISchedulerStrategyHost
    , public TEventLogHostBase
{
    explicit TSchedulerStrategyHostMock(TJobResourcesWithQuotaList nodeResourceLimitsList)
        : NodeResourceLimitsList(std::move(nodeResourceLimitsList))
        , MediumDirectory_(New<NChunkClient::TMediumDirectory>())
    {
        NChunkClient::NProto::TMediumDirectory protoDirectory;
        auto* item = protoDirectory.add_items();
        item->set_name(NChunkClient::DefaultSlotsMediumName);
        item->set_index(NChunkClient::DefaultSlotsMediumIndex);
        item->set_priority(0);
        MediumDirectory_->LoadFrom(protoDirectory);
    }

    TSchedulerStrategyHostMock()
        : TSchedulerStrategyHostMock(TJobResourcesWithQuotaList{})
    { }

    virtual IInvokerPtr GetControlInvoker(EControlQueue queue) const override
    {
        YT_UNIMPLEMENTED();
    }

    virtual IInvokerPtr GetFairShareProfilingInvoker() const override
    {
        YT_UNIMPLEMENTED();
    }

    virtual IInvokerPtr GetFairShareUpdateInvoker() const override
    {
        return GetCurrentInvoker();
    }

    virtual TJobResources GetResourceLimits(const TSchedulingTagFilter& filter) override
    {
        if (!filter.IsEmpty()) {
            return {};
        }

        TJobResources totalResources;
        for (const auto& resources : NodeResourceLimitsList) {
            totalResources += resources.ToJobResources();
        }
        return totalResources;
    }

    virtual void Disconnect(const TError& error) override
    {
        YT_ABORT();
    }

    virtual TInstant GetConnectionTime() const override
    {
        return TInstant();
    }

    virtual void ActivateOperation(TOperationId operationId) override
    { }

    virtual void AbortOperation(TOperationId /* operationId */, const TError& /* error */) override
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
        YT_ABORT();
    }

    virtual void UpdateNodesOnChangedTrees(
        const THashMap<TString, NScheduler::TSchedulingTagFilter>& /* treeIdToFilter */) override
    {
        YT_UNIMPLEMENTED();
    }

    virtual std::vector<NNodeTrackerClient::TNodeId> GetExecNodeIds(
        const TSchedulingTagFilter& /* filter */) const override
    {
        return {};
    }

    virtual TString GetExecNodeAddress(NNodeTrackerClient::TNodeId nodeId) const override
    {
        YT_ABORT();
    }

    virtual void ValidatePoolPermission(
        const NYPath::TYPath& path,
        const TString& user,
        NYTree::EPermission permission) const override
    { }

    virtual void SetSchedulerAlert(ESchedulerAlertType alertType, const TError& alert) override
    { }

    virtual TFuture<void> SetOperationAlert(
        TOperationId operationId,
        EOperationAlertType alertType,
        const TError& alert,
        std::optional<TDuration> timeout) override
    {
        return VoidFuture;
    }

    virtual NYson::IYsonConsumer* GetEventLogConsumer() override
    {
        return NYson::GetNullYsonConsumer();
    }

    virtual const NLogging::TLogger* GetEventLogger() override
    {
        return nullptr;
    }

    virtual TString FormatResources(const TJobResourcesWithQuota& resources) const override
    {
        YT_VERIFY(MediumDirectory_);
        return NScheduler::FormatResources(resources, MediumDirectory_);
    }

    virtual TString FormatResourceUsage(
        const TJobResources& usage,
        const TJobResources& limits,
        const NNodeTrackerClient::NProto::TDiskResources& diskResources) const override
    {
        YT_VERIFY(MediumDirectory_);
        return NScheduler::FormatResourceUsage(usage, limits, diskResources, MediumDirectory_);
    }

    const NChunkClient::TMediumDirectoryPtr& GetMediumDirectory() const
    {
        return MediumDirectory_;
    }

    TJobResourcesWithQuotaList NodeResourceLimitsList;
    NChunkClient::TMediumDirectoryPtr MediumDirectory_;
};

DEFINE_REFCOUNTED_TYPE(TSchedulerStrategyHostMock)


class TOperationControllerStrategyHostMock
    : public IOperationControllerStrategyHost
{
public:
    explicit TOperationControllerStrategyHostMock(const TJobResourcesWithQuotaList& jobResourcesList)
        : JobResourcesList(jobResourcesList)
    { }

    MOCK_METHOD3(ScheduleJob, TFuture<TControllerScheduleJobResultPtr>(
        const ISchedulingContextPtr& context,
        const TJobResourcesWithQuota& jobLimits,
        const TString& treeId));

    MOCK_METHOD2(OnNonscheduledJobAborted, void(TJobId, EAbortReason));

    virtual TJobResources GetNeededResources() const override
    {
        TJobResources totalResources;
        for (const auto& resources : JobResourcesList) {
            totalResources += resources.ToJobResources();
        }
        return totalResources + DemandDisruption;
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

    EPreemptionMode PreemptionMode = EPreemptionMode::Normal;

    virtual EPreemptionMode GetPreemptionMode() const override
    {
        return PreemptionMode;
    }

    void SetDemandDisruption(TJobResources demandDisruption)
    {
        DemandDisruption = demandDisruption;
    }

private:
    TJobResourcesWithQuotaList JobResourcesList;
    TJobResourcesWithQuota DemandDisruption;
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
        YT_ABORT();
    }

    virtual std::optional<EUnschedulableReason> CheckUnschedulable() const override
    {
        return std::nullopt;
    }

    virtual TInstant GetStartTime() const override
    {
        return StartTime_;
    }

    virtual std::optional<int> FindSlotIndex(const TString& /* treeId */) const override
    {
        return 0;
    }

    virtual int GetSlotIndex(const TString& treeId) const override
    {
        return 0;
    }

    virtual void SetSlotIndex(const TString& /* treeId */, int /* slotIndex */) override
    { }

    virtual TString GetAuthenticatedUser() const override
    {
        return "root";
    }

    virtual TOperationId GetId() const override
    {
        return Id_;
    }

    virtual IOperationControllerStrategyHostPtr GetControllerStrategyHost() const override
    {
        return Controller_;
    }

    virtual const NYson::TYsonString& GetSpecString() const override
    {
        YT_ABORT();
    }

    virtual TOperationRuntimeParametersPtr GetRuntimeParameters() const override
    {
        YT_ABORT();
    }

    virtual bool GetActivated() const override
    {
        YT_ABORT();
    }

    TOperationControllerStrategyHostMock& GetOperationControllerStrategyHost()
    {
        return *Controller_.Get();
    }

    void SetErasedTrees(std::vector<TString> erasedTrees) override
    {
        YT_UNIMPLEMENTED();
    }

    const std::vector<TString>& ErasedTrees() const override
    {
        YT_UNIMPLEMENTED();
    }

    void EraseTree(const TString& treeId) override
    {
        YT_UNIMPLEMENTED();
    }

private:
    TInstant StartTime_;
    TOperationId Id_;
    TOperationControllerStrategyHostMockPtr Controller_;
};

DEFINE_REFCOUNTED_TYPE(TOperationStrategyHostMock)

class TFairShareTreeHostMock
    : public IFairShareTreeHost
{
public:
    TFairShareTreeHostMock()
        : ResourceTree_(New<TResourceTree>())
    { }

    virtual NProfiling::TAggregateGauge& GetProfilingCounter(const TString& name) override
    {
        return FakeCounter_;
    }

    virtual TResourceTree* GetResourceTree() override
    {
        return ResourceTree_.Get();
    }

private:
    NProfiling::TAggregateGauge FakeCounter_;
    TResourceTreePtr ResourceTree_;
};

class TClassicFairShareTreeTest
    : public testing::Test
{
public:
    TClassicFairShareTreeTest()
    {
        TreeConfig_->AggressivePreemptionSatisfactionThreshold = 0.5;
    }

protected:
    TSchedulerConfigPtr SchedulerConfig_ = New<TSchedulerConfig>();
    TFairShareStrategyTreeConfigPtr TreeConfig_ = New<TFairShareStrategyTreeConfig>();
    TIntrusivePtr<TFairShareTreeHostMock> FairShareTreeHostMock_ = New<TFairShareTreeHostMock>();
    TFairShareSchedulingStage SchedulingStageMock_ = TFairShareSchedulingStage(
        /* nameInLogs */ "Test scheduling stage",
        TScheduleJobsProfilingCounters("/test_scheduling_stage", /* treeIdProfilingTags */ {}));

    TDiskQuota CreateDiskQuota(i64 diskSpace)
    {
        TDiskQuota diskQuota;
        diskQuota.DiskSpacePerMedium[NChunkClient::DefaultSlotsMediumIndex] = diskSpace;
        return diskQuota;
    }

    TRootElementPtr CreateTestRootElement(ISchedulerStrategyHost* host)
    {
        return New<TRootElement>(
            host,
            FairShareTreeHostMock_.Get(),
            TreeConfig_,
            // TODO(ignat): eliminate profiling from test.
            NProfiling::TProfileManager::Get()->RegisterTag("pool", RootPoolName),
            "default",
            SchedulerLogger);
    }

    TPoolPtr CreateTestPool(ISchedulerStrategyHost* host, const TString& name)
    {
        return New<TPool>(
            host,
            FairShareTreeHostMock_.Get(),
            name,
            New<TPoolConfig>(),
            /* defaultConfigured */ true,
            TreeConfig_,
            // TODO(ignat): eliminate profiling from test.
            NProfiling::TProfileManager::Get()->RegisterTag("pool", name),
            "default",
            SchedulerLogger);
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
            FairShareTreeHostMock_.Get(),
            operation,
            "default",
            SchedulerLogger);
    }

    TExecNodePtr CreateTestExecNode(NNodeTrackerClient::TNodeId id, const TJobResourcesWithQuota& nodeResources)
    {
        NNodeTrackerClient::NProto::TDiskResources diskResources;
        diskResources.mutable_disk_location_resources()->Add();
        diskResources.mutable_disk_location_resources(0)->set_limit(nodeResources.GetDiskQuota().DiskSpacePerMedium[NChunkClient::DefaultSlotsMediumIndex]);

        auto execNode = New<TExecNode>(id, NNodeTrackerClient::TNodeDescriptor(), ENodeState::Online);
        execNode->SetResourceLimits(nodeResources.ToJobResources());
        execNode->SetDiskResources(diskResources);

        return execNode;
    }

    void DoTestSchedule(
        const TRootElementPtr& rootElement,
        const TOperationElementPtr& operationElement,
        const TExecNodePtr& execNode,
        const NChunkClient::TMediumDirectoryPtr& mediumDirectory)
    {
        auto schedulingContext = CreateSchedulingContext(
            /* nodeShardId */ 0,
            SchedulerConfig_,
            execNode,
            /* runningJobs */ {},
            mediumDirectory);
        TFairShareContext context(schedulingContext, /* enableSchedulingInfoLogging */ true, SchedulerLogger);
        TDynamicAttributesList dynamicAttributes;

        context.StartStage(&SchedulingStageMock_);
        PrepareForTestScheduling(rootElement, &context, &dynamicAttributes);
        operationElement->ScheduleJob(&context, /* ignorePacking */ true);
        context.FinishStage();
    }

private:
    void PrepareForTestScheduling(
        const TRootElementPtr& rootElement,
        TFairShareContext* context,
        TDynamicAttributesList* dynamicAttributesList)
    {
        TUpdateFairShareContext updateContext;
        rootElement->PreUpdate(dynamicAttributesList, &updateContext);
        rootElement->Update(dynamicAttributesList, &updateContext);

        context->Initialize(rootElement->GetTreeSize(), /*registeredSchedulingTagFilters*/ {});
        rootElement->PrescheduleJob(context, /*starvingOnly*/ false, /*aggressiveStarvationEnabled*/ false);
        context->PrescheduleCalled = true;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TClassicFairShareTreeTest, TestAttributes)
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

    poolA->AttachParent(rootElement.Get());
    poolB->AttachParent(rootElement.Get());

    auto operationX = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(10, jobResources));
    auto operationElementX = CreateTestOperationElement(host.Get(), operationOptions, operationX.Get());

    operationElementX->AttachParent(poolA.Get(), true);
    operationElementX->Enable();

    {
        auto dynamicAttributes = TDynamicAttributesList(4);

        TUpdateFairShareContext updateContext;
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        EXPECT_EQ(0.1, rootElement->Attributes().DemandRatio);
        EXPECT_EQ(0.1, poolA->Attributes().DemandRatio);
        EXPECT_EQ(0.0, poolB->Attributes().DemandRatio);
        EXPECT_EQ(0.1, operationElementX->Attributes().DemandRatio);

        EXPECT_EQ(0.1, rootElement->Attributes().FairShareRatio);
        EXPECT_EQ(0.1, rootElement->Attributes().DemandRatio);
        EXPECT_EQ(0.0, poolB->Attributes().FairShareRatio);
        EXPECT_EQ(0.1, operationElementX->Attributes().FairShareRatio);
    }

    std::vector<TJobId> jobIds;
    for (int i = 0; i < 10; ++i) {
        auto jobId = TGuid::Create();
        jobIds.push_back(jobId);
        operationElementX->OnJobStarted(
            jobId,
            jobResources.ToJobResources(),
            /* precommitedResources */ {});
    }

    {
        auto dynamicAttributes = TDynamicAttributesList(4);

        TUpdateFairShareContext updateContext;
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        EXPECT_EQ(0.5, dynamicAttributes[operationElementX->GetTreeIndex()].SatisfactionRatio);
        EXPECT_EQ(0.5, dynamicAttributes[poolA->GetTreeIndex()].SatisfactionRatio);
        EXPECT_EQ(std::numeric_limits<double>::max(), dynamicAttributes[poolB->GetTreeIndex()].SatisfactionRatio);
    }
}

TEST_F(TClassicFairShareTreeTest, TestUpdatePreemptableJobsList)
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

    operationElementX->AttachParent(rootElement.Get(), true);
    operationElementX->Enable();

    std::vector<TJobId> jobIds;
    for (int i = 0; i < 150; ++i) {
        auto jobId = TGuid::Create();
        jobIds.push_back(jobId);
        operationElementX->OnJobStarted(
            jobId,
            jobResources.ToJobResources(),
            /* precommitedResources */ {});
    }

    auto dynamicAttributes = TDynamicAttributesList(2);

    TUpdateFairShareContext updateContext;
    rootElement->PreUpdate(&dynamicAttributes, &updateContext);
    rootElement->Update(&dynamicAttributes, &updateContext);

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

TEST_F(TClassicFairShareTreeTest, TestBestAllocationRatio)
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

    operationElementX->AttachParent(rootElement.Get(), true);
    operationElementX->Enable();

    auto dynamicAttributes = TDynamicAttributesList(4);

    TUpdateFairShareContext updateContext;
    rootElement->PreUpdate(&dynamicAttributes, &updateContext);
    rootElement->Update(&dynamicAttributes, &updateContext);

    EXPECT_EQ(1.125, operationElementX->Attributes().DemandRatio);
    EXPECT_EQ(0.375, operationElementX->Attributes().BestAllocationRatio);
    EXPECT_EQ(0.375, operationElementX->Attributes().FairShareRatio);
}

TEST_F(TClassicFairShareTreeTest, TestOperationCountLimits)
{
    auto host = New<TSchedulerStrategyHostMock>();
    auto rootElement = CreateTestRootElement(host.Get());

    TPoolPtr pools[3];
    for (int i = 0; i < 3; ++i) {
        pools[i] = CreateTestPool(host.Get(), "pool" + ToString(i));
    }

    pools[0]->AttachParent(rootElement.Get());
    pools[1]->AttachParent(rootElement.Get());

    pools[2]->AttachParent(pools[1].Get());

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

TEST_F(TClassicFairShareTreeTest, TestMaxPossibleUsageRatioWithoutLimit)
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

    // Root element.
    auto rootElement = CreateTestRootElement(host.Get());
    pool->AttachParent(rootElement.Get());

    firstOperationElement->AttachParent(pool.Get(), true);
    secondOperationElement->AttachParent(pool.Get(), true);

    // Сheck MaxPossibleUsageRatio computation.
    auto dynamicAttributes = TDynamicAttributesList(4);

    TUpdateFairShareContext updateContext;
    rootElement->PreUpdate(&dynamicAttributes, &updateContext);
    rootElement->Update(&dynamicAttributes, &updateContext);
    EXPECT_EQ(0.15, pool->Attributes().MaxPossibleUsageRatio);
}

TEST_F(TClassicFairShareTreeTest, DontSuggestMoreResourcesThanOperationNeeds)
{
    // Create 3 nodes.
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(100);
    nodeResources.SetDiskQuota(CreateDiskQuota(100));

    std::vector<TExecNodePtr> execNodes(3);
    for (int i = 0; i < execNodes.size(); ++i) {
        execNodes[i] = CreateTestExecNode(static_cast<NNodeTrackerClient::TNodeId>(i), nodeResources);
    }

    auto host = New<TSchedulerStrategyHostMock>(TJobResourcesWithQuotaList(execNodes.size(), nodeResources));

    // Create an operation with 2 jobs.
    TJobResourcesWithQuota operationJobResources;
    operationJobResources.SetCpu(10);
    operationJobResources.SetMemory(10);
    operationJobResources.SetDiskQuota(CreateDiskQuota(0));

    auto operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions->Weight = 1.0;
    auto operation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(2, operationJobResources));

    auto operationElement = CreateTestOperationElement(host.Get(), operationOptions, operation.Get());

    // Root element.
    auto rootElement = CreateTestRootElement(host.Get());
    operationElement->AttachParent(rootElement.Get(), true);

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
            return MakeFuture<TControllerScheduleJobResultPtr>(
                TErrorOr<TControllerScheduleJobResultPtr>(New<TControllerScheduleJobResult>()));
        }));

    std::vector<TFuture<void>> futures;
    auto actionQueue = New<NConcurrency::TActionQueue>();
    for (int i = 0; i < 2; ++i) {
        auto future = BIND([&, i]() {
            DoTestSchedule(rootElement, operationElement, execNodes[i], host->GetMediumDirectory());
        }).AsyncVia(actionQueue->GetInvoker()).Run();
        futures.push_back(std::move(future));
    }

    while (heartbeatsInScheduling.load() != 2) {
        // Actively waiting.
    }
    // Number of expected calls to `operationControllerStrategyHost.ScheduleJob(...)` is set to 2.
    // In this way, the mock object library checks that this heartbeat doesn't get to actual scheduling.
    DoTestSchedule(rootElement, operationElement, execNodes[2], host->GetMediumDirectory());
    readyToGo.Set();

    EXPECT_TRUE(Combine(futures).WithTimeout(TDuration::Seconds(2)).Get().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TClassicFairShareTreeTest, DoNotPreemptJobsIfFairShareRatioEqualToDemandRatio)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(100);
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(100);
    nodeResources.SetDiskQuota(CreateDiskQuota(100));

    auto execNode = CreateTestExecNode(static_cast<NNodeTrackerClient::TNodeId>(0), nodeResources);

    auto host = New<TSchedulerStrategyHostMock>(TJobResourcesWithQuotaList(1, nodeResources));

    // Create an operation with 4 jobs.
    TJobResourcesWithQuota jobResources;
    jobResources.SetCpu(10);
    jobResources.SetMemory(10);
    jobResources.SetDiskQuota(CreateDiskQuota(0));

    auto operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions->Weight = 1.0;
    auto operation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList({}));

    auto operationElement = CreateTestOperationElement(host.Get(), operationOptions, operation.Get());

    // Root element.
    auto rootElement = CreateTestRootElement(host.Get());
    operationElement->AttachParent(rootElement.Get(), true);
    operationElement->Enable();

    std::vector<TJobId> jobIds;
    for (int i = 0; i < 4; ++i) {
        auto jobId = TGuid::Create();
        jobIds.push_back(jobId);
        operationElement->OnJobStarted(
            jobId,
            jobResources.ToJobResources(),
            /* precommitedResources */ {});
    }

    auto dynamicAttributes = TDynamicAttributesList(2);

    TUpdateFairShareContext updateContext;
    rootElement->PreUpdate(&dynamicAttributes, &updateContext);
    rootElement->Update(&dynamicAttributes, &updateContext);

    EXPECT_EQ(0.4, operationElement->Attributes().DemandRatio);
    EXPECT_EQ(0.4, operationElement->Attributes().FairShareRatio);

    for (int i = 0; i < 2; ++i) {
        EXPECT_FALSE(operationElement->IsJobPreemptable(jobIds[i], true));
    }
    for (int i = 2; i < 4; ++i) {
        EXPECT_FALSE(operationElement->IsJobPreemptable(jobIds[i], false));
        EXPECT_TRUE(operationElement->IsJobPreemptable(jobIds[i], true));
    }

    TJobResources delta;
    delta.SetCpu(10);
    delta.SetMemory(10);
    operationElement->IncreaseJobResourceUsage(jobIds[0], delta);

    for (int i = 0; i < 1; ++i) {
        EXPECT_FALSE(operationElement->IsJobPreemptable(jobIds[i], true));
    }
    for (int i = 1; i < 4; ++i) {
        EXPECT_FALSE(operationElement->IsJobPreemptable(jobIds[i], false));
        EXPECT_TRUE(operationElement->IsJobPreemptable(jobIds[i], true));
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TClassicFairShareTreeTest, MaxPossibleResourceUsage)
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
    auto host = New<TSchedulerStrategyHostMock>(TJobResourcesWithQuotaList({nodeResources}));
    auto rootElement = CreateTestRootElement(host.Get());

    auto operationX = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(1, jobResources));
    auto operationElementX = CreateTestOperationElement(host.Get(), operationOptions, operationX.Get());

    auto operationY = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(1, jobResources));
    auto operationElementY = CreateTestOperationElement(host.Get(), operationOptions, operationY.Get());

    operationElementX->AttachParent(rootElement.Get(), true);
    operationElementX->Enable();

    operationElementY->AttachParent(rootElement.Get(), true);
    operationElementY->Enable();

    for (int i = 0; i < 2; ++i) {
        operationElementX->OnJobStarted(
            TGuid::Create(),
            jobResources.ToJobResources(),
            /* precommitedResources */ {});
    }

    for (int i = 0; i < 9; ++i) {
        operationElementY->OnJobStarted(
            TGuid::Create(),
            jobResources.ToJobResources(),
            /* precommitedResources */ {});
    }

    auto dynamicAttributes = TDynamicAttributesList(3);

    {
        TUpdateFairShareContext updateContext;
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        EXPECT_NEAR(0.3, operationElementX->Attributes().DemandRatio, 1e-7);
        EXPECT_NEAR(1.0, operationElementX->Attributes().BestAllocationRatio, 1e-7);
        EXPECT_NEAR(0.3, operationElementX->Attributes().MaxPossibleUsageRatio, 1e-7);
        EXPECT_NEAR(0.3, operationElementX->Attributes().FairShareRatio, 1e-7);

        EXPECT_NEAR(1.0, operationElementY->Attributes().DemandRatio, 1e-7);
        EXPECT_NEAR(1.0, operationElementY->Attributes().BestAllocationRatio, 1e-7);
        EXPECT_NEAR(1.0, operationElementY->Attributes().MaxPossibleUsageRatio, 1e-7);
        EXPECT_NEAR(0.7, operationElementY->Attributes().FairShareRatio, 1e-7);

        EXPECT_NEAR(1.0, rootElement->Attributes().MaxPossibleUsageRatio, 1e-7);
    }

    {
        TJobResourcesWithQuota demandDisruption;
        demandDisruption.SetUserSlots(-1);
        demandDisruption.SetCpu(-1.1);
        demandDisruption.SetMemory(-11);
        operationX->GetOperationControllerStrategyHost().SetDemandDisruption(demandDisruption);
        operationY->GetOperationControllerStrategyHost().SetDemandDisruption(demandDisruption);

        TUpdateFairShareContext updateContext;
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        EXPECT_NEAR(1.0, rootElement->Attributes().MaxPossibleUsageRatio, 1e-7);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TClassicFairShareTreeTest, TestFifo)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(30);
    nodeResources.SetCpu(20);
    nodeResources.SetMemory(200);

    auto host = New<TSchedulerStrategyHostMock>(TJobResourcesWithQuotaList(1, nodeResources));

    auto rootElement = CreateTestRootElement(host.Get());

    auto pool = CreateTestPool(host.Get(), "A");
    pool->AttachParent(rootElement.Get());
    pool->SetMode(ESchedulingMode::Fifo);

    auto operationOptionsX = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptionsX->Weight = 3.0;
    TJobResourcesWithQuota jobResourcesX;
    jobResourcesX.SetUserSlots(1);
    jobResourcesX.SetCpu(1);
    jobResourcesX.SetMemory(30);
    auto operationX = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(5, jobResourcesX));
    auto operationElementX = CreateTestOperationElement(host.Get(), operationOptionsX, operationX.Get());

    auto operationOptionsY = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptionsY->Weight = 2.0;
    TJobResourcesWithQuota jobResourcesY;
    jobResourcesY.SetUserSlots(1);
    jobResourcesY.SetCpu(3);
    jobResourcesY.SetMemory(10);
    auto operationY = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(5, jobResourcesY));
    auto operationElementY = CreateTestOperationElement(host.Get(), operationOptionsY, operationY.Get());

    auto operationOptionsZ = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptionsZ->Weight = 1.0;
    TJobResourcesWithQuota jobResourcesZ;
    jobResourcesZ.SetUserSlots(1);
    jobResourcesZ.SetCpu(1);
    jobResourcesZ.SetMemory(10);
    auto operationZ = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(5, jobResourcesZ));
    auto operationElementZ = CreateTestOperationElement(host.Get(), operationOptionsZ, operationZ.Get());

    operationElementX->AttachParent(pool.Get(), true);
    operationElementX->Enable();

    operationElementY->AttachParent(pool.Get(), true);
    operationElementY->Enable();

    operationElementZ->AttachParent(pool.Get(), true);
    operationElementZ->Enable();

    {
        auto dynamicAttributes = TDynamicAttributesList(5);

        TUpdateFairShareContext updateContext;
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        // We call UpdateBottomUp() and UpdateTopDown() directly here, because Update() verifies current invoker.
        rootElement->UpdateBottomUp(&dynamicAttributes, &updateContext);
        rootElement->UpdateTopDown(&dynamicAttributes, &updateContext);

        EXPECT_EQ(1.25, rootElement->Attributes().DemandRatio);
        EXPECT_EQ(1.0, rootElement->Attributes().FairShareRatio);

        EXPECT_EQ(1.25, pool->Attributes().DemandRatio);
        EXPECT_EQ(1.0, pool->Attributes().FairShareRatio);

        EXPECT_EQ(0.75, operationElementX->Attributes().DemandRatio);
        EXPECT_EQ(0.75, operationElementX->Attributes().FairShareRatio);

        EXPECT_EQ(0.75, operationElementY->Attributes().DemandRatio);
        EXPECT_EQ(0.75, operationElementY->Attributes().FairShareRatio);

        EXPECT_EQ(0.25, operationElementZ->Attributes().DemandRatio);
        EXPECT_EQ(0.0, operationElementZ->Attributes().FairShareRatio);
    }
}

TEST_F(TClassicFairShareTreeTest, TestFifo2)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(30);
    nodeResources.SetCpu(20);
    nodeResources.SetMemory(200);

    auto host = New<TSchedulerStrategyHostMock>(TJobResourcesWithQuotaList(1, nodeResources));

    auto rootElement = CreateTestRootElement(host.Get());

    auto pool = CreateTestPool(host.Get(), "A");
    pool->AttachParent(rootElement.Get());
    pool->SetMode(ESchedulingMode::Fifo);

    auto operationOptionsX = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptionsX->Weight = 3.0;
    TJobResourcesWithQuota jobResourcesX;
    jobResourcesX.SetUserSlots(1);
    jobResourcesX.SetCpu(0);
    jobResourcesX.SetMemory(50);
    auto operationX = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(5, jobResourcesX));
    auto operationElementX = CreateTestOperationElement(host.Get(), operationOptionsX, operationX.Get());

    auto operationOptionsY = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptionsY->Weight = 2.0;
    TJobResourcesWithQuota jobResourcesY;
    jobResourcesY.SetUserSlots(1);
    jobResourcesY.SetCpu(5);
    jobResourcesY.SetMemory(0);
    auto operationY = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(5, jobResourcesY));
    auto operationElementY = CreateTestOperationElement(host.Get(), operationOptionsY, operationY.Get());

    auto operationOptionsZ = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptionsZ->Weight = 1.0;
    TJobResourcesWithQuota jobResourcesZ;
    jobResourcesZ.SetUserSlots(1);
    jobResourcesZ.SetCpu(1);
    jobResourcesZ.SetMemory(10);
    auto operationZ = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(5, jobResourcesZ));
    auto operationElementZ = CreateTestOperationElement(host.Get(), operationOptionsZ, operationZ.Get());

    operationElementX->AttachParent(pool.Get(), true);
    operationElementX->Enable();

    operationElementY->AttachParent(pool.Get(), true);
    operationElementY->Enable();

    operationElementZ->AttachParent(pool.Get(), true);
    operationElementZ->Enable();

    {
        auto dynamicAttributes = TDynamicAttributesList(5);

        TUpdateFairShareContext updateContext;
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        // We call UpdateBottomUp() and UpdateTopDown() directly here, because Update() verifies current invoker.
        rootElement->UpdateBottomUp(&dynamicAttributes, &updateContext);
        rootElement->UpdateTopDown(&dynamicAttributes, &updateContext);

        EXPECT_EQ(1.5, rootElement->Attributes().DemandRatio);
        EXPECT_EQ(1.0, rootElement->Attributes().FairShareRatio);

        EXPECT_EQ(1.5, pool->Attributes().DemandRatio);
        EXPECT_EQ(1.0, pool->Attributes().FairShareRatio);

        EXPECT_EQ(1.25, operationElementX->Attributes().DemandRatio);
        EXPECT_EQ(1.0, operationElementX->Attributes().FairShareRatio);

        EXPECT_EQ(1.25, operationElementY->Attributes().DemandRatio);
        EXPECT_EQ(0.0, operationElementY->Attributes().FairShareRatio);

        EXPECT_EQ(0.25, operationElementZ->Attributes().DemandRatio);
        EXPECT_EQ(0.0, operationElementZ->Attributes().FairShareRatio);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NScheduler::NClassicScheduler
