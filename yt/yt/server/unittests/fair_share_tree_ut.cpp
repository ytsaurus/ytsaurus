#include <yt/core/test_framework/framework.h>

#include <yt/server/scheduler/fair_share_tree_element.h>
#include <yt/server/scheduler/operation_controller.h>
#include <yt/server/scheduler/resource_tree.h>

#include <yt/ytlib/chunk_client/proto/medium_directory.pb.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/yson/null_consumer.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NScheduler::NVectorScheduler {

////////////////////////////////////////////////////////////////////////////////

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

    virtual IInvokerPtr GetFairShareLoggingInvoker() const override
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

    virtual NEventLog::TFluentLogEvent LogFairShareEventFluently(TInstant now) override
    {
        YT_UNIMPLEMENTED();
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

    virtual void FlushOperationNode(TOperationId operationId) override
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
        YT_UNIMPLEMENTED();
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

    virtual void LogResourceMetering(
        const TMeteringKey& /*key*/,
        const TMeteringStatistics& /*statistics*/,
        TInstant /*lastUpdateTime*/,
        TInstant /*now*/) override
    { }

    virtual int GetDefaultAbcId() const override
    {
        return -1;
    }

    const NChunkClient::TMediumDirectoryPtr& GetMediumDirectory() const
    {
        return MediumDirectory_;
    }

    virtual void StoreStrategyStateAsync(TPersistentStrategyStatePtr /* persistentStrategyState */) override
    { }

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

    EPreemptionMode PreemptionMode = EPreemptionMode::Normal;

    virtual EPreemptionMode GetPreemptionMode() const override
    {
        return PreemptionMode;
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
        YT_ABORT();
    }
    
    virtual EOperationState GetState() const override
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

    virtual void EraseTrees(const std::vector<TString>& treeIds) override
    { }

    virtual std::optional<TJobResources> GetInitialAggregatedMinNeededResources() const override
    {
        return std::nullopt;
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
    explicit TFairShareTreeHostMock(const TFairShareStrategyTreeConfigPtr& treeConfig)
        : ResourceTree_(New<TResourceTree>(treeConfig))
    { }

    virtual NProfiling::TShardedAggregateGauge& GetProfilingCounter(const TString& name) override
    {
        return FakeCounter_;
    }

    virtual TResourceTree* GetResourceTree() override
    {
        return ResourceTree_.Get();
    }

private:
    NProfiling::TShardedAggregateGauge FakeCounter_;
    TResourceTreePtr ResourceTree_;
};

class TFairShareTreeTest
    : public testing::Test
{
public:
    TFairShareTreeTest()
    {
        TreeConfig_->AggressivePreemptionSatisfactionThreshold = 0.5;
    }

protected:
    TSchedulerConfigPtr SchedulerConfig_ = New<TSchedulerConfig>();
    TFairShareStrategyTreeConfigPtr TreeConfig_ = New<TFairShareStrategyTreeConfig>();
    TIntrusivePtr<TFairShareTreeHostMock> FairShareTreeHostMock_ = New<TFairShareTreeHostMock>(TreeConfig_);
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

    TPoolPtr CreateTestPool(ISchedulerStrategyHost* host, const TString& name, TPoolConfigPtr config = New<TPoolConfig>())
    {
        return New<TPool>(
            host,
            FairShareTreeHostMock_.Get(),
            name,
            config,
            /* defaultConfigured */ true,
            TreeConfig_,
            // TODO(ignat): eliminate profiling from test.
            NProfiling::TProfileManager::Get()->RegisterTag("pool", name),
            "default",
            SchedulerLogger);
    }

    TPoolConfigPtr CreateSimplePoolConfig(double minShareCpu, double weight = 1.0)
    {
        auto relaxedPoolConfig = New<TPoolConfig>();
        relaxedPoolConfig->MinShareResources->Cpu = minShareCpu;
        relaxedPoolConfig->Weight = weight;
        return relaxedPoolConfig;
    }

    TPoolConfigPtr CreateBurstPoolConfig(double flowCpu, double burstCpu, double minShareCpu = 0.0, double weight = 1.0)
    {
        auto burstPoolConfig = CreateSimplePoolConfig(minShareCpu, weight);
        burstPoolConfig->IntegralGuarantees->GuaranteeType = EIntegralGuaranteeType::Burst;
        burstPoolConfig->IntegralGuarantees->ResourceFlow->Cpu = flowCpu;
        burstPoolConfig->IntegralGuarantees->BurstGuaranteeResources->Cpu = burstCpu;
        return burstPoolConfig;
    }

    TPoolConfigPtr CreateRelaxedPoolConfig(double flowCpu, double minShareCpu = 0.0, double weight = 1.0)
    {
        auto relaxedPoolConfig = CreateSimplePoolConfig(minShareCpu, weight);
        relaxedPoolConfig->IntegralGuarantees->GuaranteeType = EIntegralGuaranteeType::Relaxed;
        relaxedPoolConfig->IntegralGuarantees->ResourceFlow->Cpu = flowCpu;
        return relaxedPoolConfig;
    }

    TOperationElementPtr CreateTestOperationElement(
        ISchedulerStrategyHost* host,
        IOperationStrategyHost* operation,
        TOperationFairShareTreeRuntimeParametersPtr operationOptions = nullptr)
    {
        auto operationController = New<TFairShareStrategyOperationController>(operation, SchedulerConfig_);
        if (!operationOptions) {
            operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
            operationOptions->Weight = 1.0;
        }
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

    std::pair<TOperationElementPtr, TIntrusivePtr<TOperationStrategyHostMock>> CreateOperationWithJobs(
        int jobCount,
        ISchedulerStrategyHost* host,
        TCompositeSchedulerElement* parent)
    {
        TJobResourcesWithQuota jobResources;
        jobResources.SetUserSlots(1);
        jobResources.SetCpu(1);
        jobResources.SetMemory(10_MB);

        auto operationHost = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(jobCount, jobResources));
        auto operationElement = CreateTestOperationElement(host, operationHost.Get());
        operationElement->Enable();
        operationElement->AttachParent(parent, true);
        return {operationElement, operationHost};
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

        context->Initialize(rootElement->GetTreeSize(), /* registeredSchedulingTagFilters */ {});
        rootElement->PrescheduleJob(context, EPrescheduleJobOperationCriterion::All, /* aggressiveStarvationEnabled */ false);
        context->SetPrescheduleCalled(true);
    }
};


TIntrusivePtr<TSchedulerStrategyHostMock> CreateHostWith10NodesAnd10Cpu()
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(10);
    nodeResources.SetCpu(10);
    nodeResources.SetMemory(100_MB);

    return New<TSchedulerStrategyHostMock>(TJobResourcesWithQuotaList(10, nodeResources));
}

void ResetFairShareFunctionsRecursively(TCompositeSchedulerElement* compositeElement)
{
    compositeElement->ResetFairShareFunctions();
    for (const auto& child : compositeElement->GetEnabledChildren()) {
        if (TPool* childPool = child->AsPool()) {
            ResetFairShareFunctionsRecursively(childPool);
        } else {
            child->ResetFairShareFunctions();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

MATCHER_P2(ResourceVectorNear, vec, absError, "") {
//    *result_listener << "where the remainder is " << (arg % n);
    return TResourceVector::Near(arg, vec, absError);
}

#define EXPECT_RV_NEAR(vector1, vector2) \
    EXPECT_THAT(vector2, ResourceVectorNear(vector1, 1e-6))

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

    auto poolA = CreateTestPool(host.Get(), "PoolA");
    auto poolB = CreateTestPool(host.Get(), "PoolB");

    poolA->AttachParent(rootElement.Get());
    poolB->AttachParent(rootElement.Get());

    auto operationX = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(10, jobResources));
    auto operationElementX = CreateTestOperationElement(host.Get(), operationX.Get(), operationOptions);

    operationElementX->AttachParent(poolA.Get(), true);
    operationElementX->Enable();

    {
        TDynamicAttributesList dynamicAttributes = {};

        TUpdateFairShareContext updateContext;
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        TResourceVector expectedOperationDemand = TResourceVector::FromJobResources(jobResources, nodeResources, 0.0, 1.0);
        EXPECT_EQ(expectedOperationDemand, rootElement->Attributes().DemandShare);
        EXPECT_EQ(expectedOperationDemand, poolA->Attributes().DemandShare);
        EXPECT_EQ(TResourceVector::Zero(), poolB->Attributes().DemandShare);
        EXPECT_EQ(expectedOperationDemand, operationElementX->Attributes().DemandShare);

        EXPECT_EQ(expectedOperationDemand, rootElement->Attributes().FairShare.Total);
        EXPECT_EQ(expectedOperationDemand, rootElement->Attributes().DemandShare);
        EXPECT_EQ(TResourceVector::Zero(), poolB->Attributes().FairShare.Total);
        EXPECT_EQ(expectedOperationDemand, operationElementX->Attributes().FairShare.Total);
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
        ResetFairShareFunctionsRecursively(rootElement.Get());
        auto dynamicAttributes = TDynamicAttributesList(4);

        TUpdateFairShareContext updateContext;
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        // Demand increased to 0.2 due to started jobs, so did fair share.
        // usage(0.1) / fair_share(0.2) = 0.5
        EXPECT_EQ(0.5, dynamicAttributes[operationElementX->GetTreeIndex()].SatisfactionRatio);
        EXPECT_EQ(0.5, dynamicAttributes[poolA->GetTreeIndex()].SatisfactionRatio);
        EXPECT_EQ(InfiniteSatisfactionRatio, dynamicAttributes[poolB->GetTreeIndex()].SatisfactionRatio);
    }
}

TEST_F(TFairShareTreeTest, TestResourceLimits)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(10);
    nodeResources.SetCpu(10);
    nodeResources.SetMemory(100);

    auto host = New<TSchedulerStrategyHostMock>(TJobResourcesWithQuotaList(1, nodeResources));

    auto rootElement = CreateTestRootElement(host.Get());

    auto poolA = CreateTestPool(host.Get(), "PoolA");
    poolA->AttachParent(rootElement.Get());

    auto poolB = CreateTestPool(host.Get(), "PoolB");
    poolB->AttachParent(poolA.Get());

    {
        TDynamicAttributesList dynamicAttributes = {};
        TUpdateFairShareContext updateContext;

        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        EXPECT_EQ(TResourceVector::Ones(), rootElement->Attributes().LimitsShare);
        EXPECT_EQ(nodeResources.ToJobResources(), rootElement->ResourceLimits());
        EXPECT_EQ(nodeResources.ToJobResources(), rootElement->GetTotalResourceLimits());

        EXPECT_EQ(TResourceVector::Ones(), poolA->Attributes().LimitsShare);
        EXPECT_EQ(nodeResources.ToJobResources(), poolA->ResourceLimits());
        EXPECT_EQ(nodeResources.ToJobResources(), poolA->GetTotalResourceLimits());

        EXPECT_EQ(TResourceVector::Ones(), poolB->Attributes().LimitsShare);
        EXPECT_EQ(nodeResources.ToJobResources(), poolB->ResourceLimits());
        EXPECT_EQ(nodeResources.ToJobResources(), poolB->GetTotalResourceLimits());
    }

    TJobResources poolAResourceLimits;
    poolAResourceLimits.SetUserSlots(6);
    poolAResourceLimits.SetCpu(7);
    poolAResourceLimits.SetMemory(80);

    auto poolAConfig = poolA->GetConfig();
    poolAConfig->ResourceLimits->UserSlots = poolAResourceLimits.GetUserSlots();
    poolAConfig->ResourceLimits->Cpu = static_cast<double>(poolAResourceLimits.GetCpu());
    poolAConfig->ResourceLimits->Memory = poolAResourceLimits.GetMemory();
    poolA->SetConfig(poolAConfig);

    const double maxShareRatio = 0.9;
    auto poolBConfig = poolB->GetConfig();
    poolBConfig->MaxShareRatio = maxShareRatio;
    poolB->SetConfig(poolBConfig);

    {
        TDynamicAttributesList dynamicAttributes = {};
        TUpdateFairShareContext updateContext;

        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        EXPECT_EQ(TResourceVector::Ones(), rootElement->Attributes().LimitsShare);
        EXPECT_EQ(nodeResources.ToJobResources(), rootElement->ResourceLimits());
        EXPECT_EQ(nodeResources.ToJobResources(), rootElement->GetTotalResourceLimits());

        auto poolALimitsShare = TResourceVector::FromJobResources(poolAResourceLimits, nodeResources, 1.0, 1.0);
        EXPECT_EQ(poolALimitsShare, poolA->Attributes().LimitsShare);
        EXPECT_EQ(poolAResourceLimits, poolA->ResourceLimits());
        EXPECT_EQ(nodeResources.ToJobResources(), poolA->GetTotalResourceLimits());

        auto poolBResourceLimits = nodeResources * maxShareRatio;
        auto poolBLimitsShare = TResourceVector::FromJobResources(poolBResourceLimits, nodeResources, 1.0, 1.0);
        EXPECT_EQ(poolBLimitsShare, poolB->Attributes().LimitsShare);
        EXPECT_EQ(poolBResourceLimits, poolB->ResourceLimits());
        EXPECT_EQ(nodeResources.ToJobResources(), poolB->GetTotalResourceLimits());
    }
}

TEST_F(TFairShareTreeTest, TestFractionalResourceLimits)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(10);
    nodeResources.SetCpu(11.17);
    nodeResources.SetMemory(100);

    auto host = New<TSchedulerStrategyHostMock>(TJobResourcesWithQuotaList(1, nodeResources));

    auto rootElement = CreateTestRootElement(host.Get());

    auto poolA = CreateTestPool(host.Get(), "PoolA");
    poolA->AttachParent(rootElement.Get());

    const double maxShareRatio = 0.99;

    auto poolConfig = poolA->GetConfig();
    poolConfig->MaxShareRatio = maxShareRatio;
    poolA->SetConfig(poolConfig);

    TJobResourcesWithQuota poolResourceLimits;
    poolResourceLimits.SetUserSlots(10);
    poolResourceLimits.SetCpu(11.06);
    poolResourceLimits.SetMemory(99);

    {
        TDynamicAttributesList dynamicAttributes = {};
        TUpdateFairShareContext updateContext;

        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        EXPECT_EQ(TResourceVector::Ones(), rootElement->Attributes().LimitsShare);
        EXPECT_EQ(nodeResources.ToJobResources(), rootElement->ResourceLimits());
        EXPECT_EQ(nodeResources.ToJobResources(), rootElement->GetTotalResourceLimits());

        auto poolLimitsShare = TResourceVector::FromJobResources(poolResourceLimits, nodeResources, 1.0, 1.0);
        EXPECT_EQ(poolLimitsShare, poolA->Attributes().LimitsShare);
        EXPECT_EQ(poolResourceLimits.ToJobResources(), poolA->ResourceLimits());
        EXPECT_EQ(nodeResources.ToJobResources(), poolA->GetTotalResourceLimits());
    }
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
    auto operationElementX = CreateTestOperationElement(host.Get(), operationX.Get(), operationOptions);

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

    TDynamicAttributesList dynamicAttributes;
    TUpdateFairShareContext updateContext;
    rootElement->PreUpdate(&dynamicAttributes, &updateContext);
    rootElement->Update(&dynamicAttributes, &updateContext);

    EXPECT_EQ(1.6, operationElementX->Attributes().GetDemandRatio());
    EXPECT_EQ(1.0, operationElementX->Attributes().GetFairShareRatio());

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

TEST_F(TFairShareTreeTest, TestBestAllocationShare)
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
    auto operationElementX = CreateTestOperationElement(host.Get(), operationX.Get(), operationOptions);

    operationElementX->AttachParent(rootElement.Get(), true);
    operationElementX->Enable();

    TDynamicAttributesList dynamicAttributes = {};

    TUpdateFairShareContext updateContext;
    rootElement->PreUpdate(&dynamicAttributes, &updateContext);
    rootElement->Update(&dynamicAttributes, &updateContext);

    auto totalResources = nodeResourcesA * 2. + nodeResourcesB;
    auto demandShare = TResourceVector::FromJobResources(jobResources * 3., totalResources, 0.0, 1.0);
    auto fairShare = TResourceVector::FromJobResources(jobResources, totalResources, 0.0, 1.0);
    EXPECT_EQ(demandShare, operationElementX->Attributes().DemandShare);
    EXPECT_EQ(0.375, operationElementX->PersistentAttributes().BestAllocationShare[EJobResourceType::Memory]);
    EXPECT_THAT(
        fairShare,
        ResourceVectorNear(operationElementX->Attributes().FairShare.Total, 1e-7));
}

TEST_F(TFairShareTreeTest, TestOperationCountLimits)
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

TEST_F(TFairShareTreeTest, DontSuggestMoreResourcesThanOperationNeeds)
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

    auto operationElement = CreateTestOperationElement(host.Get(), operation.Get(), operationOptions);

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

    EXPECT_TRUE(AllSucceeded(futures).WithTimeout(TDuration::Seconds(2)).Get().IsOK());
}

TEST_F(TFairShareTreeTest, TestVectorFairShareEmptyTree)
{
    // Create a cluster with 1 large node
    const int nodeCount = 1;
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(100);
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(1000);

    auto host = New<TSchedulerStrategyHostMock>(TJobResourcesWithQuotaList(nodeCount, nodeResources));

    // Create a tree with 2 pools
    auto rootElement = CreateTestRootElement(host.Get());
    auto poolA = CreateTestPool(host.Get(), "PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateTestPool(host.Get(), "PoolB");
    poolB->AttachParent(rootElement.Get());

    // Update tree
    TDynamicAttributesList dynamicAttributes = {};
    TUpdateFairShareContext updateContext;
    rootElement->PreUpdate(&dynamicAttributes, &updateContext);
    rootElement->Update(&dynamicAttributes, &updateContext);

    // Check the values
    EXPECT_EQ(TResourceVector::Zero(), rootElement->GetFairShare());
    EXPECT_EQ(TResourceVector::Zero(), poolA->GetFairShare());
    EXPECT_EQ(TResourceVector::Zero(), poolB->GetFairShare());
}

TEST_F(TFairShareTreeTest, TestVectorFairShareOneLargeOperation)
{
    // Create a cluster with 1 large node
    const int nodeCount = 1;
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(100);
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(1000);

    auto host = New<TSchedulerStrategyHostMock>(TJobResourcesWithQuotaList(nodeCount, nodeResources));

    // Create a tree with 2 pools
    auto rootElement = CreateTestRootElement(host.Get());
    auto poolA = CreateTestPool(host.Get(), "PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateTestPool(host.Get(), "PoolB");
    poolB->AttachParent(rootElement.Get());

    // Create operation with demand larger than the available resources
    const int jobCount = 200;
    TJobResourcesWithQuota jobResources;
    jobResources.SetUserSlots(1);
    jobResources.SetCpu(1);
    jobResources.SetMemory(20);

    auto operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions->Weight = 1.0;

    auto operationX = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(jobCount, jobResources));
    auto operationElementX = CreateTestOperationElement(host.Get(), operationX.Get(), operationOptions);

    operationElementX->AttachParent(poolA.Get(), true);
    operationElementX->Enable();

    // Update tree
    TDynamicAttributesList dynamicAttributes = {};
    TUpdateFairShareContext updateContext;
    rootElement->PreUpdate(&dynamicAttributes, &updateContext);
    rootElement->Update(&dynamicAttributes, &updateContext);

    // Check the values
    EXPECT_EQ(TResourceVector({0.5, 0.5, 0.0, 1.0, 0.0}), rootElement->GetFairShare());
    EXPECT_EQ(TResourceVector({0.5, 0.5, 0.0, 1.0, 0.0}), poolA->GetFairShare());
    EXPECT_EQ(TResourceVector({0.5, 0.5, 0.0, 1.0, 0.0}), operationElementX->GetFairShare());
    EXPECT_EQ(TResourceVector::Zero(), poolB->GetFairShare());
}

TEST_F(TFairShareTreeTest, TestVectorFairShareOneSmallOperation)
{
    // Create a cluster with 1 large node
    const int nodeCount = 1;
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(100);
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(1000);

    auto host = New<TSchedulerStrategyHostMock>(TJobResourcesWithQuotaList(nodeCount, nodeResources));

    // Create a tree with 2 pools
    auto rootElement = CreateTestRootElement(host.Get());
    auto poolA = CreateTestPool(host.Get(), "PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateTestPool(host.Get(), "PoolB");
    poolB->AttachParent(rootElement.Get());

    // Create operation with demand smaller than the available resources
    const int jobCount = 30;
    TJobResourcesWithQuota jobResources;
    jobResources.SetUserSlots(1);
    jobResources.SetCpu(1);
    jobResources.SetMemory(20);

    auto operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions->Weight = 1.0;

    auto operationX = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(jobCount, jobResources));
    auto operationElementX = CreateTestOperationElement(host.Get(), operationX.Get(), operationOptions);

    operationElementX->AttachParent(poolA.Get(), true);
    operationElementX->Enable();

    // Update tree
    TDynamicAttributesList dynamicAttributes = {};
    TUpdateFairShareContext updateContext;
    rootElement->PreUpdate(&dynamicAttributes, &updateContext);
    rootElement->Update(&dynamicAttributes, &updateContext);

    // Check the values
    EXPECT_EQ(TResourceVector({0.3, 0.3, 0.0, 0.6, 0.0}), rootElement->GetFairShare());
    EXPECT_EQ(TResourceVector({0.3, 0.3, 0.0, 0.6, 0.0}), poolA->GetFairShare());
    EXPECT_EQ(TResourceVector({0.3, 0.3, 0.0, 0.6, 0.0}), operationElementX->GetFairShare());
    EXPECT_EQ(TResourceVector::Zero(), poolB->GetFairShare());
}

TEST_F(TFairShareTreeTest, TestVectorFairShareTwoComplementaryOperations)
{
    // Create a cluster with 1 large node
    const int nodeCount = 1;
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(100);
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(1000);

    auto host = New<TSchedulerStrategyHostMock>(TJobResourcesWithQuotaList(nodeCount, nodeResources));

    // Create a tree with 2 pools
    auto rootElement = CreateTestRootElement(host.Get());
    auto poolA = CreateTestPool(host.Get(), "PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateTestPool(host.Get(), "PoolB");
    poolB->AttachParent(rootElement.Get());

    // Create first operation
    const int jobCount1 = 100;
    TJobResourcesWithQuota jobResources1;
    jobResources1.SetUserSlots(1);
    jobResources1.SetCpu(1);
    jobResources1.SetMemory(20);

    auto operationOptions1 = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions1->Weight = 1.0;

    auto operation1 = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(jobCount1, jobResources1));
    auto operationElement1 = CreateTestOperationElement(host.Get(), operation1.Get(), operationOptions1);

    operationElement1->AttachParent(poolA.Get(), true);
    operationElement1->Enable();

    // Second operation with symmetric resource demand
    const int jobCount2 = 100;
    TJobResourcesWithQuota jobResources2;
    jobResources2.SetUserSlots(1);
    jobResources2.SetCpu(2);
    jobResources2.SetMemory(10);

    auto operationOptions2 = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions2->Weight = 1.0;

    auto operation2 = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(jobCount2, jobResources2));
    auto operationElement2 = CreateTestOperationElement(host.Get(), operation2.Get(), operationOptions2);

    operationElement2->AttachParent(poolA.Get(), true);
    operationElement2->Enable();

    // Update tree
    TDynamicAttributesList dynamicAttributes = {};
    TUpdateFairShareContext updateContext;
    rootElement->PreUpdate(&dynamicAttributes, &updateContext);
    rootElement->Update(&dynamicAttributes, &updateContext);

    // Check the values
    EXPECT_EQ(TResourceVector({2.0 / 3, 1.0, 0.0, 1.0, 0.0}), rootElement->GetFairShare());
    EXPECT_EQ(TResourceVector({2.0 / 3, 1.0, 0.0, 1.0, 0.0}), poolA->GetFairShare());
    EXPECT_EQ(TResourceVector({1.0 / 3, 1.0 / 3, 0.0, 2.0 / 3, 0.0}), operationElement1->GetFairShare());
    EXPECT_EQ(TResourceVector({1.0 / 3, 2.0 / 3, 0.0, 1.0 / 3, 0.0}), operationElement2->GetFairShare());
    EXPECT_EQ(TResourceVector::Zero(), poolB->GetFairShare());
}

TEST_F(TFairShareTreeTest, TestVectorFairShareComplexCase)
{
    // Create a cluster with 1 large node
    const int nodeCount = 1;
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(100);
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(1000);

    auto host = New<TSchedulerStrategyHostMock>(TJobResourcesWithQuotaList(nodeCount, nodeResources));

    // Create a tree with 2 pools
    auto rootElement = CreateTestRootElement(host.Get());
    auto poolA = CreateTestPool(host.Get(), "PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateTestPool(host.Get(), "PoolB");
    poolB->AttachParent(rootElement.Get());

    // Create an operation with resource demand proportion <1, 2> and small jobCount in PoolA
    const int jobCount1 = 10;
    TJobResourcesWithQuota jobResources1;
    jobResources1.SetUserSlots(1);
    jobResources1.SetCpu(1);
    jobResources1.SetMemory(20);

    auto operationOptions1 = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions1->Weight = 1.0;

    auto operation1 = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(jobCount1, jobResources1));
    auto operationElement1 = CreateTestOperationElement(host.Get(), operation1.Get(), operationOptions1);

    operationElement1->AttachParent(poolA.Get(), true);
    operationElement1->Enable();

    // Create an operation with resource demand proportion <3, 1> and large jobCount in PoolA
    const int jobCount2 = 1000;
    TJobResourcesWithQuota jobResources2;
    jobResources2.SetUserSlots(1);
    jobResources2.SetCpu(3);
    jobResources2.SetMemory(10);

    auto operationOptions2 = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions2->Weight = 1.0;

    auto operation2 = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(jobCount2, jobResources2));
    auto operationElement2 = CreateTestOperationElement(host.Get(), operation2.Get(), operationOptions2);

    operationElement2->AttachParent(poolA.Get(), true);
    operationElement2->Enable();

    // Create operation with resource demand proportion <1, 5> and large jobCount in PoolB
    const int jobCount3 = 1000;
    TJobResourcesWithQuota jobResources3;
    jobResources3.SetUserSlots(2);
    jobResources3.SetCpu(2);
    jobResources3.SetMemory(100);

    auto operationOptions3 = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions3->Weight = 1.0;

    auto operation3 = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(jobCount3, jobResources3));
    auto operationElement3 = CreateTestOperationElement(host.Get(), operation3.Get(), operationOptions3);

    operationElement3->AttachParent(poolB.Get(), true);
    operationElement3->Enable();

    // Update tree
    TDynamicAttributesList dynamicAttributes = {};
    TUpdateFairShareContext updateContext;
    rootElement->PreUpdate(&dynamicAttributes, &updateContext);
    rootElement->Update(&dynamicAttributes, &updateContext);

    // Check the values

    // Memory will be saturated first (see the usages of operations bellow)
    EXPECT_THAT(
        rootElement->GetFairShare(),
        ResourceVectorNear(TResourceVector({16.0 / 40, 30.0 / 40, 0.0, 40.0 / 40, 0.0}), 1e-7));
    EXPECT_THAT(
        poolA->GetFairShare(),
        ResourceVectorNear(TResourceVector({11.0 / 40, 25.0 / 40, 0.0, 15.0 / 40, 0.0}), 1e-7));
    EXPECT_THAT(
        poolB->GetFairShare(),
        ResourceVectorNear(TResourceVector({5.0 / 40, 5.0 / 40, 0.0, 25.0 / 40, 0.0}), 1e-7));

    // operation1 uses 4/40 CPU and 8/40 Memory
    EXPECT_THAT(
        operationElement1->GetFairShare(),
        ResourceVectorNear(TResourceVector({4.0 / 40, 4.0 / 40, 0.0, 8.0 / 40, 0.0}), 1e-7));
    // operation2 uses 21/40 CPU and 7/40 Memory
    EXPECT_THAT(
        operationElement2->GetFairShare(),
        ResourceVectorNear(TResourceVector({7.0 / 40, 21.0 / 40, 0.0, 7.0 / 40, 0.0}), 1e-7));
    // operation3 uses 5/40 CPU and 25/40 Memory
    EXPECT_THAT(
        operationElement3->GetFairShare(),
        ResourceVectorNear(TResourceVector({5.0 / 40, 5.0 / 40, 0.0, 25.0 / 40, 0.0}), 1e-7));
}

TEST_F(TFairShareTreeTest, TestVectorFairShareNonContinuousFairShare)
{
    // Create a cluster with 1 large node
    const int nodeCount = 1;
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(100'000);
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(100_GB);
    nodeResources.SetNetwork(100);

    auto host = New<TSchedulerStrategyHostMock>(TJobResourcesWithQuotaList(nodeCount, nodeResources));

    // Create a tree with 2 pools
    auto rootElement = CreateTestRootElement(host.Get());
    auto poolA = CreateTestPool(host.Get(), "PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateTestPool(host.Get(), "PoolB");
    poolB->AttachParent(rootElement.Get());

    // Create an operation with resource demand proportion <1, 1, 4>, weight=10, and small jobCount in PoolA
    const int jobCount1 = 10;
    TJobResourcesWithQuota jobResources1;
    jobResources1.SetUserSlots(1);
    jobResources1.SetCpu(1);
    jobResources1.SetMemory(1_GB);
    jobResources1.SetNetwork(4);

    auto operationOptions1 = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions1->Weight = 10.0;

    auto operation1 = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(jobCount1, jobResources1));
    auto operationElement1 = CreateTestOperationElement(host.Get(), operation1.Get(), operationOptions1);

    operationElement1->AttachParent(poolA.Get(), true);
    operationElement1->Enable();

    // Create an operation with resource demand proportion <1, 1, 0>, weight=1, and large jobCount in PoolA
    const int jobCount2 = 1000;
    TJobResourcesWithQuota jobResources2;
    jobResources2.SetUserSlots(1);
    jobResources2.SetCpu(1);
    jobResources2.SetMemory(1_GB);
    jobResources2.SetNetwork(0);

    auto operationOptions2 = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions2->Weight = 1.0;

    auto operation2 = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(jobCount2, jobResources2));
    auto operationElement2 = CreateTestOperationElement(host.Get(), operation2.Get(), operationOptions2);

    operationElement2->AttachParent(poolA.Get(), true);
    operationElement2->Enable();

    // Update tree
    TDynamicAttributesList dynamicAttributes = {};
    TUpdateFairShareContext updateContext;
    rootElement->PreUpdate(&dynamicAttributes, &updateContext);
    rootElement->Update(&dynamicAttributes, &updateContext);

    // Check the values

    // Memory will be saturated first (see the usages of operations bellow)
    EXPECT_THAT(rootElement->GetFairShare(), ResourceVectorNear(TResourceVector({0.001, 1.0, 0.0, 1.0, 0.4}), 1e-7));
    EXPECT_THAT(poolA->GetFairShare(), ResourceVectorNear(TResourceVector({0.001, 1.0, 0.0, 1.0, 0.4}), 1e-7));
    EXPECT_THAT(poolB->GetFairShare(), ResourceVectorNear(TResourceVector::Zero(), 1e-7));

    // operation1 uses 0.1 CPU, 0.1 Memory, and 0.4 Network
    EXPECT_THAT(operationElement1->GetFairShare(), ResourceVectorNear(TResourceVector({0.0001, 0.1, 0.0, 0.1, 0.4}), 1e-7));
    // operation2 uses 0.9 CPU, 0.9 Memory, and 0 Network
    EXPECT_THAT(operationElement2->GetFairShare(), ResourceVectorNear(TResourceVector({0.0009, 0.9, 0.0, 0.9, 0.0}), 1e-7));
}

TEST_F(TFairShareTreeTest, TestVectorFairShareNonContinuousFairShareFunctionIsLeftContinuous)
{
    // Create a cluster with 1 large node.
    const int nodeCount = 1;
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(100'000);
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(100_GB);
    nodeResources.SetNetwork(100);

    auto host = New<TSchedulerStrategyHostMock>(TJobResourcesWithQuotaList(nodeCount, nodeResources));

    // Create a tree with 2 pools.
    auto rootElement = CreateTestRootElement(host.Get());
    // Use fake root to be able to set a CPU limit.
    auto fakeRootElement = CreateTestPool(host.Get(), "FakeRoot");
    fakeRootElement->AttachParent(rootElement.Get());
    auto poolA = CreateTestPool(host.Get(), "PoolA");
    poolA->AttachParent(fakeRootElement.Get());
    auto poolB = CreateTestPool(host.Get(), "PoolB");
    poolB->AttachParent(fakeRootElement.Get());

    // Set CPU limit for fake root.
    auto rootConfig = fakeRootElement->GetConfig();
    rootConfig->ResourceLimits->Cpu = 40;
    fakeRootElement->SetConfig(rootConfig);

    // Create an operation with resource demand proportion <1, 1, 4>, weight=10, and small jobCount in PoolA.
    const int jobCount1 = 10;
    TJobResourcesWithQuota jobResources1;
    jobResources1.SetUserSlots(1);
    jobResources1.SetCpu(1);
    jobResources1.SetMemory(1_GB);
    jobResources1.SetNetwork(4);

    auto operationOptions1 = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions1->Weight = 10.0;

    auto operation1 = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(jobCount1, jobResources1));
    auto operationElement1 = CreateTestOperationElement(host.Get(), operation1.Get(), operationOptions1);

    operationElement1->AttachParent(poolA.Get(), true);
    operationElement1->Enable();

    // Create an operation with resource demand proportion <1, 1, 0>, weight=1, and large jobCount in PoolA.
    const int jobCount2 = 1000;
    TJobResourcesWithQuota jobResources2;
    jobResources2.SetUserSlots(1);
    jobResources2.SetCpu(1);
    jobResources2.SetMemory(1_GB);
    jobResources2.SetNetwork(0);

    auto operationOptions2 = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions2->Weight = 1.0;

    auto operation2 = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(jobCount2, jobResources2));
    auto operationElement2 = CreateTestOperationElement(host.Get(), operation2.Get(), operationOptions2);

    operationElement2->AttachParent(poolA.Get(), true);
    operationElement2->Enable();

    // Update tree.
    TDynamicAttributesList dynamicAttributes = {};
    TUpdateFairShareContext updateContext;
    rootElement->PreUpdate(&dynamicAttributes, &updateContext);
    rootElement->Update(&dynamicAttributes, &updateContext);

    // Check the values.
    // 0.4 is a discontinuity point of root's FSBS, so the amount of fair share given to poolA equals to
    // the left limit of FSBS at 0.4, even though we have enough resources to allocate the right limit at 0.4.
    // This is a fundamental property of our strategy.
    EXPECT_THAT(rootElement->GetFairShare(), ResourceVectorNear(TResourceVector({0.00014, 0.14, 0.0, 0.14, 0.4}), 1e-7));
    EXPECT_THAT(fakeRootElement->GetFairShare(), ResourceVectorNear(TResourceVector({0.00014, 0.14, 0.0, 0.14, 0.4}), 1e-7));
    EXPECT_THAT(poolA->GetFairShare(), ResourceVectorNear(TResourceVector({0.00014, 0.14, 0.0, 0.14, 0.4}), 1e-7));
    EXPECT_THAT(poolB->GetFairShare(), ResourceVectorNear(TResourceVector::Zero(), 1e-7));

    // Operation 1 uses 0.1 CPU, 0.1 Memory, and 0.4 Network.
    EXPECT_THAT(operationElement1->GetFairShare(), ResourceVectorNear(TResourceVector({0.0001, 0.1, 0.0, 0.1, 0.4}), 1e-7));
    // Operation 2 uses 0.04 CPU, 0.04 Memory, and 0.0 Network.
    EXPECT_THAT(operationElement2->GetFairShare(), ResourceVectorNear(TResourceVector({0.00004, 0.04, 0.0, 0.04, 0.0}), 1e-7));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TFairShareTreeTest, DoNotPreemptJobsIfFairShareRatioEqualToDemandRatio)
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

    auto operationElement = CreateTestOperationElement(host.Get(), operation.Get(), operationOptions);

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

    EXPECT_EQ(TResourceVector({0.0, 0.4, 0.0, 0.4, 0.0}), operationElement->Attributes().DemandShare);
    EXPECT_EQ(TResourceVector({0.0, 0.4, 0.0, 0.4, 0.0}), operationElement->Attributes().FairShare.Total);

    for (int i = 0; i < 2; ++i) {
        EXPECT_FALSE(operationElement->IsJobPreemptable(jobIds[i], /* aggressivePreemptionEnabled */ true));
    }
    for (int i = 2; i < 4; ++i) {
        EXPECT_FALSE(operationElement->IsJobPreemptable(jobIds[i], /* aggressivePreemptionEnabled */ false));
        EXPECT_TRUE(operationElement->IsJobPreemptable(jobIds[i], /* aggressivePreemptionEnabled */ true));
    }

    TJobResources delta;
    delta.SetCpu(10);
    delta.SetMemory(10);
    // FairShare is now less than usage and we would start preempting jobs of this operation.
    operationElement->IncreaseJobResourceUsage(jobIds[0], delta);

    for (int i = 0; i < 1; ++i) {
        EXPECT_FALSE(operationElement->IsJobPreemptable(jobIds[i], /* aggressivePreemptionEnabled */ true));
    }
    for (int i = 1; i < 4; ++i) {
        EXPECT_FALSE(operationElement->IsJobPreemptable(jobIds[i], /* aggressivePreemptionEnabled */ false));
        EXPECT_TRUE(operationElement->IsJobPreemptable(jobIds[i], /* aggressivePreemptionEnabled */ true));
    }
}

TEST_F(TFairShareTreeTest, TestRelaxedPoolFairShareSimple)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto relaxedPool = CreateTestPool(host.Get(), "relaxed", CreateRelaxedPoolConfig(
        /* flowCpu */ 10,
        /* minShareCpu */ 10));
    relaxedPool->AttachParent(rootElement.Get());

    auto [operationElement, operationHost] = CreateOperationWithJobs(30, host.Get(), relaxedPool.Get());

    {
        auto dynamicAttributes = TDynamicAttributesList(3);

        TUpdateFairShareContext updateContext;
        updateContext.Now = TInstant::Now();
        updateContext.PreviousUpdateTime = updateContext.Now - TDuration::Minutes(1);
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_RV_NEAR(unit * 3, operationElement->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(unit * 3, operationElement->Attributes().FairShare.Total);

        EXPECT_EQ(unit, relaxedPool->Attributes().FairShare.MinShareGuarantee);
        EXPECT_EQ(unit, relaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit, relaxedPool->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(unit * 3, relaxedPool->Attributes().FairShare.Total);

        EXPECT_RV_NEAR(unit, rootElement->Attributes().FairShare.MinShareGuarantee);
        EXPECT_EQ(unit, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit, rootElement->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(unit * 3, rootElement->Attributes().FairShare.Total);
    }
}

TEST_F(TFairShareTreeTest, TestBurstPoolFairShareSimple)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto burstPool = CreateTestPool(host.Get(), "burst", CreateBurstPoolConfig(
        /* flowCpu */ 10,
        /* burstCpu */ 10,
        /* minShareCpu */ 10));
    burstPool->AttachParent(rootElement.Get());

    auto [operationElement, operationHost] = CreateOperationWithJobs(30, host.Get(), burstPool.Get());

    {
        auto dynamicAttributes = TDynamicAttributesList(3);

        TUpdateFairShareContext updateContext;
        updateContext.Now = TInstant::Now();
        updateContext.PreviousUpdateTime = updateContext.Now - TDuration::Minutes(1);
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_RV_NEAR(unit * 3, operationElement->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(unit * 3, operationElement->Attributes().FairShare.Total);

        EXPECT_EQ(unit, burstPool->Attributes().FairShare.MinShareGuarantee);
        EXPECT_EQ(unit, burstPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit, burstPool->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(unit * 3, burstPool->Attributes().FairShare.Total);

        EXPECT_EQ(unit, rootElement->Attributes().FairShare.MinShareGuarantee);
        EXPECT_EQ(unit, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit, rootElement->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(unit * 3, rootElement->Attributes().FairShare.Total);
    }
}

TEST_F(TFairShareTreeTest, TestAccumulatedVolumeProvidesMore)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto relaxedPool = CreateTestPool(host.Get(), "relaxed", CreateRelaxedPoolConfig(/* flowCpu */ 10));
    relaxedPool->AttachParent(rootElement.Get());

    auto firstUpdateTime = TInstant::Now();
    {
        // Make first update to accumulate volume
        auto dynamicAttributes = TDynamicAttributesList(3);
        TUpdateFairShareContext updateContext;
        updateContext.Now = firstUpdateTime;
        updateContext.PreviousUpdateTime = updateContext.Now - TDuration::Minutes(1);
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);
    }

    auto [operationElement, operationHost] = CreateOperationWithJobs(30, host.Get(), relaxedPool.Get());
    auto secondUpdateTime = TInstant::Now() + TDuration::Minutes(1);
    {
        auto dynamicAttributes = TDynamicAttributesList(3);

        TUpdateFairShareContext updateContext;
        updateContext.Now = secondUpdateTime;
        updateContext.PreviousUpdateTime = firstUpdateTime;
        ResetFairShareFunctionsRecursively(rootElement.Get());
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_RV_NEAR(unit * 3, operationElement->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(unit * 3, operationElement->Attributes().FairShare.Total);

        EXPECT_EQ(TResourceVector::Zero(), relaxedPool->Attributes().FairShare.MinShareGuarantee);
        EXPECT_RV_NEAR(unit * 2, relaxedPool->Attributes().FairShare.IntegralGuarantee);  // Here we get two times more share ratio than guaranteed by flow.
        EXPECT_RV_NEAR(unit, relaxedPool->Attributes().FairShare.WeightProportional);
    }
}

TEST_F(TFairShareTreeTest, TestMinSharePoolAndBurstPool)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto burstPool = CreateTestPool(host.Get(), "burst", CreateBurstPoolConfig(
        /* flowCpu */ 100,
        /* burstCpu */ 50));
    burstPool->AttachParent(rootElement.Get());

    auto minSharePool = CreateTestPool(host.Get(), "minShare", CreateSimplePoolConfig(/* minShareCpu */ 50));
    minSharePool->AttachParent(rootElement.Get());

    auto [burstOperationElement, burstOperationHost] = CreateOperationWithJobs(100, host.Get(), burstPool.Get());
    auto [minShareOperationElement, minShareOperationHost] = CreateOperationWithJobs(100, host.Get(), minSharePool.Get());

    {
        auto dynamicAttributes = TDynamicAttributesList(3);

        TUpdateFairShareContext updateContext;
        updateContext.Now = TInstant::Now();
        updateContext.PreviousUpdateTime = updateContext.Now - TDuration::Minutes(1);
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 5, minSharePool->Attributes().FairShare.MinShareGuarantee);
        EXPECT_EQ(unit * 0, minSharePool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, minSharePool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 0, burstPool->Attributes().FairShare.MinShareGuarantee);
        EXPECT_EQ(unit * 5, burstPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, burstPool->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(unit * 5, rootElement->Attributes().FairShare.MinShareGuarantee);
        EXPECT_EQ(unit * 5, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_EQ(unit * 0, rootElement->Attributes().FairShare.WeightProportional);
    }
}

TEST_F(TFairShareTreeTest, TestMinSharePoolAndRelaxedPool)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto minSharePool = CreateTestPool(host.Get(), "minShare", CreateSimplePoolConfig(/* minShareCpu */ 50));
    minSharePool->AttachParent(rootElement.Get());

    auto relaxedPool = CreateTestPool(host.Get(), "relaxed", CreateRelaxedPoolConfig(/* flowCpu */ 100, /* minShareCpu */ 0));
    relaxedPool->AttachParent(rootElement.Get());

    auto [minShareOperationElement, minShareOperationHost] = CreateOperationWithJobs(100, host.Get(), minSharePool.Get());
    auto [relaxedOperationElement, relaxedOperationHost] = CreateOperationWithJobs(100, host.Get(), relaxedPool.Get());

    {
        auto dynamicAttributes = TDynamicAttributesList(3);

        TUpdateFairShareContext updateContext;
        updateContext.Now = TInstant::Now();
        updateContext.PreviousUpdateTime = updateContext.Now - TDuration::Minutes(1);
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 5, minSharePool->Attributes().FairShare.MinShareGuarantee);
        EXPECT_EQ(unit * 0, minSharePool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, minSharePool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 0, relaxedPool->Attributes().FairShare.MinShareGuarantee);
        EXPECT_RV_NEAR(unit * 5, relaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, relaxedPool->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(unit * 5, rootElement->Attributes().FairShare.MinShareGuarantee);
        EXPECT_RV_NEAR(unit * 5, relaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_EQ(unit * 0, rootElement->Attributes().FairShare.WeightProportional);
    }
}

TEST_F(TFairShareTreeTest, TestBurstGetsAll_RelaxedNone)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto burstPool = CreateTestPool(host.Get(), "burst", CreateBurstPoolConfig(
        /* flowCpu */ 100,
        /* burstCpu */ 100));
    burstPool->AttachParent(rootElement.Get());

    auto relaxedPool = CreateTestPool(host.Get(), "relaxed", CreateRelaxedPoolConfig(/* flowCpu */ 100));
    relaxedPool->AttachParent(rootElement.Get());

    auto [burstOperationElement, burstOperationHost] = CreateOperationWithJobs(100, host.Get(), burstPool.Get());
    auto [relaxedOperationElement, relaxedOperationHost] = CreateOperationWithJobs(100, host.Get(), relaxedPool.Get());

    {
        auto dynamicAttributes = TDynamicAttributesList(3);

        TUpdateFairShareContext updateContext;
        updateContext.Now = TInstant::Now();
        updateContext.PreviousUpdateTime = updateContext.Now - TDuration::Minutes(1);
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 0, burstPool->Attributes().FairShare.MinShareGuarantee);
        EXPECT_EQ(unit * 10, burstPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, burstPool->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(unit * 0, relaxedPool->Attributes().FairShare.Total);

        EXPECT_RV_NEAR(unit * 0, rootElement->Attributes().FairShare.MinShareGuarantee);
        EXPECT_EQ(unit * 10, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_EQ(unit * 0, rootElement->Attributes().FairShare.WeightProportional);
    }
}

TEST_F(TFairShareTreeTest, TestBurstGetsBurstGuaranteeOnly_RelaxedGetsRemaining)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto burstPool = CreateTestPool(host.Get(), "burst", CreateBurstPoolConfig(
        /* flowCpu */ 100,
        /* burstCpu */ 50));
    burstPool->AttachParent(rootElement.Get());

    auto relaxedPool = CreateTestPool(host.Get(), "relaxed", CreateRelaxedPoolConfig(/* flowCpu */ 100));
    relaxedPool->AttachParent(rootElement.Get());

    auto [burstOperationElement, burstOperationHost] = CreateOperationWithJobs(100, host.Get(), burstPool.Get());
    auto [relaxedOperationElement, relaxedOperationHost] = CreateOperationWithJobs(100, host.Get(), relaxedPool.Get());

    {
        auto dynamicAttributes = TDynamicAttributesList(3);

        TUpdateFairShareContext updateContext;
        updateContext.Now = TInstant::Now();
        updateContext.PreviousUpdateTime = updateContext.Now - TDuration::Minutes(1);
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 0, burstPool->Attributes().FairShare.MinShareGuarantee);
        EXPECT_EQ(unit * 5, burstPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, burstPool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 0, relaxedPool->Attributes().FairShare.MinShareGuarantee);
        EXPECT_RV_NEAR(unit * 5, relaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, relaxedPool->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(unit * 0, rootElement->Attributes().FairShare.MinShareGuarantee);
        EXPECT_RV_NEAR(unit * 10, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_EQ(unit * 0, rootElement->Attributes().FairShare.WeightProportional);
    }
}

// TODO(renadeen): Enable when weight proportional will be independent of min share (YT-13578).
TEST_F(TFairShareTreeTest, DISABLED_TestMinShareAndBurstAndRelaxedPoolsShareWeightProportionalComponent)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto minSharePool = CreateTestPool(host.Get(), "minShare", CreateSimplePoolConfig(/* minShareCpu */ 10, /* weight */ 1));
    minSharePool->AttachParent(rootElement.Get());

    auto burstPool = CreateTestPool(host.Get(), "burst", CreateBurstPoolConfig(
        /* flowCpu */ 100,
        /* burstCpu */ 10,
        /* minShareCpu */ 0,
        /* weight */ 2));
    burstPool->AttachParent(rootElement.Get());

    auto relaxedPool = CreateTestPool(host.Get(), "relaxed", CreateRelaxedPoolConfig(
        /* flowCpu */ 10,
        /* minShareCpu */ 0,
        /* weight */ 4));
    relaxedPool->AttachParent(rootElement.Get());

    auto [minShareOperationElement, minShareOperationHost] = CreateOperationWithJobs(100, host.Get(), minSharePool.Get());
    auto [burstOperationElement, burstOperationHost] = CreateOperationWithJobs(100, host.Get(), burstPool.Get());
    auto [relaxedOperationElement, relaxedOperationHost] = CreateOperationWithJobs(100, host.Get(), relaxedPool.Get());

    {
        auto dynamicAttributes = TDynamicAttributesList(10);

        TUpdateFairShareContext updateContext;
        updateContext.Now = TInstant::Now();
        updateContext.PreviousUpdateTime = updateContext.Now - TDuration::Minutes(1);
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 1, minSharePool->Attributes().FairShare.MinShareGuarantee);
        EXPECT_EQ(unit * 0, minSharePool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 1, minSharePool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 0, burstPool->Attributes().FairShare.MinShareGuarantee);
        EXPECT_EQ(unit * 1, burstPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 2, burstPool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 0, relaxedPool->Attributes().FairShare.MinShareGuarantee);
        EXPECT_EQ(unit * 1, relaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 4, relaxedPool->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(unit * 8, rootElement->Attributes().FairShare.MinShareGuarantee);
        EXPECT_EQ(unit * 2, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_EQ(unit * 0, rootElement->Attributes().FairShare.WeightProportional);
    }
}

TEST_F(TFairShareTreeTest, TestTwoRelaxedPoolsGetShareRatioProportionalToVolume)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto relaxedPool1 = CreateTestPool(host.Get(), "relaxed1", CreateRelaxedPoolConfig(/* flowCpu */ 100));
    relaxedPool1->AttachParent(rootElement.Get());

    auto relaxedPool2 = CreateTestPool(host.Get(), "relaxed2", CreateRelaxedPoolConfig(/* flowCpu */ 100));
    relaxedPool2->AttachParent(rootElement.Get());

    auto [relaxedOperationElement1, relaxedOperationHost1] = CreateOperationWithJobs(100, host.Get(), relaxedPool1.Get());
    auto [relaxedOperationElement2, relaxedOperationHost2] = CreateOperationWithJobs(100, host.Get(), relaxedPool2.Get());

    TJobResources oneTenthOfCluster;
    oneTenthOfCluster.SetCpu(10);
    oneTenthOfCluster.SetUserSlots(10);
    oneTenthOfCluster.SetMemory(100_MB);

    auto volume1 = oneTenthOfCluster * TDuration::Minutes(1).SecondsFloat();  // 10% of cluster for 1 minute
    auto volume2 = oneTenthOfCluster * TDuration::Minutes(1).SecondsFloat() * 3.0;  // 30% of cluster for 1 minute
    relaxedPool1->InitAccumulatedResourceVolume(volume1);
    relaxedPool2->InitAccumulatedResourceVolume(volume2);
    {
        auto dynamicAttributes = TDynamicAttributesList(3);

        TUpdateFairShareContext updateContext;
        updateContext.Now = TInstant::Now();
        updateContext.PreviousUpdateTime = std::nullopt;  // It disables refill stage.
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 0, relaxedPool1->Attributes().FairShare.MinShareGuarantee);
        EXPECT_RV_NEAR(unit * 1, relaxedPool1->Attributes().FairShare.IntegralGuarantee);
        // TODO(renadeen): Uncomment when weight proportional will be independent of min share (YT-13578).
//        EXPECT_RV_NEAR(unit * 3, relaxedPool1->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 0, relaxedPool2->Attributes().FairShare.MinShareGuarantee);
        EXPECT_RV_NEAR(unit * 3, relaxedPool2->Attributes().FairShare.IntegralGuarantee);
        // TODO(renadeen): Uncomment when weight proportional will be independent of min share (YT-13578).
//        EXPECT_RV_NEAR(unit * 3, relaxedPool2->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(unit * 0, rootElement->Attributes().FairShare.MinShareGuarantee);
        EXPECT_RV_NEAR(unit * 4, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 6, rootElement->Attributes().FairShare.WeightProportional);
    }
}

TEST_F(TFairShareTreeTest, TestMinShareAdjustmentToTotalResources)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto minSharePool1 = CreateTestPool(host.Get(), "minShare1", CreateSimplePoolConfig(/* minShareCpu */ 30));
    minSharePool1->AttachParent(rootElement.Get());

    auto minSharePool2 = CreateTestPool(host.Get(), "minShare2", CreateSimplePoolConfig(/* minShareCpu */ 90));
    minSharePool2->AttachParent(rootElement.Get());

    auto [operationElement1, operationHost1] = CreateOperationWithJobs(100, host.Get(), minSharePool1.Get());
    auto [operationElement2, operationHost2] = CreateOperationWithJobs(100, host.Get(), minSharePool2.Get());

    {
        auto dynamicAttributes = TDynamicAttributesList(3);

        TUpdateFairShareContext updateContext;
        updateContext.Now = TInstant::Now();
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 2.5, minSharePool1->Attributes().FairShare.MinShareGuarantee);
        EXPECT_RV_NEAR(unit * 0, minSharePool1->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, minSharePool1->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 7.5, minSharePool2->Attributes().FairShare.MinShareGuarantee);
        EXPECT_EQ(unit * 0, minSharePool2->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, minSharePool2->Attributes().FairShare.WeightProportional);
    }
}

TEST_F(TFairShareTreeTest, TestMinSharePlusBurstGuaranteeAdjustmentToTotalResources)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto minSharePool = CreateTestPool(host.Get(), "minShare", CreateSimplePoolConfig(/* minShareCpu */ 90));
    minSharePool->AttachParent(rootElement.Get());

    auto burstPool = CreateTestPool(host.Get(), "burst", CreateBurstPoolConfig(
        /* flowCpu */ 60,
        /* burstCpu */ 60));
    burstPool->AttachParent(rootElement.Get());

    auto [operationElement1, operationHost1] = CreateOperationWithJobs(100, host.Get(), minSharePool.Get());
    auto [operationElement2, operationHost2] = CreateOperationWithJobs(100, host.Get(), burstPool.Get());

    {
        auto dynamicAttributes = TDynamicAttributesList(3);

        TUpdateFairShareContext updateContext;
        updateContext.Now = TInstant::Now();
        updateContext.PreviousUpdateTime = updateContext.Now - TDuration::Minutes(1);
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_RV_NEAR(unit * 6, minSharePool->Attributes().FairShare.MinShareGuarantee);
        EXPECT_RV_NEAR(unit * 0, minSharePool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, minSharePool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 0, burstPool->Attributes().FairShare.MinShareGuarantee);
        EXPECT_RV_NEAR(unit * 4, burstPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, burstPool->Attributes().FairShare.WeightProportional);
    }
}

TEST_F(TFairShareTreeTest, TestLimitsLowerThanMinShare)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto parentConfig = CreateSimplePoolConfig(/* minShareCpu */ 100);
    parentConfig->ResourceLimits->Cpu = 50;
    auto minSharePoolParent = CreateTestPool(host.Get(), "minShareParent", parentConfig);
    minSharePoolParent->AttachParent(rootElement.Get());

    auto minSharePoolChild = CreateTestPool(host.Get(), "minShareChild", CreateSimplePoolConfig(/* minShareCpu */ 100));
    minSharePoolChild->AttachParent(minSharePoolParent.Get());

    auto [opElement, opHost] = CreateOperationWithJobs(100, host.Get(), minSharePoolChild.Get());

    {
        auto dynamicAttributes = TDynamicAttributesList(3);

        TUpdateFairShareContext updateContext;
        updateContext.Now = TInstant::Now();
        updateContext.PreviousUpdateTime = updateContext.Now - TDuration::Minutes(1);
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 5, minSharePoolParent->Attributes().FairShare.MinShareGuarantee);
        EXPECT_RV_NEAR(unit * 5, minSharePoolParent->Attributes().FairShare.Total);

        EXPECT_EQ(unit * 5, minSharePoolChild->Attributes().FairShare.MinShareGuarantee);
        EXPECT_RV_NEAR(unit * 5, minSharePoolChild->Attributes().FairShare.Total);
    }
}

TEST_F(TFairShareTreeTest, TestLimitsLowerThanChildBurstShare)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto parentConfig = CreateSimplePoolConfig(/* minShareCpu */ 0);
    parentConfig->ResourceLimits->Cpu = 50;
    auto limitedParent = CreateTestPool(host.Get(), "limitedParent", parentConfig);
    limitedParent->AttachParent(rootElement.Get());

    auto burstChild = CreateTestPool(host.Get(), "burst", CreateBurstPoolConfig(
        /* flowCpu */ 100,
        /* burstCpu */ 100,
        /* minShareCpu */ 0));
    burstChild->AttachParent(limitedParent.Get());

    auto [opElement, opHost] = CreateOperationWithJobs(100, host.Get(), burstChild.Get());

    {
        auto dynamicAttributes = TDynamicAttributesList(3);

        TUpdateFairShareContext updateContext;
        updateContext.Now = TInstant::Now();
        updateContext.PreviousUpdateTime = updateContext.Now - TDuration::Minutes(1);
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 5, burstChild->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 5, burstChild->Attributes().FairShare.Total);
    }
}
TEST_F(TFairShareTreeTest, TestLimitsLowerThanChildBurstShare2)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto parentConfig = CreateSimplePoolConfig(/* minShareCpu */ 50);
    parentConfig->ResourceLimits->Cpu = 50;
    auto limitedParent = CreateTestPool(host.Get(), "limitedParent", parentConfig);
    limitedParent->AttachParent(rootElement.Get());

    auto burstChild = CreateTestPool(host.Get(), "burst", CreateBurstPoolConfig(
        /* flowCpu */ 10,
        /* burstCpu */ 10,
        /* minShareCpu */ 0));
    burstChild->AttachParent(limitedParent.Get());

    auto [opElement, opHost] = CreateOperationWithJobs(100, host.Get(), burstChild.Get());

    {
        auto dynamicAttributes = TDynamicAttributesList(3);

        TUpdateFairShareContext updateContext;
        updateContext.Now = TInstant::Now();
        updateContext.PreviousUpdateTime = updateContext.Now - TDuration::Minutes(1);
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
        rootElement->Update(&dynamicAttributes, &updateContext);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 0, burstChild->Attributes().FairShare.MinShareGuarantee);
        EXPECT_EQ(unit * 0, burstChild->Attributes().FairShare.IntegralGuarantee);  // Integral share wasn't given due to violation of parent limits.
        EXPECT_RV_NEAR(unit * 5, burstChild->Attributes().FairShare.WeightProportional);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NScheduler::NVectorScheduler
