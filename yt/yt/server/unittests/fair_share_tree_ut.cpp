#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/scheduler/fair_share_tree_element.h>
#include <yt/yt/server/scheduler/operation_controller.h>
#include <yt/yt/server/scheduler/resource_tree.h>

#include <yt/yt/ytlib/chunk_client/proto/medium_directory.pb.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/profiling/profile_manager.h>

#include <yt/yt/core/yson/null_consumer.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NScheduler::NVectorScheduler {

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

class TSchedulerStrategyHostMock
    : public TRefCounted
    , public ISchedulerStrategyHost
    , public TEventLogHostBase
{
public:
    explicit TSchedulerStrategyHostMock(std::vector<TExecNodePtr> execNodes)
        : NodeShardInvokers_({GetCurrentInvoker()})
        , ExecNodes_(std::move(execNodes))
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
        : TSchedulerStrategyHostMock(std::vector<TExecNodePtr>())
    { }

    IInvokerPtr GetControlInvoker(EControlQueue /*queue*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    IInvokerPtr GetFairShareLoggingInvoker() const override
    {
        YT_UNIMPLEMENTED();
    }

    IInvokerPtr GetFairShareProfilingInvoker() const override
    {
        YT_UNIMPLEMENTED();
    }

    IInvokerPtr GetFairShareUpdateInvoker() const override
    {
        return GetCurrentInvoker();
    }

    IInvokerPtr GetOrchidWorkerInvoker() const override
    {
        return GetCurrentInvoker();
    }

    const std::vector<IInvokerPtr>& GetNodeShardInvokers() const override
    {
        return NodeShardInvokers_;
    }

    NEventLog::TFluentLogEvent LogFairShareEventFluently(TInstant /*now*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TJobResources GetResourceLimits(const TSchedulingTagFilter& filter) const override
    {
        TJobResources result;
        for (const auto& execNode : ExecNodes_) {
            if (execNode->CanSchedule(filter)) {
                result += execNode->GetResourceLimits();
            }
        }
        return result;
    }

    TJobResources GetResourceUsage(const TSchedulingTagFilter& /*filter*/) const override
    {
        YT_ABORT();
    }

    void Disconnect(const TError& /*error*/) override
    {
        YT_ABORT();
    }

    TInstant GetConnectionTime() const override
    {
        return TInstant();
    }

    void MarkOperationAsRunningInStrategy(TOperationId /*operationId*/) override
    { }

    void AbortOperation(TOperationId /*operationId*/, const TError& /*error*/) override
    { }

    void FlushOperationNode(TOperationId /*operationId*/) override
    { }

    TMemoryDistribution GetExecNodeMemoryDistribution(const TSchedulingTagFilter& filter) const override
    {
        TMemoryDistribution result;
        for (const auto& execNode : ExecNodes_) 
            if (execNode->CanSchedule(filter)) {
                ++result[execNode->GetResourceLimits().GetMemory()];
            }
        return result;
    }

    TRefCountedExecNodeDescriptorMapPtr CalculateExecNodeDescriptors(
        const TSchedulingTagFilter& /*filter*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    void UpdateNodesOnChangedTrees(
        const THashMap<TString, NScheduler::TSchedulingTagFilter>& /*treeIdToFilter*/) override
    {
        YT_UNIMPLEMENTED();
    }

    std::vector<NNodeTrackerClient::TNodeId> GetExecNodeIds(
        const TSchedulingTagFilter& /*filter*/) const override
    {
        return {};
    }

    TString GetExecNodeAddress(NNodeTrackerClient::TNodeId /*nodeId*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    void ValidatePoolPermission(
        const NYPath::TYPath& /*path*/,
        const TString& /*user*/,
        NYTree::EPermission /*permission*/) const override
    { }

    void SetSchedulerAlert(ESchedulerAlertType /*alertType*/, const TError& /*alert*/) override
    { }

    TFuture<void> SetOperationAlert(
        TOperationId /*operationId*/,
        EOperationAlertType /*alertType*/,
        const TError& /*alert*/,
        std::optional<TDuration> /*timeout*/) override
    {
        return VoidFuture;
    }

    NYson::IYsonConsumer* GetEventLogConsumer() override
    {
        return NYson::GetNullYsonConsumer();
    }

    const NLogging::TLogger* GetEventLogger() override
    {
        return nullptr;
    }

    TString FormatResources(const TJobResourcesWithQuota& resources) const override
    {
        YT_VERIFY(MediumDirectory_);
        return NScheduler::FormatResources(resources);
    }

    TString FormatResourceUsage(
        const TJobResources& usage,
        const TJobResources& limits,
        const NNodeTrackerClient::NProto::TDiskResources& diskResources) const override
    {
        YT_VERIFY(MediumDirectory_);
        return NScheduler::FormatResourceUsage(usage, limits, diskResources, MediumDirectory_);
    }

    void SerializeResources(const TJobResourcesWithQuota& /*resources*/, NYson::IYsonConsumer* /*consumer*/) const override
    {
        YT_ABORT();
    }

    void LogResourceMetering(
        const TMeteringKey& /*key*/,
        const TMeteringStatistics& /*statistics*/,
        const THashMap<TString, TString>& /*otherTags*/,
        TInstant /*lastUpdateTime*/,
        TInstant /*now*/) override
    { }

    int GetDefaultAbcId() const override
    {
        return -1;
    }

    const NChunkClient::TMediumDirectoryPtr& GetMediumDirectory() const
    {
        return MediumDirectory_;
    }

    void InvokeStoringStrategyState(TPersistentStrategyStatePtr /* persistentStrategyState */) override
    { }

    const THashMap<TString, TString>& GetUserDefaultParentPoolMap() const override
    {
        static THashMap<TString, TString> stub;
        return stub;
    }

private:
    std::vector<IInvokerPtr> NodeShardInvokers_;
    std::vector<TExecNodePtr> ExecNodes_;
    NChunkClient::TMediumDirectoryPtr MediumDirectory_;
};

DEFINE_REFCOUNTED_TYPE(TSchedulerStrategyHostMock)


class TOperationControllerStrategyHostMock
    : public IOperationControllerStrategyHost
{
public:
    explicit TOperationControllerStrategyHostMock(TJobResourcesWithQuotaList jobResourcesList)
        : JobResourcesList(std::move(jobResourcesList))
    { }

    MOCK_METHOD(TFuture<TControllerScheduleJobResultPtr>, ScheduleJob, (
        const ISchedulingContextPtr& context,
        const TJobResources& jobLimits,
        const TString& treeId,
        const TFairShareStrategyTreeConfigPtr& treeConfig), (override));

    MOCK_METHOD(void, OnNonscheduledJobAborted, (TJobId, EAbortReason), (override));

    TJobResources GetNeededResources() const override
    {
        TJobResources totalResources;
        for (const auto& resources : JobResourcesList) {
            totalResources += resources.ToJobResources();
        }
        return totalResources;
    }

    void UpdateMinNeededJobResources() override
    { }

    TJobResourcesWithQuotaList GetMinNeededJobResources() const override
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

    int GetPendingJobCount() const override
    {
        return JobResourcesList.size();
    }

    EPreemptionMode PreemptionMode = EPreemptionMode::Normal;

    EPreemptionMode GetPreemptionMode() const override
    {
        return PreemptionMode;
    }

private:
    TJobResourcesWithQuotaList JobResourcesList;
};

DECLARE_REFCOUNTED_TYPE(TOperationControllerStrategyHostMock)
DEFINE_REFCOUNTED_TYPE(TOperationControllerStrategyHostMock)

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
        YT_ABORT();
    }

    EOperationState GetState() const override
    {
        YT_ABORT();
    }


    std::optional<EUnschedulableReason> CheckUnschedulable() const override
    {
        return std::nullopt;
    }

    TInstant GetStartTime() const override
    {
        return StartTime_;
    }

    std::optional<int> FindSlotIndex(const TString& /* treeId */) const override
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
        YT_ABORT();
    }

    TStrategyOperationSpecPtr GetStrategySpecForTree(const TString& /*treeId*/) const override
    {
        YT_ABORT();
    }

    const NYson::TYsonString& GetSpecString() const override
    {
        YT_ABORT();
    }

    TOperationRuntimeParametersPtr GetRuntimeParameters() const override
    {
        YT_ABORT();
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

    std::optional<TJobResources> GetInitialAggregatedMinNeededResources() const override
    {
        return std::nullopt;
    }

private:
    TInstant StartTime_;
    TOperationId Id_;
    TOperationControllerStrategyHostMockPtr Controller_;
};

DECLARE_REFCOUNTED_TYPE(TOperationStrategyHostMock)
DEFINE_REFCOUNTED_TYPE(TOperationStrategyHostMock)

class TFairShareTreeHostMock
    : public IFairShareTreeHost
{
public:
    explicit TFairShareTreeHostMock(const TFairShareStrategyTreeConfigPtr& treeConfig)
        : ResourceTree_(New<TResourceTree>(treeConfig, std::vector<IInvokerPtr>({GetCurrentInvoker()})))
    { }

    TResourceTree* GetResourceTree() override
    {
        return ResourceTree_.Get();
    }

private:
    TResourceTreePtr ResourceTree_;
};

class TFairShareTreeTest
    : public testing::Test
{
public:
    TFairShareTreeTest()
    {
        TreeConfig_->AggressivePreemptionSatisfactionThreshold = 0.5;
        TreeConfig_->MinChildHeapSize = 3;
        TreeConfig_->EnableConditionalPreemption = true;
        TreeConfig_->UseResourceUsageWithPrecommit = false;
        TreeConfig_->ShouldDistributeFreeVolumeAmongChildren = true;
    }

protected:
    TSchedulerConfigPtr SchedulerConfig_ = New<TSchedulerConfig>();
    TFairShareStrategyTreeConfigPtr TreeConfig_ = New<TFairShareStrategyTreeConfig>();
    TIntrusivePtr<TFairShareTreeHostMock> FairShareTreeHostMock_ = New<TFairShareTreeHostMock>(TreeConfig_);
    TScheduleJobsStage SchedulingStageMock_ = TScheduleJobsStage(
        /* nameInLogs */ "Test scheduling stage",
        TScheduleJobsProfilingCounters(NProfiling::TProfiler{"/test_scheduling_stage"}));

    int SlotIndex_ = 0;
    NNodeTrackerClient::TNodeId ExecNodeId_ = 0;

    TDiskQuota CreateDiskQuota(i64 diskSpace)
    {
        TDiskQuota diskQuota;
        diskQuota.DiskSpacePerMedium[NChunkClient::DefaultSlotsMediumIndex] = diskSpace;
        return diskQuota;
    }

    TSchedulerRootElementPtr CreateTestRootElement(ISchedulerStrategyHost* host)
    {
        return New<TSchedulerRootElement>(
            host,
            FairShareTreeHostMock_.Get(),
            TreeConfig_,
            "default",
            SchedulerLogger);
    }

    TSchedulerPoolElementPtr CreateTestPool(ISchedulerStrategyHost* host, const TString& name, TPoolConfigPtr config = New<TPoolConfig>())
    {
        return New<TSchedulerPoolElement>(
            host,
            FairShareTreeHostMock_.Get(),
            name,
            std::move(config),
            /* defaultConfigured */ true,
            TreeConfig_,
            "default",
            SchedulerLogger);
    }

    TPoolConfigPtr CreateSimplePoolConfig(double strongGuaranteeCpu = 0.0, double weight = 1.0)
    {
        auto relaxedPoolConfig = New<TPoolConfig>();
        relaxedPoolConfig->StrongGuaranteeResources->Cpu = strongGuaranteeCpu;
        relaxedPoolConfig->Weight = weight;
        return relaxedPoolConfig;
    }

    TPoolConfigPtr CreateIntegralPoolConfig(EIntegralGuaranteeType type, double flowCpu, double burstCpu, double strongGuaranteeCpu = 0.0, double weight = 1.0)
    {
        auto integralPoolConfig = CreateSimplePoolConfig(strongGuaranteeCpu, weight);
        integralPoolConfig->IntegralGuarantees->GuaranteeType = type;
        integralPoolConfig->IntegralGuarantees->ResourceFlow->Cpu = flowCpu;
        integralPoolConfig->IntegralGuarantees->BurstGuaranteeResources->Cpu = burstCpu;
        return integralPoolConfig;
    }

    TPoolConfigPtr CreateBurstPoolConfig(double flowCpu, double burstCpu, double strongGuaranteeCpu = 0.0, double weight = 1.0)
    {
        return CreateIntegralPoolConfig(EIntegralGuaranteeType::Burst, flowCpu, burstCpu, strongGuaranteeCpu, weight);
    }

    TPoolConfigPtr CreateRelaxedPoolConfig(double flowCpu, double strongGuaranteeCpu = 0.0, double weight = 1.0)
    {
        return CreateIntegralPoolConfig(EIntegralGuaranteeType::Relaxed, flowCpu, 0.0, strongGuaranteeCpu, weight);
    }

    TSchedulerOperationElementPtr CreateTestOperationElement(
        ISchedulerStrategyHost* host,
        IOperationStrategyHost* operation,
        TSchedulerCompositeElement* parent,
        TOperationFairShareTreeRuntimeParametersPtr operationOptions = nullptr,
        TStrategyOperationSpecPtr operationSpec = nullptr)
    {
        auto operationController = New<TFairShareStrategyOperationController>(operation, SchedulerConfig_, host->GetNodeShardInvokers().size());
        if (!operationOptions) {
            operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
            operationOptions->Weight = 1.0;
        }
        if (!operationSpec) {
            operationSpec = New<TStrategyOperationSpec>();
        }
        auto operationElement = New<TSchedulerOperationElement>(
            TreeConfig_,
            operationSpec,
            operationOptions,
            operationController,
            SchedulerConfig_,
            host,
            FairShareTreeHostMock_.Get(),
            operation,
            "default",
            SchedulerLogger);
        operationElement->AttachParent(parent, SlotIndex_++);
        parent->EnableChild(operationElement);
        operationElement->Enable();
        return operationElement;
    }

    std::pair<TSchedulerOperationElementPtr, TIntrusivePtr<TOperationStrategyHostMock>> CreateOperationWithJobs(
        int jobCount,
        ISchedulerStrategyHost* host,
        TSchedulerCompositeElement* parent)
    {
        TJobResourcesWithQuota jobResources;
        jobResources.SetUserSlots(1);
        jobResources.SetCpu(1);
        jobResources.SetMemory(10_MB);

        auto operationHost = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(jobCount, jobResources));
        auto operationElement = CreateTestOperationElement(host, operationHost.Get(), parent);
        return {operationElement, operationHost};
    }

    TExecNodePtr CreateTestExecNode(const TJobResourcesWithQuota& nodeResources, TBooleanFormulaTags tags = {})
    {
        NNodeTrackerClient::NProto::TDiskResources diskResources;
        diskResources.mutable_disk_location_resources()->Add();
        diskResources.mutable_disk_location_resources(0)->set_limit(nodeResources.GetDiskQuota().DiskSpacePerMedium[NChunkClient::DefaultSlotsMediumIndex]);

        auto execNode = New<TExecNode>(ExecNodeId_++, NNodeTrackerClient::TNodeDescriptor(), ENodeState::Online);
        execNode->SetResourceLimits(nodeResources.ToJobResources());
        execNode->SetDiskResources(diskResources);

        execNode->Tags() = std::move(tags);

        return execNode;
    }

    std::vector<TExecNodePtr> CreateTestExecNodeList(int count, const TJobResourcesWithQuota& nodeResources)
    {
        std::vector<TExecNodePtr> execNodes;
        for (int i = 0; i < count; i++) {
            execNodes.push_back(CreateTestExecNode(nodeResources));
        }
        return execNodes;
    }

    TIntrusivePtr<TSchedulerStrategyHostMock> CreateHostWith10NodesAnd10Cpu()
    {
        TJobResourcesWithQuota nodeResources;
        nodeResources.SetUserSlots(10);
        nodeResources.SetCpu(10);
        nodeResources.SetMemory(100_MB);

        return New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(10, nodeResources));
    }

    TJobPtr CreateTestJob(
        TJobId jobId,
        TOperationId operationId,
        const TExecNodePtr& execNode,
        TInstant startTime,
        TJobResources jobResources)
    {
        return New<TJob>(
            jobId,
            EJobType::Vanilla,
            operationId,
            /*incarnationId*/ TGuid::Create(),
            /*controllerEpoch*/ 0,
            execNode,
            startTime,
            jobResources,
            /*interruptible*/ false,
            /*preemptionMode*/ EPreemptionMode::Normal,
            /*treeId*/ "");
    }

    void DoTestSchedule(
        const TSchedulerRootElementPtr& rootElement,
        const TSchedulerOperationElementPtr& operationElement,
        const TExecNodePtr& execNode,
        const TSchedulerStrategyHostMock* host)
    {
        auto schedulingContext = CreateSchedulingContext(
            /* nodeShardId */ 0,
            SchedulerConfig_,
            execNode,
            /* runningJobs */ {},
            host->GetMediumDirectory());

        DoFairShareUpdate(host, rootElement);

        TScheduleJobsContext context(
            schedulingContext,
            rootElement->GetTreeSize(),
            /* registeredSchedulingTagFilters */ {},
            /* enableSchedulingInfoLogging */ true,
            SchedulerLogger);
        context.StartStage(&SchedulingStageMock_, "stage");

        context.PrepareForScheduling(rootElement);
        rootElement->CalculateCurrentResourceUsage(&context);
        rootElement->PrescheduleJob(&context, EPrescheduleJobOperationCriterion::All);

        operationElement->ScheduleJob(&context, /* ignorePacking */ true);

        context.FinishStage();
    }

    void DoFairShareUpdate(
        const ISchedulerStrategyHost* host,
        const TSchedulerRootElementPtr& rootElement,
        TInstant now = TInstant(),
        std::optional<TInstant> previousUpdateTime = std::nullopt,
        bool checkForStarvation = false)
    {
		NFairShare::TFairShareUpdateContext context(
            /* totalResourceLimits */ host->GetResourceLimits(TreeConfig_->NodesFilter),
            TreeConfig_->MainResource,
            TreeConfig_->IntegralGuarantees->PoolCapacitySaturationPeriod,
            TreeConfig_->IntegralGuarantees->SmoothPeriod,
            now,
            previousUpdateTime);

        TFairSharePostUpdateContext fairSharePostUpdateContext;
        TManageTreeSchedulingSegmentsContext manageSegmentsContext{
            .TreeConfig = TreeConfig_,
            .TotalResourceLimits = context.TotalResourceLimits,
        };

        rootElement->PreUpdate(&context);

		NFairShare::TFairShareUpdateExecutor updateExecutor(rootElement, &context);
		updateExecutor.Run();

		rootElement->PostUpdate(&fairSharePostUpdateContext, &manageSegmentsContext);

		if (checkForStarvation) {
            rootElement->UpdateStarvationAttributes(now, /*enablePoolStarvation*/ true);
        }
    }
};


void ResetFairShareFunctionsRecursively(TSchedulerCompositeElement* compositeElement)
{
    compositeElement->ResetFairShareFunctions();
    for (const auto& child : compositeElement->GetEnabledChildren()) {
        if (auto* childPool = dynamic_cast<TSchedulerCompositeElement*>(child.Get())) {
            ResetFairShareFunctionsRecursively(childPool);
        } else {
            child->ResetFairShareFunctions();
        }
    }
}

TResourceVolume GetHugeVolume()
{
    TResourceVolume hugeVolume;
    hugeVolume.SetCpu(10000000000);
    hugeVolume.SetUserSlots(10000000000);
    hugeVolume.SetMemory(10000000000_MB);
    return hugeVolume;
}

TJobResources OneHundredthOfCluster()
{
    TJobResources oneHundredthOfCluster;
    oneHundredthOfCluster.SetCpu(1);
    oneHundredthOfCluster.SetUserSlots(1);
    oneHundredthOfCluster.SetMemory(10_MB);
    return oneHundredthOfCluster;
}

////////////////////////////////////////////////////////////////////////////////

MATCHER_P2(ResourceVectorNear, vec, absError, "") {
    return TResourceVector::Near(arg, vec, absError);
}

#define EXPECT_RV_NEAR(vector1, vector2) \
    EXPECT_THAT(vector2, ResourceVectorNear(vector1, 1e-7))

MATCHER_P2(ResourceVolumeNear, vec, absError, "") {
    bool result = true;
    TResourceVolume::ForEachResource([&] (EJobResourceType /*resourceType*/, auto TResourceVolume::* resourceDataMember) {
        result = result && std::abs(static_cast<double>(arg.*resourceDataMember - vec.*resourceDataMember)) < absError;
    });
    return result;
}

#define EXPECT_RESOURCE_VOLUME_NEAR(vector1, vector2) \
    EXPECT_THAT(vector2, ResourceVolumeNear(vector1, 1e-7))

////////////////////////////////////////////////////////////////////////////////

TEST_F(TFairShareTreeTest, TestAttributes)
{
    constexpr int OperationCount = 4;

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(10);
    nodeResources.SetCpu(10);
    nodeResources.SetMemory(100);

    TJobResourcesWithQuota jobResources;
    jobResources.SetUserSlots(1);
    jobResources.SetCpu(1);
    jobResources.SetMemory(10);

    auto host = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(10, nodeResources));

    auto rootElement = CreateTestRootElement(host.Get());

    auto fifoPoolConfig = New<TPoolConfig>();
    fifoPoolConfig->Mode = ESchedulingMode::Fifo;
    fifoPoolConfig->FifoSortParameters = {EFifoSortParameter::Weight};

    auto poolA = CreateTestPool(host.Get(), "PoolA");
    auto poolB = CreateTestPool(host.Get(), "PoolB");
    auto poolC = CreateTestPool(host.Get(), "PoolC", fifoPoolConfig);
    auto poolD = CreateTestPool(host.Get(), "PoolD", fifoPoolConfig);

    poolA->AttachParent(rootElement.Get());
    poolB->AttachParent(rootElement.Get());
    poolC->AttachParent(rootElement.Get());
    poolD->AttachParent(rootElement.Get());

    std::array<TIntrusivePtr<TOperationStrategyHostMock>, OperationCount> operations;
    std::array<TSchedulerOperationElementPtr, OperationCount> operationElements;

    for (auto& operation : operations) {
        operation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(10, jobResources));
    }

    for (int i = 0; i < OperationCount; ++i) {
        TSchedulerCompositeElement* parent = i < 2
            ? poolA.Get()
            : poolC.Get();

        TOperationFairShareTreeRuntimeParametersPtr operationOptions;
        if (i == 2) {
            // We need this to ensure FIFO order of operations 2 and 3.
            operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
            operationOptions->Weight = 10.0;
        }

        operationElements[i] = CreateTestOperationElement(host.Get(), operations[i].Get(), parent, operationOptions);
    }

    {
        DoFairShareUpdate(host.Get(), rootElement);

        auto expectedOperationDemand = TResourceVector::FromJobResources(jobResources, nodeResources);
        auto poolExpectedDemand = expectedOperationDemand * (OperationCount / 2.0);
        auto totalExpectedDemand = expectedOperationDemand * OperationCount;

        EXPECT_THAT(totalExpectedDemand, ResourceVectorNear(rootElement->Attributes().DemandShare, 1e-7));
        EXPECT_THAT(poolExpectedDemand, ResourceVectorNear(poolA->Attributes().DemandShare, 1e-7));
        EXPECT_THAT(TResourceVector::Zero(), ResourceVectorNear(poolB->Attributes().DemandShare, 1e-7));
        EXPECT_THAT(poolExpectedDemand, ResourceVectorNear(poolC->Attributes().DemandShare, 1e-7));
        EXPECT_THAT(TResourceVector::Zero(), ResourceVectorNear(poolD->Attributes().DemandShare, 1e-7));
        for (const auto& operationElement : operationElements) {
            EXPECT_THAT(expectedOperationDemand, ResourceVectorNear(operationElement->Attributes().DemandShare, 1e-7));
        }

        EXPECT_THAT(totalExpectedDemand, ResourceVectorNear(rootElement->Attributes().FairShare.Total, 1e-7));
        EXPECT_THAT(poolExpectedDemand, ResourceVectorNear(poolA->Attributes().FairShare.Total, 1e-7));
        EXPECT_THAT(TResourceVector::Zero(), ResourceVectorNear(poolB->Attributes().FairShare.Total, 1e-7));
        EXPECT_THAT(poolExpectedDemand, ResourceVectorNear(poolC->Attributes().FairShare.Total, 1e-7));
        EXPECT_THAT(TResourceVector::Zero(), ResourceVectorNear(poolD->Attributes().FairShare.Total, 1e-7));
        for (const auto& operationElement : operationElements) {
            EXPECT_THAT(expectedOperationDemand, ResourceVectorNear(operationElement->Attributes().FairShare.Total, 1e-7));
        }
    }

    for (int i = 0; i < 10; ++i) {
        operationElements[0]->OnJobStarted(
            TGuid::Create(),
            jobResources.ToJobResources(),
            /* precommitedResources */ {});
        operationElements[2]->OnJobStarted(
            TGuid::Create(),
            jobResources.ToJobResources(),
            /* precommitedResources */ {});
    }

    {
        ResetFairShareFunctionsRecursively(rootElement.Get());

        DoFairShareUpdate(host.Get(), rootElement);

        // Demand increased to 0.2 due to started jobs, so did fair share.
        // usage(0.1) / fair_share(0.2) = 0.5
        EXPECT_EQ(0.5, operationElements[0]->Attributes().SatisfactionRatio);
        EXPECT_EQ(0.0, operationElements[1]->Attributes().SatisfactionRatio);
        EXPECT_EQ(0.5, operationElements[2]->Attributes().SatisfactionRatio);
        EXPECT_EQ(0.0, operationElements[3]->Attributes().SatisfactionRatio);
        EXPECT_EQ(0.0, poolA->Attributes().SatisfactionRatio);
        EXPECT_EQ(InfiniteSatisfactionRatio, poolB->Attributes().SatisfactionRatio);
        // NB(eshcherbin): Here it's 1/3 because in FIFO pools we don't search for the least satisfied child;
        // in this case, we take the minimum of the pool's local satisfaction (1/3) and the first child's satisfaction (0.5).
        EXPECT_NEAR(1.0 / 3.0, poolC->Attributes().SatisfactionRatio, 1e-7);
        EXPECT_EQ(InfiniteSatisfactionRatio, poolD->Attributes().SatisfactionRatio);
    }

    for (int i = 0; i < 10; ++i) {
        operationElements[1]->OnJobStarted(
            TGuid::Create(),
            jobResources.ToJobResources(),
            /* precommitedResources */ {});
        operationElements[3]->OnJobStarted(
            TGuid::Create(),
            jobResources.ToJobResources(),
            /* precommitedResources */ {});
    }

    {
        ResetFairShareFunctionsRecursively(rootElement.Get());

        DoFairShareUpdate(host.Get(), rootElement);

        // Demand increased to 0.2 due to started jobs, so did fair share.
        // usage(0.1) / fair_share(0.2) = 0.5
        EXPECT_EQ(0.5, operationElements[0]->Attributes().SatisfactionRatio);
        EXPECT_EQ(0.5, operationElements[1]->Attributes().SatisfactionRatio);
        EXPECT_EQ(0.5, operationElements[2]->Attributes().SatisfactionRatio);
        EXPECT_EQ(0.5, operationElements[3]->Attributes().SatisfactionRatio);
        EXPECT_EQ(0.5, poolA->Attributes().SatisfactionRatio);
        EXPECT_EQ(InfiniteSatisfactionRatio, poolB->Attributes().SatisfactionRatio);
        EXPECT_EQ(0.5, poolC->Attributes().SatisfactionRatio);
        EXPECT_EQ(InfiniteSatisfactionRatio, poolD->Attributes().SatisfactionRatio);
    }
}

TEST_F(TFairShareTreeTest, TestResourceLimits)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(10);
    nodeResources.SetCpu(10);
    nodeResources.SetMemory(100);

    auto totalLimitsShare = TResourceVector::FromJobResources(nodeResources.ToJobResources(), nodeResources.ToJobResources());

    auto host = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(1, nodeResources));

    auto rootElement = CreateTestRootElement(host.Get());

    auto poolA = CreateTestPool(host.Get(), "PoolA");
    poolA->AttachParent(rootElement.Get());

    auto poolB = CreateTestPool(host.Get(), "PoolB");
    poolB->AttachParent(poolA.Get());

    {
        DoFairShareUpdate(host.Get(), rootElement);

        EXPECT_EQ(totalLimitsShare, rootElement->Attributes().LimitsShare);
        EXPECT_EQ(nodeResources.ToJobResources(), rootElement->GetResourceLimits());
        EXPECT_EQ(nodeResources.ToJobResources(), rootElement->GetTotalResourceLimits());

        EXPECT_EQ(totalLimitsShare, poolA->Attributes().LimitsShare);
        EXPECT_EQ(nodeResources.ToJobResources(), poolA->GetResourceLimits());
        EXPECT_EQ(nodeResources.ToJobResources(), poolA->GetTotalResourceLimits());

        EXPECT_EQ(totalLimitsShare, poolB->Attributes().LimitsShare);
        EXPECT_EQ(nodeResources.ToJobResources(), poolB->GetResourceLimits());
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
        DoFairShareUpdate(host.Get(), rootElement);

        EXPECT_EQ(totalLimitsShare, rootElement->Attributes().LimitsShare);
        EXPECT_EQ(nodeResources.ToJobResources(), rootElement->GetResourceLimits());
        EXPECT_EQ(nodeResources.ToJobResources(), rootElement->GetTotalResourceLimits());

        auto poolALimitsShare = TResourceVector::FromJobResources(poolAResourceLimits, nodeResources);
        EXPECT_EQ(poolALimitsShare, poolA->Attributes().LimitsShare);
        EXPECT_EQ(poolAResourceLimits, poolA->GetResourceLimits());
        EXPECT_EQ(nodeResources.ToJobResources(), poolA->GetTotalResourceLimits());

        auto poolBResourceLimits = nodeResources * maxShareRatio;
        auto poolBLimitsShare = TResourceVector::FromJobResources(poolBResourceLimits, nodeResources);
        EXPECT_EQ(poolBLimitsShare, poolB->Attributes().LimitsShare);
        EXPECT_EQ(poolBResourceLimits, poolB->GetResourceLimits());
        EXPECT_EQ(nodeResources.ToJobResources(), poolB->GetTotalResourceLimits());
    }
}

TEST_F(TFairShareTreeTest, TestSchedulingTagFilterResourceLimits)
{
    TJobResourcesWithQuota nodeResources1;
    nodeResources1.SetUserSlots(2);
    nodeResources1.SetCpu(3);
    nodeResources1.SetMemory(10);

    TJobResourcesWithQuota nodeResources2;
    nodeResources2.SetUserSlots(10);
    nodeResources2.SetCpu(10);
    nodeResources2.SetMemory(100);

    TJobResourcesWithQuota nodeResources3;
    nodeResources3.SetUserSlots(20);
    nodeResources3.SetCpu(30);
    nodeResources3.SetMemory(500);

    TJobResourcesWithQuota nodeResources134;
    nodeResources134.SetUserSlots(100);
    nodeResources134.SetCpu(50);
    nodeResources134.SetMemory(1000);

    TJobResourcesWithQuotaList nodeResourceLimitsList = {nodeResources1, nodeResources2, nodeResources3, nodeResources134};

    TBooleanFormulaTags tags1(THashSet<TString>({"tag_1"}));
    TBooleanFormulaTags tags2(THashSet<TString>({"tag_2"}));
    TBooleanFormulaTags tags3(THashSet<TString>({"tag_3"}));
    TBooleanFormulaTags tags134(THashSet<TString>({"tag_1", "tag_3", "tag_4"}));
    std::vector<TBooleanFormulaTags> tagList = {tags1, tags2, tags3, tags134};

    std::vector<TExecNodePtr> execNodes(4);
    for (int i = 0; i < std::ssize(execNodes); ++i) {
        execNodes[i] = CreateTestExecNode(nodeResourceLimitsList[i], tagList[i]);
    }

    auto host = New<TSchedulerStrategyHostMock>(execNodes);

    auto rootElement = CreateTestRootElement(host.Get());

    auto configA = New<TPoolConfig>();
    auto poolA = CreateTestPool(host.Get(), "PoolA", configA);
    poolA->AttachParent(rootElement.Get());

    auto configB = New<TPoolConfig>();
    configB->SchedulingTagFilter = MakeBooleanFormula("tag_1");
    auto poolB = CreateTestPool(host.Get(), "PoolB", configB);
    poolB->AttachParent(rootElement.Get());

    auto configC = New<TPoolConfig>();
    configC->SchedulingTagFilter = MakeBooleanFormula("tag_3") & poolB->GetConfig()->SchedulingTagFilter;
    auto poolC = CreateTestPool(host.Get(), "PoolC", configC);
    poolC->AttachParent(poolB.Get());

    auto configD = New<TPoolConfig>();
    configD->SchedulingTagFilter = MakeBooleanFormula("tag_3");
    auto poolD = CreateTestPool(host.Get(), "PoolD", configD);
    poolD->AttachParent(poolB.Get());

    auto configE = New<TPoolConfig>();
    configE->SchedulingTagFilter = MakeBooleanFormula("tag_2 | (tag_1 & tag_4)");
    auto poolE = CreateTestPool(host.Get(), "PoolE", configE);
    poolE->AttachParent(rootElement.Get());

    auto operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions->Weight = 1.0;

    TJobResourcesWithQuota jobResources;
    jobResources.SetUserSlots(1);
    jobResources.SetCpu(1);
    jobResources.SetMemory(10);

    auto operationX = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(10, jobResources));
    auto specX = New<TStrategyOperationSpec>();
    specX->SchedulingTagFilter = MakeBooleanFormula("tag_1 | tag_2 | tag_5");
    auto operationElementX = CreateTestOperationElement(host.Get(), operationX.Get(), rootElement.Get(), operationOptions, specX);

    auto operationY = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(5, jobResources));
    auto operationElementY = CreateTestOperationElement(host.Get(), operationX.Get(), poolD.Get(), operationOptions);

    {
        DoFairShareUpdate(host.Get(), rootElement);

        EXPECT_EQ(nodeResources1 + nodeResources2 + nodeResources3 + nodeResources134,
            poolA->GetSchedulingTagFilterResourceLimits());
        EXPECT_EQ(nodeResources1 + nodeResources134, poolB->GetSchedulingTagFilterResourceLimits());
        EXPECT_EQ(nodeResources134.ToJobResources(), poolC->GetSchedulingTagFilterResourceLimits());
        EXPECT_EQ(nodeResources3 + nodeResources134, poolD->GetSchedulingTagFilterResourceLimits());
        EXPECT_EQ(nodeResources2 + nodeResources134, poolE->GetSchedulingTagFilterResourceLimits());

        EXPECT_EQ(nodeResources1 + nodeResources2 + nodeResources134,
            operationElementX->GetSchedulingTagFilterResourceLimits());
        EXPECT_EQ(nodeResources1 + nodeResources2 + nodeResources3 + nodeResources134,
            operationElementY->GetSchedulingTagFilterResourceLimits());
    }    
}


TEST_F(TFairShareTreeTest, TestFractionalResourceLimits)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(10);
    nodeResources.SetCpu(11.17);
    nodeResources.SetMemory(100);

    auto totalLimitsShare = TResourceVector::FromJobResources(nodeResources.ToJobResources(), nodeResources.ToJobResources());

    auto host = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(1, nodeResources));

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
        DoFairShareUpdate(host.Get(), rootElement);

        EXPECT_EQ(totalLimitsShare, rootElement->Attributes().LimitsShare);
        EXPECT_EQ(nodeResources.ToJobResources(), rootElement->GetResourceLimits());
        EXPECT_EQ(nodeResources.ToJobResources(), rootElement->GetTotalResourceLimits());

        auto poolLimitsShare = TResourceVector::FromJobResources(poolResourceLimits, nodeResources);
        EXPECT_EQ(poolLimitsShare, poolA->Attributes().LimitsShare);
        EXPECT_EQ(poolResourceLimits.ToJobResources(), poolA->GetResourceLimits());
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

    auto host = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(10, nodeResources));

    auto rootElement = CreateTestRootElement(host.Get());

    auto operationX = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(10, jobResources));
    auto operationElementX = CreateTestOperationElement(host.Get(), operationX.Get(), rootElement.Get(), operationOptions);

    std::vector<TJobId> jobIds;
    for (int i = 0; i < 150; ++i) {
        auto jobId = TGuid::Create();
        jobIds.push_back(jobId);
        operationElementX->OnJobStarted(
            jobId,
            jobResources.ToJobResources(),
            /* precommitedResources */ {});
    }

    DoFairShareUpdate(host.Get(), rootElement);

    EXPECT_EQ(1.6, MaxComponent(operationElementX->Attributes().DemandShare));
    EXPECT_EQ(1.0, MaxComponent(operationElementX->Attributes().FairShare.Total));

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

    auto execNodes = CreateTestExecNodeList(2, nodeResourcesA);
    execNodes.push_back(CreateTestExecNode(nodeResourcesB));

    auto host = New<TSchedulerStrategyHostMock>(execNodes);

    auto rootElement = CreateTestRootElement(host.Get());

    auto operationX = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(3, jobResources));
    auto operationElementX = CreateTestOperationElement(host.Get(), operationX.Get(), rootElement.Get(), operationOptions);

    DoFairShareUpdate(host.Get(), rootElement);

    auto totalResources = nodeResourcesA * 2. + nodeResourcesB;
    auto demandShare = TResourceVector::FromJobResources(jobResources * 3., totalResources);
    auto fairShare = TResourceVector::FromJobResources(jobResources, totalResources);
    EXPECT_EQ(demandShare, operationElementX->Attributes().DemandShare);
    EXPECT_EQ(0.375, operationElementX->PersistentAttributes().BestAllocationShare[EJobResourceType::Memory]);
    EXPECT_RV_NEAR(fairShare, operationElementX->Attributes().FairShare.Total);
}

TEST_F(TFairShareTreeTest, TestOperationCountLimits)
{
    auto host = New<TSchedulerStrategyHostMock>();
    auto rootElement = CreateTestRootElement(host.Get());

    TSchedulerPoolElementPtr pools[3];
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
    for (int i = 0; i < std::ssize(execNodes); ++i) {
        execNodes[i] = CreateTestExecNode(nodeResources);
    }

    auto host = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(execNodes.size(), nodeResources));

    auto rootElement = CreateTestRootElement(host.Get());

    // Create an operation with 2 jobs.
    TJobResourcesWithQuota operationJobResources;
    operationJobResources.SetCpu(10);
    operationJobResources.SetMemory(10);
    operationJobResources.SetDiskQuota(CreateDiskQuota(0));

    auto operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions->Weight = 1.0;
    auto operation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(2, operationJobResources));
    auto operationElement = CreateTestOperationElement(host.Get(), operation.Get(), rootElement.Get(), operationOptions);

    // We run operation with 2 jobs and simulate 3 concurrent heartbeats.
    // Two of them must succeed and call controller ScheduleJob,
    // the third one must skip ScheduleJob call since resource usage precommit is limited by operation demand.

    auto readyToGo = NewPromise<void>();
    auto& operationControllerStrategyHost = operation->GetOperationControllerStrategyHost();
    std::atomic<int> heartbeatsInScheduling(0);
    EXPECT_CALL(
        operationControllerStrategyHost,
        ScheduleJob(testing::_, testing::_, testing::_, testing::_))
        .Times(2)
        .WillRepeatedly(testing::Invoke([&] (auto /*context*/, auto /*jobLimits*/, auto /*treeId*/, auto /*treeConfig*/) {
            heartbeatsInScheduling.fetch_add(1);
            EXPECT_TRUE(NConcurrency::WaitFor(readyToGo.ToFuture()).IsOK());
            return MakeFuture<TControllerScheduleJobResultPtr>(
                TErrorOr<TControllerScheduleJobResultPtr>(New<TControllerScheduleJobResult>()));
        }));

    std::vector<TFuture<void>> futures;
    auto actionQueue = New<NConcurrency::TActionQueue>();
    for (int i = 0; i < 2; ++i) {
        auto future = BIND([&, i]() {
            DoTestSchedule(rootElement, operationElement, execNodes[i], host.Get());
        }).AsyncVia(actionQueue->GetInvoker()).Run();
        futures.push_back(std::move(future));
    }

    while (heartbeatsInScheduling.load() != 2) {
        // Actively waiting.
    }
    // Number of expected calls to `operationControllerStrategyHost.ScheduleJob(...)` is set to 2.
    // In this way, the mock object library checks that this heartbeat doesn't get to actual scheduling.
    DoTestSchedule(rootElement, operationElement, execNodes[2], host.Get());
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

    auto host = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(nodeCount, nodeResources));

    // Create a tree with 2 pools
    auto rootElement = CreateTestRootElement(host.Get());
    auto poolA = CreateTestPool(host.Get(), "PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateTestPool(host.Get(), "PoolB");
    poolB->AttachParent(rootElement.Get());

    DoFairShareUpdate(host.Get(), rootElement);

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

    auto host = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(nodeCount, nodeResources));

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
    auto operationElementX = CreateTestOperationElement(host.Get(), operationX.Get(), poolA.Get(), operationOptions);

    DoFairShareUpdate(host.Get(), rootElement);

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

    auto host = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(nodeCount, nodeResources));

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
    auto operationElementX = CreateTestOperationElement(host.Get(), operationX.Get(), poolA.Get(), operationOptions);

    DoFairShareUpdate(host.Get(), rootElement);

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

    auto host = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(nodeCount, nodeResources));

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
    auto operationElement1 = CreateTestOperationElement(host.Get(), operation1.Get(), poolA.Get(), operationOptions1);

    // Second operation with symmetric resource demand
    const int jobCount2 = 100;
    TJobResourcesWithQuota jobResources2;
    jobResources2.SetUserSlots(1);
    jobResources2.SetCpu(2);
    jobResources2.SetMemory(10);

    auto operationOptions2 = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions2->Weight = 1.0;

    auto operation2 = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(jobCount2, jobResources2));
    auto operationElement2 = CreateTestOperationElement(host.Get(), operation2.Get(), poolA.Get(), operationOptions2);

    DoFairShareUpdate(host.Get(), rootElement);

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

    auto host = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(nodeCount, nodeResources));

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
    auto operationElement1 = CreateTestOperationElement(host.Get(), operation1.Get(), poolA.Get(), operationOptions1);

    // Create an operation with resource demand proportion <3, 1> and large jobCount in PoolA
    const int jobCount2 = 1000;
    TJobResourcesWithQuota jobResources2;
    jobResources2.SetUserSlots(1);
    jobResources2.SetCpu(3);
    jobResources2.SetMemory(10);

    auto operationOptions2 = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions2->Weight = 1.0;

    auto operation2 = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(jobCount2, jobResources2));
    auto operationElement2 = CreateTestOperationElement(host.Get(), operation2.Get(), poolA.Get(), operationOptions2);

    // Create operation with resource demand proportion <1, 5> and large jobCount in PoolB
    const int jobCount3 = 1000;
    TJobResourcesWithQuota jobResources3;
    jobResources3.SetUserSlots(2);
    jobResources3.SetCpu(2);
    jobResources3.SetMemory(100);

    auto operationOptions3 = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions3->Weight = 1.0;

    auto operation3 = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(jobCount3, jobResources3));
    auto operationElement3 = CreateTestOperationElement(host.Get(), operation3.Get(), poolB.Get(), operationOptions3);

    DoFairShareUpdate(host.Get(), rootElement);

    // Check the values

    // Memory will be saturated first (see the usages of operations bellow)
    EXPECT_RV_NEAR(rootElement->GetFairShare(), TResourceVector({16.0 / 40, 30.0 / 40, 0.0, 40.0 / 40, 0.0}));
    EXPECT_RV_NEAR(poolA->GetFairShare(), TResourceVector({11.0 / 40, 25.0 / 40, 0.0, 15.0 / 40, 0.0}));
    EXPECT_RV_NEAR(poolB->GetFairShare(), TResourceVector({5.0 / 40, 5.0 / 40, 0.0, 25.0 / 40, 0.0}));

    // operation1 uses 4/40 CPU and 8/40 Memory
    EXPECT_RV_NEAR(operationElement1->GetFairShare(), TResourceVector({4.0 / 40, 4.0 / 40, 0.0, 8.0 / 40, 0.0}));
    // operation2 uses 21/40 CPU and 7/40 Memory
    EXPECT_RV_NEAR(operationElement2->GetFairShare(), TResourceVector({7.0 / 40, 21.0 / 40, 0.0, 7.0 / 40, 0.0}));
    // operation3 uses 5/40 CPU and 25/40 Memory
    EXPECT_RV_NEAR(operationElement3->GetFairShare(), TResourceVector({5.0 / 40, 5.0 / 40, 0.0, 25.0 / 40, 0.0}));
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

    auto host = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(nodeCount, nodeResources));

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
    auto operationElement1 = CreateTestOperationElement(host.Get(), operation1.Get(), poolA.Get(), operationOptions1);

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
    auto operationElement2 = CreateTestOperationElement(host.Get(), operation2.Get(), poolA.Get(), operationOptions2);

    DoFairShareUpdate(host.Get(), rootElement);

    // Check the values

    // Memory will be saturated first (see the usages of operations bellow)
    EXPECT_RV_NEAR(rootElement->GetFairShare(), TResourceVector({0.001, 1.0, 0.0, 1.0, 0.4}));
    EXPECT_RV_NEAR(poolA->GetFairShare(), TResourceVector({0.001, 1.0, 0.0, 1.0, 0.4}));
    EXPECT_RV_NEAR(poolB->GetFairShare(), TResourceVector::Zero());

    // operation1 uses 0.1 CPU, 0.1 Memory, and 0.4 Network
    EXPECT_RV_NEAR(operationElement1->GetFairShare(), TResourceVector({0.0001, 0.1, 0.0, 0.1, 0.4}));
    // operation2 uses 0.9 CPU, 0.9 Memory, and 0 Network
    EXPECT_RV_NEAR(operationElement2->GetFairShare(), TResourceVector({0.0009, 0.9, 0.0, 0.9, 0.0}));
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

    auto host = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(nodeCount, nodeResources));

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
    auto operationElement1 = CreateTestOperationElement(host.Get(), operation1.Get(), poolA.Get(), operationOptions1);

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
    auto operationElement2 = CreateTestOperationElement(host.Get(), operation2.Get(), poolA.Get(), operationOptions2);

    DoFairShareUpdate(host.Get(), rootElement);

    // Check the values.
    // 0.4 is a discontinuity point of root's FSBS, so the amount of fair share given to poolA equals to
    // the left limit of FSBS at 0.4, even though we have enough resources to allocate the right limit at 0.4.
    // This is a fundamental property of our strategy.
    EXPECT_RV_NEAR(rootElement->GetFairShare(), TResourceVector({0.00014, 0.14, 0.0, 0.14, 0.4}));
    EXPECT_RV_NEAR(fakeRootElement->GetFairShare(), TResourceVector({0.00014, 0.14, 0.0, 0.14, 0.4}));
    EXPECT_RV_NEAR(poolA->GetFairShare(), TResourceVector({0.00014, 0.14, 0.0, 0.14, 0.4}));
    EXPECT_RV_NEAR(poolB->GetFairShare(), TResourceVector::Zero());

    // Operation 1 uses 0.1 CPU, 0.1 Memory, and 0.4 Network.
    EXPECT_RV_NEAR(operationElement1->GetFairShare(), TResourceVector({0.0001, 0.1, 0.0, 0.1, 0.4}));
    // Operation 2 uses 0.04 CPU, 0.04 Memory, and 0.0 Network.
    EXPECT_RV_NEAR(operationElement2->GetFairShare(), TResourceVector({0.00004, 0.04, 0.0, 0.04, 0.0}));
}

TEST_F(TFairShareTreeTest, TestVectorFairShareImpreciseComposition)
{
    // NB: This test is reconstructed from a core dump. Don't be surprised by precise resource demands. See YT-13864.

    // Create a cluster with 1 large node.
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(3);
    nodeResources.SetCpu(3);
    nodeResources.SetMemory(8316576848);

    auto host = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(1, nodeResources));

    auto rootElement = CreateTestRootElement(host.Get());

    auto poolConfig = New<TPoolConfig>();
    poolConfig->StrongGuaranteeResources->Cpu = 3;
    auto pool = CreateTestPool(host.Get(), "Pool", poolConfig);
    pool->AttachParent(rootElement.Get());

    TJobResourcesWithQuota jobResourcesA;
    jobResourcesA.SetUserSlots(2);
    jobResourcesA.SetCpu(2);
    jobResourcesA.SetMemory(805306368);
    jobResourcesA.SetNetwork(0);

    auto operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions->Weight = 1.0;

    auto operationA = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList{});
    auto operationElementA = CreateTestOperationElement(host.Get(), operationA.Get(), pool.Get(), operationOptions);

    TJobResourcesWithQuota jobResourcesB;
    jobResourcesB.SetUserSlots(3);
    jobResourcesB.SetCpu(3);
    jobResourcesB.SetMemory(1207959552);

    auto operationB = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(1, jobResourcesB));
    auto operationElementB = CreateTestOperationElement(host.Get(), operationB.Get(), pool.Get(), operationOptions);

    operationElementA->OnJobStarted(
        TGuid::Create(),
        jobResourcesA.ToJobResources(),
        /* precommitedResources */ {});

	DoFairShareUpdate(host.Get(), rootElement);

    EXPECT_FALSE(Dominates(TResourceVector::Ones(), pool->GetFairShare()));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TFairShareTreeTest, TruncateUnsatisfiedChildFairShareInFifoPools)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(10);
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(100);

    auto execNode = CreateTestExecNode(nodeResources);

    auto host = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(1, nodeResources));

    auto poolConfig = New<TPoolConfig>();
    poolConfig->TruncateFifoPoolUnsatisfiedChildFairShare = true;
    poolConfig->Mode = ESchedulingMode::Fifo;

    auto rootElement = CreateTestRootElement(host.Get());
    auto poolA = CreateTestPool(host.Get(), "poolA", poolConfig);
    auto poolB = CreateTestPool(host.Get(), "poolB", poolConfig);
    poolA->AttachParent(rootElement.Get());
    poolB->AttachParent(rootElement.Get());

    TJobResourcesWithQuota jobResources;
    jobResources.SetUserSlots(3);
    jobResources.SetCpu(30);
    jobResources.SetMemory(30);

    auto operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions->Weight = 1.0;

    auto operationAFirst = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList({jobResources}));
    auto operationElementAFirst = CreateTestOperationElement(host.Get(), operationAFirst.Get(), poolA.Get(), operationOptions);

    auto operationASecond = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList({jobResources}));
    auto operationElementASecond = CreateTestOperationElement(host.Get(), operationASecond.Get(), poolA.Get(), operationOptions);

    auto operationBFirst = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList({jobResources}));
    auto operationElementBFirst = CreateTestOperationElement(host.Get(), operationBFirst.Get(), poolB.Get(), operationOptions);

    auto operationBSecond = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList({jobResources}));
    auto operationElementBSecond = CreateTestOperationElement(host.Get(), operationBSecond.Get(), poolB.Get(), operationOptions);

    DoFairShareUpdate(host.Get(), rootElement);

    EXPECT_EQ(TResourceVector({0.3, 0.3, 0.0, 0.3, 0.0}), operationElementAFirst->Attributes().DemandShare);
    EXPECT_EQ(TResourceVector({0.3, 0.3, 0.0, 0.3, 0.0}), operationElementASecond->Attributes().DemandShare);
    EXPECT_EQ(TResourceVector({0.3, 0.3, 0.0, 0.3, 0.0}), operationElementBFirst->Attributes().DemandShare);
    EXPECT_EQ(TResourceVector({0.3, 0.3, 0.0, 0.3, 0.0}), operationElementBSecond->Attributes().DemandShare);

    EXPECT_EQ(TResourceVector({0.3, 0.3, 0.0, 0.3, 0.0}), operationElementAFirst->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector({0.0, 0.0, 0.0, 0.0, 0.0}), operationElementASecond->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector({0.3, 0.3, 0.0, 0.3, 0.0}), operationElementBFirst->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector({0.0, 0.0, 0.0, 0.0, 0.0}), operationElementBSecond->Attributes().FairShare.Total);

    EXPECT_EQ(TResourceVector({0.3, 0.3, 0.0, 0.3, 0.0}), poolA->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector({0.3, 0.3, 0.0, 0.3, 0.0}), poolB->Attributes().FairShare.Total);
}

TEST_F(TFairShareTreeTest, DoNotPreemptJobsIfFairShareRatioEqualToDemandRatio)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(100);
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(100);
    nodeResources.SetDiskQuota(CreateDiskQuota(100));

    auto execNode = CreateTestExecNode(nodeResources);

    auto host = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(1, nodeResources));

    auto rootElement = CreateTestRootElement(host.Get());

    // Create an operation with 4 jobs.
    TJobResourcesWithQuota jobResources;
    jobResources.SetCpu(10);
    jobResources.SetMemory(10);
    jobResources.SetDiskQuota(CreateDiskQuota(0));

    auto operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions->Weight = 1.0;
    auto operation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList({}));
    auto operationElement = CreateTestOperationElement(host.Get(), operation.Get(), rootElement.Get(), operationOptions);

    std::vector<TJobId> jobIds;
    for (int i = 0; i < 4; ++i) {
        auto jobId = TGuid::Create();
        jobIds.push_back(jobId);
        operationElement->OnJobStarted(
            jobId,
            jobResources.ToJobResources(),
            /* precommitedResources */ {});
    }

	DoFairShareUpdate(host.Get(), rootElement);

    EXPECT_EQ(TResourceVector({0.0, 0.4, 0.0, 0.4, 0.0}), operationElement->Attributes().DemandShare);
    EXPECT_EQ(TResourceVector({0.0, 0.4, 0.0, 0.4, 0.0}), operationElement->Attributes().FairShare.Total);

    for (int i = 0; i < 2; ++i) {
        EXPECT_FALSE(operationElement->IsJobPreemptable(jobIds[i], /* aggressivePreemptionEnabled */ true));
    }
    for (int i = 2; i < 4; ++i) {
        EXPECT_FALSE(operationElement->IsJobPreemptable(jobIds[i], /* aggressivePreemptionEnabled */ false));
        EXPECT_TRUE(operationElement->IsJobPreemptable(jobIds[i], /* aggressivePreemptionEnabled */ true));
    }

    TJobResources newResources;
    newResources.SetCpu(20);
    newResources.SetMemory(20);
    // FairShare is now less than usage and we would start preempting jobs of this operation.
    operationElement->SetJobResourceUsage(jobIds[0], newResources);

    for (int i = 0; i < 1; ++i) {
        EXPECT_FALSE(operationElement->IsJobPreemptable(jobIds[i], /* aggressivePreemptionEnabled */ true));
    }
    for (int i = 1; i < 4; ++i) {
        EXPECT_FALSE(operationElement->IsJobPreemptable(jobIds[i], /* aggressivePreemptionEnabled */ false));
        EXPECT_TRUE(operationElement->IsJobPreemptable(jobIds[i], /* aggressivePreemptionEnabled */ true));
    }
}

TEST_F(TFairShareTreeTest, TestConditionalPreemption)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(30);
    nodeResources.SetCpu(30);
    nodeResources.SetMemory(300_MB);
    auto execNode = CreateTestExecNode(nodeResources);

    auto host = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(1, nodeResources));
    auto rootElement = CreateTestRootElement(host.Get());
    auto blockingPool = CreateTestPool(host.Get(), "blocking", CreateSimplePoolConfig(/*strongGuaranteeCpu*/ 10.0));
    auto guaranteedPool = CreateTestPool(host.Get(), "guaranteed", CreateSimplePoolConfig(/*strongGuaranteeCpu*/ 20.0));

    blockingPool->AttachParent(rootElement.Get());
    guaranteedPool->AttachParent(rootElement.Get());

    TJobResources jobResources;
    jobResources.SetUserSlots(15);
    jobResources.SetCpu(15);
    jobResources.SetMemory(150_MB);

    auto blockingOperation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList());
    auto blockingOperationElement = CreateTestOperationElement(host.Get(), blockingOperation.Get(), blockingPool.Get());
    blockingOperationElement->OnJobStarted(TGuid::Create(), jobResources, /*precommitedResources*/ {});

    jobResources.SetUserSlots(1);
    jobResources.SetCpu(1);
    jobResources.SetMemory(10_MB);

    auto donorOperation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(5, jobResources));
    auto donorOperationSpec = New<TStrategyOperationSpec>();
    donorOperationSpec->MaxUnpreemptableRunningJobCount = 0;
    auto donorOperationElement = CreateTestOperationElement(
        host.Get(),
        donorOperation.Get(),
        guaranteedPool.Get(),
        /*operationOptions*/ nullptr,
        donorOperationSpec);

    auto now = TInstant::Now();

    std::vector<TJobPtr> donorJobs;
    for (int i = 0; i < 15; ++i) {
        auto job = CreateTestJob(TGuid::Create(), donorOperation->GetId(), execNode, now, jobResources);
        donorJobs.push_back(job);
        donorOperationElement->OnJobStarted(job->GetId(), job->ResourceLimits(), /*precommitedResources*/ {});
    }

    auto [starvingOperationElement, starvingOperation] = CreateOperationWithJobs(10, host.Get(), guaranteedPool.Get());

    {
        DoFairShareUpdate(host.Get(), rootElement, now, /*previousUpdateTime*/ std::nullopt, /*checkForStarvation*/ true);

        TResourceVector unit = {1.0, 1.0, 0.0, 1.0, 0.0};
        EXPECT_RV_NEAR(unit / 3.0, blockingPool->Attributes().FairShare.Total);
        EXPECT_RV_NEAR(unit * 2.0 / 3.0, guaranteedPool->Attributes().FairShare.Total);
        EXPECT_NEAR(1.5, blockingPool->Attributes().LocalSatisfactionRatio, 1e-7);
        EXPECT_NEAR(0.75, guaranteedPool->Attributes().LocalSatisfactionRatio, 1e-7);

        EXPECT_RV_NEAR(unit / 3.0, blockingOperationElement->Attributes().FairShare.Total);
        EXPECT_RV_NEAR(unit / 3.0, donorOperationElement->Attributes().FairShare.Total);
        EXPECT_RV_NEAR(unit / 3.0, starvingOperationElement->Attributes().FairShare.Total);
        EXPECT_NEAR(1.5, blockingOperationElement->Attributes().LocalSatisfactionRatio, 1e-7);
        EXPECT_NEAR(1.5, donorOperationElement->Attributes().LocalSatisfactionRatio, 1e-7);
        EXPECT_NEAR(0.0, starvingOperationElement->Attributes().LocalSatisfactionRatio, 1e-7);

        EXPECT_NEAR(0.8, starvingOperationElement->GetEffectiveFairShareStarvationTolerance(), 1e-7);
        EXPECT_NEAR(0.8, guaranteedPool->GetEffectiveFairShareStarvationTolerance(), 1e-7);

        EXPECT_EQ(ESchedulableStatus::BelowFairShare, starvingOperationElement->GetStatus(/*atUpdate*/ true));
        EXPECT_EQ(ESchedulableStatus::BelowFairShare, guaranteedPool->GetStatus(/*atUpdate*/ true));

        EXPECT_EQ(now, starvingOperationElement->PersistentAttributes().BelowFairShareSince);
        EXPECT_EQ(now, guaranteedPool->PersistentAttributes().BelowFairShareSince);
    }

    {
        auto timeout = starvingOperationElement->GetEffectiveFairShareStarvationTimeout() + TDuration::MilliSeconds(100);
        now += timeout;
        DoFairShareUpdate(host.Get(), rootElement, now, now - timeout, /*checkForStarvation*/ true);

        EXPECT_EQ(EStarvationStatus::NonStarving, donorOperationElement->GetStarvationStatus());
        EXPECT_EQ(EStarvationStatus::Starving, starvingOperationElement->GetStarvationStatus());
        EXPECT_EQ(EStarvationStatus::Starving, guaranteedPool->GetStarvationStatus());

        EXPECT_EQ(nullptr, blockingOperationElement->GetLowestStarvingAncestor());
        EXPECT_EQ(guaranteedPool.Get(), donorOperationElement->GetLowestStarvingAncestor());
        EXPECT_EQ(starvingOperationElement.Get(), starvingOperationElement->GetLowestStarvingAncestor());

        EXPECT_EQ(nullptr, blockingOperationElement->GetLowestAggressivelyStarvingAncestor());
        EXPECT_EQ(nullptr, donorOperationElement->GetLowestAggressivelyStarvingAncestor());
        EXPECT_EQ(nullptr, starvingOperationElement->GetLowestAggressivelyStarvingAncestor());

        EXPECT_FALSE(blockingOperationElement->IsEligibleForPreemptiveScheduling(/*isAggressive*/ false));
        EXPECT_TRUE(donorOperationElement->IsEligibleForPreemptiveScheduling(/*isAggressive*/ false));
        EXPECT_TRUE(starvingOperationElement->IsEligibleForPreemptiveScheduling(/*isAggressive*/ false));
    }

    auto schedulingContext = CreateSchedulingContext(
        /*nodeShardId*/ 0,
        SchedulerConfig_,
        execNode,
        /*runningJobs*/ {},
        host->GetMediumDirectory());

    TScheduleJobsContext context(
        schedulingContext,
        rootElement->GetTreeSize(),
        /*registeredSchedulingTagFilters*/ {},
        /*enableSchedulingInfoLogging*/ true,
        SchedulerLogger);
    context.StartStage(&SchedulingStageMock_, "stage");
    context.PrepareForScheduling(rootElement);

    for (int jobIndex = 0; jobIndex < 10; ++jobIndex) {
        EXPECT_FALSE(donorOperationElement->IsJobPreemptable(donorJobs[jobIndex]->GetId(), /*aggressivePreemptionEnabled*/ false));
    }

    EXPECT_EQ(guaranteedPool.Get(), donorOperationElement->FindPreemptionBlockingAncestor(
        /*isAggressive*/ false,
        context.DynamicAttributesList(),
        TreeConfig_));
    for (int jobIndex = 10; jobIndex < 15; ++jobIndex) {
        const auto& job = donorJobs[jobIndex];
        EXPECT_TRUE(donorOperationElement->IsJobPreemptable(job->GetId(), /*aggressivePreemptionEnabled*/ false));
        context.ConditionallyPreemptableJobSetMap()[guaranteedPool->GetTreeIndex()].insert(job.Get());
    }

    context.PrepareConditionalUsageDiscounts(rootElement.Get(), /*isAggressive*/ false);

    auto jobs = context.GetConditionallyPreemptableJobsInPool(guaranteedPool.Get());
    EXPECT_EQ(5, std::ssize(jobs));
    for (int jobIndex = 10; jobIndex < 15; ++jobIndex) {
        EXPECT_TRUE(jobs.contains(donorJobs[jobIndex].Get()));
    }

    EXPECT_TRUE(context.GetConditionallyPreemptableJobsInPool(blockingPool.Get()).empty());
    EXPECT_TRUE(context.GetConditionallyPreemptableJobsInPool(rootElement.Get()).empty());

    TJobResources expectedDiscount;
    expectedDiscount.SetUserSlots(5);
    expectedDiscount.SetCpu(5);
    expectedDiscount.SetMemory(50_MB);

    EXPECT_EQ(expectedDiscount, schedulingContext->GetMaxConditionalUsageDiscount());
    EXPECT_EQ(expectedDiscount, schedulingContext->GetConditionalDiscountForOperation(starvingOperation->GetId()));
    // It's a bit weird that a preemptable job's usage is added to the discount of its operation, but this is how we do it.
    EXPECT_EQ(expectedDiscount, schedulingContext->GetConditionalDiscountForOperation(donorOperation->GetId()));
    EXPECT_EQ(TJobResources(), schedulingContext->GetConditionalDiscountForOperation(blockingOperation->GetId()));
}

TEST_F(TFairShareTreeTest, TestIncorrectStatusDueToPrecisionError)
{
    // Test is based on real circumstances, all resource amounts are not random.
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(74000);
    nodeResources.SetCpu(7994.4);
    nodeResources.SetMemory(72081411174707);
    nodeResources.SetNetwork(14800);
    nodeResources.SetGpu(1184);
    auto execNode = CreateTestExecNode(nodeResources);

    auto host = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(1, nodeResources));
    auto rootElement = CreateTestRootElement(host.Get());
    auto pool = CreateTestPool(host.Get(), "pool", CreateSimplePoolConfig());

    pool->AttachParent(rootElement.Get());

    TJobResources jobResourcesA;
    jobResourcesA.SetUserSlots(1);
    jobResourcesA.SetCpu(3.81);
    jobResourcesA.SetMemory(107340424507);
    jobResourcesA.SetNetwork(0);
    jobResourcesA.SetGpu(1);

    auto operationA = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList());
    auto operationElementA = CreateTestOperationElement(host.Get(), operationA.Get(), pool.Get());
    operationElementA->OnJobStarted(TGuid::Create(), jobResourcesA, /*precommitedResources*/ {});

    TJobResources jobResourcesB;
    jobResourcesB.SetUserSlots(1);
    jobResourcesB.SetCpu(4);
    jobResourcesB.SetMemory(107340424507);
    jobResourcesB.SetNetwork(0);
    jobResourcesB.SetGpu(1);

    auto operationB = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList());
    auto operationElementB = CreateTestOperationElement(host.Get(), operationB.Get(), pool.Get());
    operationElementB->OnJobStarted(TGuid::Create(), jobResourcesB, /*precommitedResources*/ {});

    DoFairShareUpdate(host.Get(), rootElement);

    EXPECT_EQ(pool->Attributes().UsageShare, pool->Attributes().DemandShare);
    EXPECT_TRUE(Dominates(
        pool->Attributes().DemandShare + TResourceVector::SmallEpsilon(),
        pool->Attributes().FairShare.Total));

    EXPECT_EQ(ESchedulableStatus::Normal, pool->GetStatus(/* atUpdate */ true));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TFairShareTreeTest, TestRelaxedPoolFairShareSimple)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto relaxedPool = CreateTestPool(host.Get(), "relaxed", CreateRelaxedPoolConfig(
        /* flowCpu */ 10,
        /* strongGuaranteeCpu */ 10));
    relaxedPool->AttachParent(rootElement.Get());

    auto [operationElement, operationHost] = CreateOperationWithJobs(30, host.Get(), relaxedPool.Get());

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(host.Get(), rootElement, now, now - TDuration::Minutes(1));

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_RV_NEAR(unit * 3, operationElement->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(unit * 3, operationElement->Attributes().FairShare.Total);

        EXPECT_EQ(unit, relaxedPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit, relaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit, relaxedPool->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(unit * 3, relaxedPool->Attributes().FairShare.Total);

        EXPECT_RV_NEAR(unit, rootElement->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit, rootElement->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(unit * 3, rootElement->Attributes().FairShare.Total);
    }
}

TEST_F(TFairShareTreeTest, TestRelaxedPoolWithIncreasedMultiplierLimit)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto defaultRelaxedPool = CreateTestPool(host.Get(), "defaultRelaxed", CreateRelaxedPoolConfig(/* flowCpu */ 10));
    defaultRelaxedPool->AttachParent(rootElement.Get());

    auto increasedLimitConfig = CreateRelaxedPoolConfig(/* flowCpu */ 10);
    increasedLimitConfig->IntegralGuarantees->RelaxedShareMultiplierLimit = 5;
    auto increasedLimitRelaxedPool = CreateTestPool(host.Get(), "increasedLimitRelaxed", increasedLimitConfig);
    increasedLimitRelaxedPool->AttachParent(rootElement.Get());

    auto [operationElement1, operationHost1] = CreateOperationWithJobs(100, host.Get(), defaultRelaxedPool.Get());
    auto [operationElement2, operationHost2] = CreateOperationWithJobs(100, host.Get(), increasedLimitRelaxedPool.Get());

    defaultRelaxedPool->InitAccumulatedResourceVolume(GetHugeVolume());
    increasedLimitRelaxedPool->InitAccumulatedResourceVolume(GetHugeVolume());

    {
        DoFairShareUpdate(host.Get(), rootElement);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 3, defaultRelaxedPool->Attributes().FairShare.IntegralGuarantee);  // Default multiplier is 3.
        EXPECT_EQ(unit * 5, increasedLimitRelaxedPool->Attributes().FairShare.IntegralGuarantee);
    }
}

TEST_F(TFairShareTreeTest, TestBurstPoolFairShareSimple)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto burstPool = CreateTestPool(host.Get(), "burst", CreateBurstPoolConfig(
        /* flowCpu */ 10,
        /* burstCpu */ 10,
        /* strongGuaranteeCpu */ 10));
    burstPool->AttachParent(rootElement.Get());

    auto [operationElement, operationHost] = CreateOperationWithJobs(30, host.Get(), burstPool.Get());

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(host.Get(), rootElement, now, now - TDuration::Minutes(1));

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_RV_NEAR(unit * 3, operationElement->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(unit * 3, operationElement->Attributes().FairShare.Total);

        EXPECT_EQ(unit, burstPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit, burstPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit, burstPool->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(unit * 3, burstPool->Attributes().FairShare.Total);

        EXPECT_EQ(unit, rootElement->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit, rootElement->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(unit * 3, rootElement->Attributes().FairShare.Total);
    }
}

TEST_F(TFairShareTreeTest, TestAccumulatedVolumeProvidesMore)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto relaxedPool = CreateTestPool(host.Get(), "relaxed", CreateRelaxedPoolConfig(/*flowCpu*/ 10));
    relaxedPool->AttachParent(rootElement.Get());

    auto firstUpdateTime = TInstant::Now();
    {
        // Make first update to accumulate volume
        DoFairShareUpdate(
			host.Get(),
			rootElement,
			/*now*/ firstUpdateTime,
			/*previousUpdateTime*/ firstUpdateTime - TDuration::Minutes(1));
    }

    auto [operationElement, operationHost] = CreateOperationWithJobs(30, host.Get(), relaxedPool.Get());
    auto secondUpdateTime = firstUpdateTime + TDuration::Minutes(1);
    {
        ResetFairShareFunctionsRecursively(rootElement.Get());
        DoFairShareUpdate(
			host.Get(),
			rootElement,
			/*now*/ secondUpdateTime,
			/*previousUpdateTime*/ firstUpdateTime);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_RV_NEAR(unit * 3, operationElement->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(unit * 3, operationElement->Attributes().FairShare.Total);

        EXPECT_EQ(TResourceVector::Zero(), relaxedPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 2, relaxedPool->Attributes().FairShare.IntegralGuarantee);  // Here we get two times more share ratio than guaranteed by flow.
        EXPECT_RV_NEAR(unit, relaxedPool->Attributes().FairShare.WeightProportional);
    }
}

TEST_F(TFairShareTreeTest, TestStrongGuaranteePoolVsBurstPool)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto burstPool = CreateTestPool(host.Get(), "burst", CreateBurstPoolConfig(
        /* flowCpu */ 100,
        /* burstCpu */ 50));
    burstPool->AttachParent(rootElement.Get());

    auto strongPool = CreateTestPool(host.Get(), "strong", CreateSimplePoolConfig(/* strongGuaranteeCpu */ 50));
    strongPool->AttachParent(rootElement.Get());

    auto [burstOperationElement, burstOperationHost] = CreateOperationWithJobs(100, host.Get(), burstPool.Get());
    auto [strongOperationElement, strongOperationHost] = CreateOperationWithJobs(100, host.Get(), strongPool.Get());

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(host.Get(), rootElement, now, now - TDuration::Minutes(1));

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 5, strongPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit * 0, strongPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, strongPool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 0, burstPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit * 5, burstPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, burstPool->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(unit * 5, rootElement->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit * 5, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_EQ(unit * 0, rootElement->Attributes().FairShare.WeightProportional);
    }
}

TEST_F(TFairShareTreeTest, TestStrongGuaranteePoolVsRelaxedPool)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto strongPool = CreateTestPool(host.Get(), "strong", CreateSimplePoolConfig(/* strongGuaranteeCpu */ 50));
    strongPool->AttachParent(rootElement.Get());

    auto relaxedPool = CreateTestPool(host.Get(), "relaxed", CreateRelaxedPoolConfig(/* flowCpu */ 100, /* strongGuaranteeCpu */ 0));
    relaxedPool->AttachParent(rootElement.Get());

    auto [strongOperationElement, strongOperationHost] = CreateOperationWithJobs(100, host.Get(), strongPool.Get());
    auto [relaxedOperationElement, relaxedOperationHost] = CreateOperationWithJobs(100, host.Get(), relaxedPool.Get());

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(host.Get(), rootElement, now, now - TDuration::Minutes(1));

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 5, strongPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit * 0, strongPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, strongPool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 0, relaxedPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 5, relaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, relaxedPool->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(unit * 5, rootElement->Attributes().FairShare.StrongGuarantee);
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
        auto now = TInstant::Now();
        DoFairShareUpdate(host.Get(), rootElement, now, now - TDuration::Minutes(1));

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 0, burstPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit * 10, burstPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, burstPool->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(unit * 0, relaxedPool->Attributes().FairShare.Total);

        EXPECT_RV_NEAR(unit * 0, rootElement->Attributes().FairShare.StrongGuarantee);
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
        auto now = TInstant::Now();
        DoFairShareUpdate(host.Get(), rootElement, now, now - TDuration::Minutes(1));

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 0, burstPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit * 5, burstPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, burstPool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 0, relaxedPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 5, relaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, relaxedPool->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(unit * 0, rootElement->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 10, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_EQ(unit * 0, rootElement->Attributes().FairShare.WeightProportional);
    }
}

TEST_F(TFairShareTreeTest, TestAllKindsOfPoolsShareWeightProportionalComponent)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto strongPool = CreateTestPool(host.Get(), "strong", CreateSimplePoolConfig(/* strongGuaranteeCpu */ 10, /* weight */ 1));
    strongPool->AttachParent(rootElement.Get());

    auto burstPool = CreateTestPool(host.Get(), "burst", CreateBurstPoolConfig(
        /* flowCpu */ 10,
        /* burstCpu */ 10,
        /* strongGuaranteeCpu */ 0,
        /* weight */ 1));
    burstPool->AttachParent(rootElement.Get());

    auto relaxedPool = CreateTestPool(host.Get(), "relaxed", CreateRelaxedPoolConfig(
        /* flowCpu */ 10,
        /* strongGuaranteeCpu */ 0,
        /* weight */ 2));
    relaxedPool->AttachParent(rootElement.Get());

    auto noGuaranteePool = CreateTestPool(host.Get(), "noguarantee", CreateSimplePoolConfig(/* strongGuaranteeCpu */ 0, /* weight */ 3));
    noGuaranteePool->AttachParent(rootElement.Get());


    auto [strongOperationElement, strongOperationHost] = CreateOperationWithJobs(100, host.Get(), strongPool.Get());
    auto [burstOperationElement, burstOperationHost] = CreateOperationWithJobs(100, host.Get(), burstPool.Get());
    auto [relaxedOperationElement, relaxedOperationHost] = CreateOperationWithJobs(100, host.Get(), relaxedPool.Get());
    auto [noGuaranteeElement, noGuaranteeOperationHost] = CreateOperationWithJobs(100, host.Get(), noGuaranteePool.Get());

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(host.Get(), rootElement, now, now - TDuration::Minutes(1));

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 1, strongPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit * 0, strongPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 1, strongPool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 0, burstPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit * 1, burstPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 1, burstPool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 0, relaxedPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit * 1, relaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 2, relaxedPool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 0, noGuaranteePool->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit * 0, noGuaranteePool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 3, noGuaranteePool->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(unit * 1, rootElement->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit * 2, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 7, rootElement->Attributes().FairShare.WeightProportional);
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

    auto volume1 = TResourceVolume(oneTenthOfCluster, TDuration::Minutes(1));  // 10% of cluster for 1 minute
    auto volume2 = TResourceVolume(oneTenthOfCluster, TDuration::Minutes(1) * 3.0);  // 30% of cluster for 1 minute
    relaxedPool1->InitAccumulatedResourceVolume(volume1);
    relaxedPool2->InitAccumulatedResourceVolume(volume2);
    {
        DoFairShareUpdate(host.Get(), rootElement);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 0, relaxedPool1->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 1, relaxedPool1->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 3, relaxedPool1->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 0, relaxedPool2->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 3, relaxedPool2->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 3, relaxedPool2->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(unit * 0, rootElement->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 4, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 6, rootElement->Attributes().FairShare.WeightProportional);
    }
}

TEST_F(TFairShareTreeTest, TestVolumeOverflowDistributionSimple)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());
    auto ancestor = CreateTestPool(host.Get(), "ancestor", CreateIntegralPoolConfig(
        EIntegralGuaranteeType::None,
        /*flowCpu*/ 100,
        /*burstCpu*/ 100));
    ancestor->AttachParent(rootElement.Get());

    auto acceptablePool = CreateTestPool(host.Get(), "acceptablePool", CreateRelaxedPoolConfig(/*flowCpu*/ 1));  // 1% of cluster.
    acceptablePool->AttachParent(ancestor.Get());

    auto overflowedPool = CreateTestPool(host.Get(), "overflowedPool", CreateRelaxedPoolConfig(/*flowCpu*/ 10));  // 10% of cluster.
    overflowedPool->AttachParent(ancestor.Get());

    acceptablePool->InitAccumulatedResourceVolume(TResourceVolume());
    overflowedPool->InitAccumulatedResourceVolume(GetHugeVolume());
    {
        auto updateTime = TInstant::Now();
        DoFairShareUpdate(
            host.Get(),
            rootElement,
            /*now*/ updateTime,
            /*previousUpdateTime*/ updateTime - TDuration::Seconds(10));

        auto selfVolume = TResourceVolume(OneHundredthOfCluster(), TDuration::Seconds(10));  // 1% of cluster for 10 seconds.
        auto overflowedVolume = TResourceVolume(OneHundredthOfCluster() * 10., TDuration::Seconds(10));  // 10% of cluster for 10 seconds.
        auto expectedVolume = selfVolume + overflowedVolume;
        EXPECT_RESOURCE_VOLUME_NEAR(expectedVolume, acceptablePool->GetAccumulatedResourceVolume());
    }
}

TEST_F(TFairShareTreeTest, TestVolumeOverflowDistributionWithMinimalVolumeShares)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());
    auto ancestor = CreateTestPool(host.Get(), "ancestor", CreateIntegralPoolConfig(
        EIntegralGuaranteeType::None,
        /*flowCpu*/ 100,
        /*burstCpu*/ 100));
    ancestor->AttachParent(rootElement.Get());

    auto acceptablePool = CreateTestPool(host.Get(), "acceptablePool", CreateRelaxedPoolConfig(/*flowCpu*/ 1));  // 1% of cluster.
    acceptablePool->AttachParent(ancestor.Get());

    auto overflowedPool = CreateTestPool(host.Get(), "overflowedPool", CreateRelaxedPoolConfig(/*flowCpu*/ 10));  // 10% of cluster.
    overflowedPool->AttachParent(ancestor.Get());

    acceptablePool->InitAccumulatedResourceVolume(TResourceVolume());
    overflowedPool->InitAccumulatedResourceVolume(GetHugeVolume());
    {
        // Volume overflow will be very small due to 1 millisecond interval.
        auto updateTime = TInstant::Now();
        DoFairShareUpdate(
            host.Get(),
            rootElement,
            /*now*/ updateTime,
            /*previousUpdateTime*/ updateTime - TDuration::MilliSeconds(1));

        auto selfVolume = TResourceVolume(OneHundredthOfCluster(), TDuration::MilliSeconds(1));  // 1% of cluster for 1 millisecond.
        auto overflowedVolume = TResourceVolume(OneHundredthOfCluster() * 10., TDuration::MilliSeconds(1));  // 10% of cluster for 1 millisecond.
        auto expectedVolume = selfVolume + overflowedVolume;
        auto actualVolume = acceptablePool->GetAccumulatedResourceVolume() ;

        EXPECT_EQ(expectedVolume.GetCpu(), actualVolume.GetCpu());
        EXPECT_NEAR(expectedVolume.GetMemory(), actualVolume.GetMemory(), 1);
        EXPECT_NEAR(expectedVolume.GetUserSlots(), actualVolume.GetUserSlots(), 1E-6);
        EXPECT_NEAR(expectedVolume.GetNetwork(), actualVolume.GetNetwork(), 1E-6);
        EXPECT_NEAR(expectedVolume.GetGpu(), actualVolume.GetGpu(), 1E-6);
    }
}

TEST_F(TFairShareTreeTest, TestVolumeOverflowDistributionIfPoolDoesNotAcceptIt)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());
    auto ancestor = CreateTestPool(host.Get(), "ancestor", CreateIntegralPoolConfig(
        EIntegralGuaranteeType::None,
        /*flowCpu*/ 100,
        /*burstCpu*/ 100));
    ancestor->AttachParent(rootElement.Get());

    auto poolConfig = CreateRelaxedPoolConfig(/*flowCpu*/ 1);
    poolConfig->IntegralGuarantees->CanAcceptFreeVolume = false;
    auto notAcceptablePool = CreateTestPool(host.Get(), "notAcceptablePool", poolConfig);  // 1% of cluster.
    notAcceptablePool->AttachParent(ancestor.Get());

    auto overflowedPool = CreateTestPool(host.Get(), "overflowedPool", CreateRelaxedPoolConfig(/*flowCpu*/ 10));  // 10% of cluster.
    overflowedPool->AttachParent(ancestor.Get());

    notAcceptablePool->InitAccumulatedResourceVolume(TResourceVolume());
    overflowedPool->InitAccumulatedResourceVolume(GetHugeVolume());
    {
        auto updateTime = TInstant::Now();
        DoFairShareUpdate(
            host.Get(),
            rootElement,
            /*now*/ updateTime,
            /*previousUpdateTime*/ updateTime - TDuration::Seconds(10));

        auto selfVolume = TResourceVolume(OneHundredthOfCluster(), TDuration::Seconds(10));
        EXPECT_RESOURCE_VOLUME_NEAR(selfVolume, notAcceptablePool->GetAccumulatedResourceVolume());
    }
}

TEST_F(TFairShareTreeTest, TestVolumeOverflowDisributionWithLimitedAcceptablePool)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());
    auto ancestor = CreateTestPool(host.Get(), "ancestor", CreateIntegralPoolConfig(
        EIntegralGuaranteeType::None,
        /*flowCpu*/ 100,
        /*burstCpu*/ 100));
    ancestor->AttachParent(rootElement.Get());

    auto emptyVolumePool = CreateTestPool(host.Get(), "fullyAcceptablePool", CreateRelaxedPoolConfig(/*flowCpu*/ 1));
    emptyVolumePool->AttachParent(ancestor.Get());

    auto limitedAcceptablePool = CreateTestPool(host.Get(), "limitedAcceptablePool", CreateRelaxedPoolConfig(/*flowCpu*/ 1));
    limitedAcceptablePool->AttachParent(ancestor.Get());

    auto overflowedPool = CreateTestPool(host.Get(), "overflowedPool", CreateRelaxedPoolConfig(/*flowCpu*/ 10));
    overflowedPool->AttachParent(ancestor.Get());

    auto smallVolumeUnit = TResourceVolume(OneHundredthOfCluster(), TDuration::Seconds(10));  // 1% of cluster for 10 seconds.

    emptyVolumePool->InitAccumulatedResourceVolume(TResourceVolume());
    overflowedPool->InitAccumulatedResourceVolume(GetHugeVolume());

    limitedAcceptablePool->Attributes().ResourceFlowRatio = 0.01;  // 1 flow cpu. It is needed for integral pool capacity.

    auto fullCapacity = limitedAcceptablePool->GetIntegralPoolCapacity();
    limitedAcceptablePool->InitAccumulatedResourceVolume(fullCapacity - smallVolumeUnit*3);  // Can accept three units until full capacity.
    {
        auto updateTime = TInstant::Now();
        DoFairShareUpdate(
            host.Get(),
            rootElement,
            /*now*/ updateTime,
            /*previousUpdateTime*/ updateTime - TDuration::Seconds(10));

        // Limited pool will get 1 (self volume) + 2 (from overflowed sibling) and will have full capacity.
        EXPECT_RESOURCE_VOLUME_NEAR(fullCapacity, limitedAcceptablePool->GetAccumulatedResourceVolume());

        // Empty volume pool will get 1 (self volume) + 8 (remaining from overflowed sibling).
        auto emptyVolumePoolVolume = smallVolumeUnit*9;
        EXPECT_RESOURCE_VOLUME_NEAR(emptyVolumePoolVolume, emptyVolumePool->GetAccumulatedResourceVolume());
    }
}

TEST_F(TFairShareTreeTest, TestStrongGuaranteeAdjustmentToTotalResources)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto strongPool1 = CreateTestPool(host.Get(), "strong1", CreateSimplePoolConfig(/* strongGuaranteeCpu */ 30));
    strongPool1->AttachParent(rootElement.Get());

    auto strongPool2 = CreateTestPool(host.Get(), "strong2", CreateSimplePoolConfig(/* strongGuaranteeCpu */ 90));
    strongPool2->AttachParent(rootElement.Get());

    auto [operationElement1, operationHost1] = CreateOperationWithJobs(100, host.Get(), strongPool1.Get());
    auto [operationElement2, operationHost2] = CreateOperationWithJobs(100, host.Get(), strongPool2.Get());

    {
        DoFairShareUpdate(host.Get(), rootElement);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 2.5, strongPool1->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 0, strongPool1->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, strongPool1->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 7.5, strongPool2->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit * 0, strongPool2->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, strongPool2->Attributes().FairShare.WeightProportional);
    }
}

TEST_F(TFairShareTreeTest, TestStrongGuaranteePlusBurstGuaranteeAdjustmentToTotalResources)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto strongPool = CreateTestPool(host.Get(), "strong", CreateSimplePoolConfig(/* strongGuaranteeCpu */ 90));
    strongPool->AttachParent(rootElement.Get());

    auto burstPool = CreateTestPool(host.Get(), "burst", CreateBurstPoolConfig(
        /* flowCpu */ 60,
        /* burstCpu */ 60));
    burstPool->AttachParent(rootElement.Get());

    auto [operationElement1, operationHost1] = CreateOperationWithJobs(100, host.Get(), strongPool.Get());
    auto [operationElement2, operationHost2] = CreateOperationWithJobs(100, host.Get(), burstPool.Get());

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(host.Get(), rootElement, now, now - TDuration::Minutes(1));

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_RV_NEAR(unit * 6, strongPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 0, strongPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, strongPool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 0, burstPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 4, burstPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, burstPool->Attributes().FairShare.WeightProportional);
    }
}

TEST_F(TFairShareTreeTest, TestLimitsLowerThanStrongGuarantee)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto parentConfig = CreateSimplePoolConfig(/* strongGuaranteeCpu */ 100);
    parentConfig->ResourceLimits->Cpu = 50;
    auto strongPoolParent = CreateTestPool(host.Get(), "strongParent", parentConfig);
    strongPoolParent->AttachParent(rootElement.Get());

    auto strongPoolChild = CreateTestPool(host.Get(), "strongChild", CreateSimplePoolConfig(/* strongGuaranteeCpu */ 100));
    strongPoolChild->AttachParent(strongPoolParent.Get());

    auto [opElement, opHost] = CreateOperationWithJobs(100, host.Get(), strongPoolChild.Get());

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(host.Get(), rootElement, now, now - TDuration::Minutes(1));

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 5, strongPoolParent->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 5, strongPoolParent->Attributes().FairShare.Total);

        EXPECT_EQ(unit * 5, strongPoolChild->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 5, strongPoolChild->Attributes().FairShare.Total);
    }
}

TEST_F(TFairShareTreeTest, TestParentWithoutGuaranteeAndHisLimitsLowerThanChildBurstShare)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto parentConfig = CreateSimplePoolConfig(/* strongGuaranteeCpu */ 0);
    parentConfig->ResourceLimits->Cpu = 50;
    auto limitedParent = CreateTestPool(host.Get(), "limitedParent", parentConfig);
    limitedParent->AttachParent(rootElement.Get());

    auto burstChild = CreateTestPool(host.Get(), "burst", CreateBurstPoolConfig(
        /* flowCpu */ 100,
        /* burstCpu */ 100,
        /* strongGuaranteeCpu */ 0));
    burstChild->AttachParent(limitedParent.Get());

    auto [opElement, opHost] = CreateOperationWithJobs(100, host.Get(), burstChild.Get());

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(host.Get(), rootElement, now, now - TDuration::Minutes(1));

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 5, burstChild->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 5, burstChild->Attributes().FairShare.Total);
    }
}

TEST_F(TFairShareTreeTest, TestParentWithStrongGuaranteeAndHisLimitsLowerThanChildBurstShare)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto parentConfig = CreateSimplePoolConfig(/* strongGuaranteeCpu */ 50);
    parentConfig->ResourceLimits->Cpu = 50;
    auto limitedParent = CreateTestPool(host.Get(), "limitedParent", parentConfig);
    limitedParent->AttachParent(rootElement.Get());

    auto burstChild = CreateTestPool(host.Get(), "burst", CreateBurstPoolConfig(
        /* flowCpu */ 10,
        /* burstCpu */ 10,
        /* strongGuaranteeCpu */ 0));
    burstChild->AttachParent(limitedParent.Get());

    auto [opElement, opHost] = CreateOperationWithJobs(100, host.Get(), burstChild.Get());

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(host.Get(), rootElement, now, now - TDuration::Minutes(1));

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 0, burstChild->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit * 0, burstChild->Attributes().FairShare.IntegralGuarantee);  // Integral share wasn't given due to violation of parent limits.
        EXPECT_RV_NEAR(unit * 5, burstChild->Attributes().FairShare.WeightProportional);
    }
}

TEST_F(TFairShareTreeTest, TestStrongGuaranteeAndRelaxedPoolVsRelaxedPool)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto strongAndRelaxedPool = CreateTestPool(host.Get(), "min_share_and_relaxed", CreateRelaxedPoolConfig(
        /* flowCpu */ 100,
        /* strongGuaranteeCpu */ 40));
    strongAndRelaxedPool->AttachParent(rootElement.Get());

    auto relaxedPool = CreateTestPool(host.Get(), "relaxed", CreateRelaxedPoolConfig(/* flowCpu */ 100));
    relaxedPool->AttachParent(rootElement.Get());

    auto [strongAndRelaxedOperationElement, strongAndRelaxedOperationHost] = CreateOperationWithJobs(100, host.Get(), strongAndRelaxedPool.Get());
    auto [relaxedOperationElement, relaxedOperationHost] = CreateOperationWithJobs(100, host.Get(), relaxedPool.Get());

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(host.Get(), rootElement, now, now - TDuration::Minutes(1));

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 4, strongAndRelaxedPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 3, strongAndRelaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, strongAndRelaxedPool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 0, relaxedPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 3, relaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, relaxedPool->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(unit * 4, rootElement->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 6, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, rootElement->Attributes().FairShare.WeightProportional);
    }
}

TEST_F(TFairShareTreeTest, PromisedFairShareOfIntegralPools)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto burstPoolParent = CreateTestPool(host.Get(), "burstParent", CreateSimplePoolConfig());
    burstPoolParent->AttachParent(rootElement.Get());

    auto burstPool = CreateTestPool(host.Get(), "burst", CreateBurstPoolConfig(
        /* flowCpu */ 30,
        /* burstCpu */ 100));
    burstPool->AttachParent(burstPoolParent.Get());

    auto relaxedPoolParent = CreateTestPool(host.Get(), "relaxedParent", CreateSimplePoolConfig());
    relaxedPoolParent->AttachParent(rootElement.Get());

    auto relaxedPool = CreateTestPool(host.Get(), "relaxed", CreateRelaxedPoolConfig(/* flowCpu */ 70));
    relaxedPool->AttachParent(relaxedPoolParent.Get());

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(host.Get(), rootElement, now, now - TDuration::Minutes(1));

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_RV_NEAR(unit * 3, burstPool->Attributes().PromisedFairShare);
        EXPECT_RV_NEAR(unit * 3, burstPoolParent->Attributes().PromisedFairShare);

        EXPECT_RV_NEAR(unit * 7, relaxedPool->Attributes().PromisedFairShare);
        EXPECT_RV_NEAR(unit * 7, relaxedPoolParent->Attributes().PromisedFairShare);

        EXPECT_EQ(unit * 10, rootElement->Attributes().PromisedFairShare);
    }
}

TEST_F(TFairShareTreeTest, ChildHeap)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(100);
    nodeResources.SetDiskQuota(CreateDiskQuota(100));
    auto execNode = CreateTestExecNode(nodeResources);

    auto host = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(1, nodeResources));

    // Root element.
    auto rootElement = CreateTestRootElement(host.Get());

    // 1/10 of all resources.
    TJobResourcesWithQuota operationJobResources;
    operationJobResources.SetCpu(10);
    operationJobResources.SetMemory(10);
    operationJobResources.SetDiskQuota(CreateDiskQuota(0));

    // Create 5 operations.
    constexpr int OperationCount = 5;
    std::vector<TOperationStrategyHostMockPtr> operations(OperationCount);
    std::vector<TSchedulerOperationElementPtr> operationElements(OperationCount);
    for (int opIndex = 0; opIndex < OperationCount; ++opIndex) {
        auto operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
        operationOptions->Weight = 1.0;
        // Operation with 2 jobs.

        operations[opIndex] = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(2, operationJobResources));
        operationElements[opIndex] = CreateTestOperationElement(
            host.Get(),
            operations[opIndex].Get(),
            rootElement.Get(),
            operationOptions);
    }

    // Expect 2 ScheduleJob calls for each operation.
    for (auto operation : operations) {
        auto& operationControllerStrategyHost = operation->GetOperationControllerStrategyHost();
        EXPECT_CALL(
            operationControllerStrategyHost,
            ScheduleJob(testing::_, testing::_, testing::_, testing::_))
            .Times(2)
            .WillRepeatedly(testing::Invoke([&] (auto /*context*/, auto /*jobLimits*/, auto /*treeId*/, auto /*treeConfig*/) {
                auto result = New<TControllerScheduleJobResult>();
                result->StartDescriptor.emplace(TGuid::Create(), EJobType::Vanilla, operationJobResources, /* interruptible */ false);
                return MakeFuture<TControllerScheduleJobResultPtr>(
                    TErrorOr<TControllerScheduleJobResultPtr>(result));
            }));
    }

	DoFairShareUpdate(host.Get(), rootElement);

    auto schedulingContext = CreateSchedulingContext(
        /* nodeShardId */ 0,
        SchedulerConfig_,
        execNode,
        /* runningJobs */ {},
        host->GetMediumDirectory());

    TScheduleJobsContext context(
        schedulingContext,
        rootElement->GetTreeSize(),
        /* registeredSchedulingTagFilters */ {},
        /* enableSchedulingInfoLogging */ true,
        SchedulerLogger);
    context.StartStage(&SchedulingStageMock_, "stage1");
    context.PrepareForScheduling(rootElement);
    rootElement->CalculateCurrentResourceUsage(&context);
    rootElement->PrescheduleJob(&context, EPrescheduleJobOperationCriterion::All);

    for (auto operationElement : operationElements) {
        const auto& dynamicAttributes = context.DynamicAttributesFor(rootElement.Get());
        ASSERT_TRUE(dynamicAttributes.Active);
    }

    for (int iter = 0; iter < 2; ++iter) {
        for (auto operationElement : operationElements) {
            auto scheduleJobResult = operationElement->ScheduleJob(&context, /* ignorePacking */ true);
            ASSERT_TRUE(scheduleJobResult.Scheduled);

            const auto& childHeapMap = context.ChildHeapMap();
            YT_VERIFY(childHeapMap.contains(rootElement->GetTreeIndex()));

            const auto& childHeap = GetOrCrash(context.ChildHeapMap(), rootElement->GetTreeIndex());

            int heapIndex = 0;
            for (auto* element : childHeap.GetHeap()) {
                ASSERT_EQ(context.DynamicAttributesFor(element).HeapIndex, heapIndex);
                ++heapIndex;
            }
        }
    }
    context.FinishStage();

    // NB(eshcherbin): It is impossible to have two consecutive non-preemptive scheduling stages, however
    // here we only need to trigger the second PrescheduleJob call so that the child heap is rebuilt.
    context.StartStage(&SchedulingStageMock_, "stage2");
    context.PrepareForScheduling(rootElement);
    rootElement->CalculateCurrentResourceUsage(&context);
    rootElement->PrescheduleJob(&context, EPrescheduleJobOperationCriterion::All);

    for (auto operationElement : operationElements) {
        const auto& childHeapMap = context.ChildHeapMap();
        YT_VERIFY(childHeapMap.contains(rootElement->GetTreeIndex()));

        const auto& childHeap = GetOrCrash(context.ChildHeapMap(), rootElement->GetTreeIndex());
        int heapIndex = 0;
        for (auto* element : childHeap.GetHeap()) {
            ASSERT_EQ(context.DynamicAttributesFor(element).HeapIndex, heapIndex);
            ++heapIndex;
        }
    }
}

TEST_F(TFairShareTreeTest, TestAccumulatedResourceVolumeRatioBeforeFairShareUpdate)
{
    auto host = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(0, TJobResourcesWithQuota()));
    auto rootElement = CreateTestRootElement(host.Get());

    auto relaxedPool = CreateTestPool(host.Get(), "relaxed", CreateRelaxedPoolConfig(/* flowCpu */ 100));
    relaxedPool->AttachParent(rootElement.Get());
    EXPECT_EQ(0.0, relaxedPool->GetAccumulatedResourceRatioVolume());
}

TEST_F(TFairShareTreeTest, TestPoolCapacityDoesntDecreaseExistingAccumulatedVolume)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto relaxedPool = CreateTestPool(host.Get(), "relaxed", CreateRelaxedPoolConfig(/* flowCpu */ 100));
    relaxedPool->AttachParent(rootElement.Get());

    auto hugeVolume = GetHugeVolume();
    relaxedPool->InitAccumulatedResourceVolume(hugeVolume);
    {
        auto now = TInstant::Now();
        // Enable refill of volume.
        DoFairShareUpdate(
            host.Get(),
            rootElement,
            now,
            now - TDuration::Seconds(1));

        auto updatedVolume = relaxedPool->GetAccumulatedResourceVolume();
        EXPECT_EQ(hugeVolume.GetCpu(), updatedVolume.GetCpu());
        EXPECT_EQ(hugeVolume.GetMemory(), updatedVolume.GetMemory());
        EXPECT_EQ(hugeVolume.GetUserSlots(), updatedVolume.GetUserSlots());
        EXPECT_EQ(hugeVolume.GetGpu(), updatedVolume.GetGpu());
        EXPECT_EQ(hugeVolume.GetNetwork(), updatedVolume.GetNetwork());
    }
}

TEST_F(TFairShareTreeTest, TestIntegralPoolsWithParent)
{
    auto host = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(host.Get());

    auto limitedParent = CreateTestPool(host.Get(), "parent", CreateIntegralPoolConfig(
        EIntegralGuaranteeType::None,
        /* flowCpu */ 100,
        /* burstCpu */ 100));
    limitedParent->AttachParent(rootElement.Get());

    auto burstPool = CreateTestPool(host.Get(), "burst", CreateBurstPoolConfig(
        /* flowCpu */ 50,
        /* burstCpu */ 100));
    burstPool->AttachParent(limitedParent.Get());

    auto relaxedPool = CreateTestPool(host.Get(), "relaxed", CreateRelaxedPoolConfig(/* flowCpu */ 50));
    relaxedPool->AttachParent(limitedParent.Get());

    auto [burstOperationElement, burstOperationHost] = CreateOperationWithJobs(100, host.Get(), burstPool.Get());
    auto [relaxedOperationElement, relaxedOperationHost] = CreateOperationWithJobs(100, host.Get(), relaxedPool.Get());

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(
            host.Get(),
            rootElement,
            now,
            now - TDuration::Minutes(1));

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 0, burstPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit * 5, burstPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, burstPool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 0, relaxedPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit * 5, relaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, relaxedPool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 0, limitedParent->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit * 10, limitedParent->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, limitedParent->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(unit * 0, rootElement->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit * 10, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_EQ(unit * 0, rootElement->Attributes().FairShare.WeightProportional);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NScheduler::NVectorScheduler
