#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/scheduler/fair_share_tree_element.h>
#include <yt/yt/server/scheduler/operation_controller.h>
#include <yt/yt/server/scheduler/resource_tree.h>

#include <yt/yt/ytlib/chunk_client/proto/medium_directory.pb.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/profiling/profile_manager.h>

#include <yt/yt/core/yson/null_consumer.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NScheduler {

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

    IInvokerPtr GetBackgroundInvoker() const override
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

    NEventLog::TFluentLogEvent LogAccumulatedUsageEventFluently(TInstant /*now*/) override
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

    std::optional<int> FindMediumIndexByName(const TString& /*mediumName*/) const override
    {
        YT_UNIMPLEMENTED();
    }
    
    const TString& GetMediumNameByIndex(int /*mediumIndex*/) const override
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
    
    void SerializeDiskQuota(const TDiskQuota& /*diskQuota*/, NYson::IYsonConsumer* /*consumer*/) const override
    {
        YT_ABORT();
    }

    void LogResourceMetering(
        const TMeteringKey& /*key*/,
        const TMeteringStatistics& /*statistics*/,
        const THashMap<TString, TString>& /*otherTags*/,
        TInstant /*connectionTime*/,
        TInstant /*previousLogTime*/,
        TInstant /*currentTime*/) override
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

    TFuture<void> UpdateLastMeteringLogTime(TInstant /*time*/) override
    {
        return VoidFuture;
    }

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

using TSchedulerStrategyHostMockPtr = TIntrusivePtr<TSchedulerStrategyHostMock>;

////////////////////////////////////////////////////////////////////////////////

class TOperationControllerStrategyHostMock
    : public IOperationControllerStrategyHost
{
public:
    explicit TOperationControllerStrategyHostMock(TJobResourcesWithQuotaList jobResourcesList)
        : JobResourcesList(std::move(jobResourcesList))
    { }

    TControllerEpoch GetEpoch() const override
    {
        return 0;
    }

    MOCK_METHOD(TFuture<TControllerScheduleJobResultPtr>, ScheduleJob, (
        const ISchedulingContextPtr& context,
        const TJobResources& jobLimits,
        const TString& treeId,
        const TString& poolPath,
        const TFairShareStrategyTreeConfigPtr& treeConfig), (override));

    MOCK_METHOD(void, OnNonscheduledJobAborted, (TJobId, EAbortReason, const TString&, TControllerEpoch), (override));

    TCompositeNeededResources GetNeededResources() const override
    {
        TJobResources totalResources;
        for (const auto& resources : JobResourcesList) {
            totalResources += resources.ToJobResources();
        }
        return TCompositeNeededResources{.DefaultResources = totalResources};
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

    EPreemptionMode PreemptionMode = EPreemptionMode::Normal;

    EPreemptionMode GetPreemptionMode() const override
    {
        return PreemptionMode;
    }

private:
    TJobResourcesWithQuotaList JobResourcesList;
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
        YT_ABORT();
    }


    std::optional<EUnschedulableReason> CheckUnschedulable(const std::optional<TString>& /* treeId */) const override
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

    const NYson::TYsonString& GetTrimmedAnnotations() const override
    {
        return TrimmedAnnotations_;
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
    NYson::TYsonString TrimmedAnnotations_;
    TOperationId Id_;
    TOperationControllerStrategyHostMockPtr Controller_;
};

using TOperationStrategyHostMockPtr = TIntrusivePtr<TOperationStrategyHostMock>;

////////////////////////////////////////////////////////////////////////////////

class TFairShareTreeElementHostMock
    : public IFairShareTreeElementHost
{
public:
    explicit TFairShareTreeElementHostMock(const TFairShareStrategyTreeConfigPtr& treeConfig)
        : ResourceTree_(New<TResourceTree>(treeConfig, std::vector<IInvokerPtr>({GetCurrentInvoker()})))
    { }

    TResourceTree* GetResourceTree() override
    {
        return ResourceTree_.Get();
    }

private:
    TResourceTreePtr ResourceTree_;
};

class TFairShareTreeElementTest
    : public testing::Test
{
public:
    TFairShareTreeElementTest()
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
    TIntrusivePtr<TFairShareTreeElementHostMock> FairShareTreeElementHostMock_ = New<TFairShareTreeElementHostMock>(TreeConfig_);
    TScheduleJobsStage NonPreemptiveSchedulingStage_{
        .Type = EJobSchedulingStage::NonPreemptive,
        .ProfilingCounters = TScheduleJobsProfilingCounters(NProfiling::TProfiler{"/non_preemptive_test_scheduling_stage"})};
    TScheduleJobsStage PreemptiveSchedulingStage_{
        .Type = EJobSchedulingStage::Preemptive,
        .ProfilingCounters = TScheduleJobsProfilingCounters(NProfiling::TProfiler{"/preemptive_test_scheduling_stage"})};

    int SlotIndex_ = 0;
    NNodeTrackerClient::TNodeId ExecNodeId_ = 0;

    TDiskQuota CreateDiskQuota(i64 diskSpace)
    {
        TDiskQuota diskQuota;
        diskQuota.DiskSpacePerMedium[NChunkClient::DefaultSlotsMediumIndex] = diskSpace;
        return diskQuota;
    }

    TSchedulerRootElementPtr CreateTestRootElement(ISchedulerStrategyHost* strategyHost)
    {
        return New<TSchedulerRootElement>(
            strategyHost,
            FairShareTreeElementHostMock_.Get(),
            TreeConfig_,
            "default",
            SchedulerLogger);
    }

    TSchedulerPoolElementPtr CreateTestPool(ISchedulerStrategyHost* strategyHost, const TString& name, TPoolConfigPtr config = New<TPoolConfig>())
    {
        return New<TSchedulerPoolElement>(
            strategyHost,
            FairShareTreeElementHostMock_.Get(),
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
        ISchedulerStrategyHost* strategyHost,
        IOperationStrategyHost* operation,
        TSchedulerCompositeElement* parent,
        TOperationFairShareTreeRuntimeParametersPtr operationOptions = nullptr,
        TStrategyOperationSpecPtr operationSpec = nullptr)
    {
        auto operationController = New<TFairShareStrategyOperationController>(
            operation,
            SchedulerConfig_,
            strategyHost->GetNodeShardInvokers().size());
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
            strategyHost,
            FairShareTreeElementHostMock_.Get(),
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
        ISchedulerStrategyHost* strategyHost,
        TSchedulerCompositeElement* parent)
    {
        TJobResourcesWithQuota jobResources;
        jobResources.SetUserSlots(1);
        jobResources.SetCpu(1);
        jobResources.SetMemory(10_MB);

        auto operationHost = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(jobCount, jobResources));
        auto operationElement = CreateTestOperationElement(strategyHost, operationHost.Get(), parent);
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
            TDiskQuota(),
            /*interruptible*/ false,
            /*preemptionMode*/ EPreemptionMode::Normal,
            /*treeId*/ "",
            /*schedulingIndex*/ UndefinedSchedulingIndex);
    }

    void DoTestSchedule(
        const TSchedulerRootElementPtr& rootElement,
        const TSchedulerOperationElementPtr& operationElement,
        const TExecNodePtr& execNode,
        const TSchedulerStrategyHostMock* strategyHost)
    {
        auto schedulingContext = CreateSchedulingContext(
            /* nodeShardId */ 0,
            SchedulerConfig_,
            execNode,
            /* runningJobs */ {},
            strategyHost->GetMediumDirectory());

        DoFairShareUpdate(strategyHost, rootElement);

        TScheduleJobsContext context(
            schedulingContext,
            /* registeredSchedulingTagFilters */ {},
            /* enableSchedulingInfoLogging */ true,
            SchedulerLogger);
        context.StartStage(&NonPreemptiveSchedulingStage_);

        context.PrepareForScheduling(rootElement);
        rootElement->FillResourceUsageInDynamicAttributes(&context.DynamicAttributesList(), /*resourceUsageSnapshot*/ nullptr);
        rootElement->PrescheduleJob(&context, /*targetOperationPreemptionPriority*/ {});

        operationElement->ScheduleJob(&context, /* ignorePacking */ true);

        context.FinishStage();
    }

    void DoFairShareUpdate(
        const ISchedulerStrategyHost* strategyHost,
        const TSchedulerRootElementPtr& rootElement,
        TInstant now = TInstant(),
        std::optional<TInstant> previousUpdateTime = std::nullopt,
        bool checkForStarvation = false)
    {
        ResetFairShareFunctionsRecursively(rootElement.Get());

		NVectorHdrf::TFairShareUpdateContext context(
            /*totalResourceLimits*/ strategyHost->GetResourceLimits(TreeConfig_->NodesFilter),
            TreeConfig_->MainResource,
            TreeConfig_->IntegralGuarantees->PoolCapacitySaturationPeriod,
            TreeConfig_->IntegralGuarantees->SmoothPeriod,
            now,
            previousUpdateTime);

        TFairSharePostUpdateContext fairSharePostUpdateContext{
            .TreeConfig = TreeConfig_,
        };

        rootElement->PreUpdate(&context);

		NVectorHdrf::TFairShareUpdateExecutor updateExecutor(rootElement, &context);
		updateExecutor.Run();

		rootElement->PostUpdate(&fairSharePostUpdateContext);

		if (checkForStarvation) {
            rootElement->UpdateStarvationAttributes(now, /*enablePoolStarvation*/ true);
        }
    }

private:
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
};

////////////////////////////////////////////////////////////////////////////////

TResourceVolume GetHugeVolume()
{
    TResourceVolume hugeVolume;
    hugeVolume.SetCpu(10000000000);
    hugeVolume.SetUserSlots(10000000000);
    hugeVolume.SetMemory(10000000000_MB);
    return hugeVolume;
}

TJobResources GetOnePercentOfCluster()
{
    TJobResources onePercentOfCluster;
    onePercentOfCluster.SetCpu(1);
    onePercentOfCluster.SetUserSlots(1);
    onePercentOfCluster.SetMemory(10_MB);
    return onePercentOfCluster;
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

// Preupdate and postupdate tests.

TEST_F(TFairShareTreeElementTest, TestSatisfactionRatio)
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

    auto strategyHost = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(10, nodeResources));

    auto rootElement = CreateTestRootElement(strategyHost.Get());

    auto fifoPoolConfig = New<TPoolConfig>();
    fifoPoolConfig->Mode = ESchedulingMode::Fifo;
    fifoPoolConfig->FifoSortParameters = {EFifoSortParameter::Weight};

    auto poolA = CreateTestPool(strategyHost.Get(), "PoolA");
    auto poolB = CreateTestPool(strategyHost.Get(), "PoolB");
    auto poolC = CreateTestPool(strategyHost.Get(), "PoolC", fifoPoolConfig);
    auto poolD = CreateTestPool(strategyHost.Get(), "PoolD", fifoPoolConfig);

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

        operationElements[i] = CreateTestOperationElement(strategyHost.Get(), operations[i].Get(), parent, operationOptions);
    }

    for (int i = 0; i < 10; ++i) {
        operationElements[0]->OnJobStarted(
            TGuid::Create(),
            jobResources.ToJobResources(),
            /*precommitedResources*/ {},
            /*scheduleJobEpoch*/ 0);
        operationElements[2]->OnJobStarted(
            TGuid::Create(),
            jobResources.ToJobResources(),
            /*precommitedResources*/ {},
            /*scheduleJobEpoch*/ 0);
    }

    {
        DoFairShareUpdate(strategyHost.Get(), rootElement);

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
            /*precommitedResource */ {},
            /*scheduleJobEpoch*/ 0);
        operationElements[3]->OnJobStarted(
            TGuid::Create(),
            jobResources.ToJobResources(),
            /*precommitedResources*/ {},
            /*scheduleJobEpoch*/ 0);
    }

    {
        DoFairShareUpdate(strategyHost.Get(), rootElement);

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

TEST_F(TFairShareTreeElementTest, TestResourceLimits)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(10);
    nodeResources.SetCpu(10);
    nodeResources.SetMemory(100);

    auto strategyHost = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(1, nodeResources));

    auto rootElement = CreateTestRootElement(strategyHost.Get());

    auto poolA = CreateTestPool(strategyHost.Get(), "PoolA");
    poolA->AttachParent(rootElement.Get());

    auto poolB = CreateTestPool(strategyHost.Get(), "PoolB");
    poolB->AttachParent(poolA.Get());

    {
        DoFairShareUpdate(strategyHost.Get(), rootElement);

        EXPECT_EQ(nodeResources.ToJobResources(), rootElement->GetResourceLimits());
        EXPECT_EQ(nodeResources.ToJobResources(), rootElement->GetTotalResourceLimits());

        EXPECT_EQ(nodeResources.ToJobResources(), poolA->GetResourceLimits());
        EXPECT_EQ(nodeResources.ToJobResources(), poolA->GetTotalResourceLimits());

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
        DoFairShareUpdate(strategyHost.Get(), rootElement);

        EXPECT_EQ(nodeResources.ToJobResources(), rootElement->GetResourceLimits());
        EXPECT_EQ(nodeResources.ToJobResources(), rootElement->GetTotalResourceLimits());

        EXPECT_EQ(poolAResourceLimits, poolA->GetResourceLimits());
        EXPECT_EQ(nodeResources.ToJobResources(), poolA->GetTotalResourceLimits());

        auto poolBResourceLimits = nodeResources * maxShareRatio;
        EXPECT_EQ(poolBResourceLimits, poolB->GetResourceLimits());
        EXPECT_EQ(nodeResources.ToJobResources(), poolB->GetTotalResourceLimits());
    }
}

TEST_F(TFairShareTreeElementTest, TestSchedulingTagFilterResourceLimits)
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

    auto strategyHost = New<TSchedulerStrategyHostMock>(execNodes);

    auto rootElement = CreateTestRootElement(strategyHost.Get());

    auto configA = New<TPoolConfig>();
    auto poolA = CreateTestPool(strategyHost.Get(), "PoolA", configA);
    poolA->AttachParent(rootElement.Get());

    auto configB = New<TPoolConfig>();
    configB->SchedulingTagFilter = MakeBooleanFormula("tag_1");
    auto poolB = CreateTestPool(strategyHost.Get(), "PoolB", configB);
    poolB->AttachParent(rootElement.Get());

    auto configC = New<TPoolConfig>();
    configC->SchedulingTagFilter = MakeBooleanFormula("tag_3") & poolB->GetConfig()->SchedulingTagFilter;
    auto poolC = CreateTestPool(strategyHost.Get(), "PoolC", configC);
    poolC->AttachParent(poolB.Get());

    auto configD = New<TPoolConfig>();
    configD->SchedulingTagFilter = MakeBooleanFormula("tag_3");
    auto poolD = CreateTestPool(strategyHost.Get(), "PoolD", configD);
    poolD->AttachParent(poolB.Get());

    auto configE = New<TPoolConfig>();
    configE->SchedulingTagFilter = MakeBooleanFormula("tag_2 | (tag_1 & tag_4)");
    auto poolE = CreateTestPool(strategyHost.Get(), "PoolE", configE);
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
    auto operationElementX = CreateTestOperationElement(strategyHost.Get(), operationX.Get(), rootElement.Get(), operationOptions, specX);

    auto operationY = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(5, jobResources));
    auto operationElementY = CreateTestOperationElement(strategyHost.Get(), operationX.Get(), poolD.Get(), operationOptions);

    {
        DoFairShareUpdate(strategyHost.Get(), rootElement);

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

TEST_F(TFairShareTreeElementTest, TestFractionalResourceLimits)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(10);
    nodeResources.SetCpu(11.17);
    nodeResources.SetMemory(100);

    auto strategyHost = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(1, nodeResources));

    auto rootElement = CreateTestRootElement(strategyHost.Get());

    auto poolA = CreateTestPool(strategyHost.Get(), "PoolA");
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
        DoFairShareUpdate(strategyHost.Get(), rootElement);

        EXPECT_EQ(nodeResources.ToJobResources(), rootElement->GetResourceLimits());
        EXPECT_EQ(nodeResources.ToJobResources(), rootElement->GetTotalResourceLimits());

        EXPECT_EQ(poolResourceLimits.ToJobResources(), poolA->GetResourceLimits());
        EXPECT_EQ(nodeResources.ToJobResources(), poolA->GetTotalResourceLimits());
    }
}

TEST_F(TFairShareTreeElementTest, TestBestAllocationShare)
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

    auto strategyHost = New<TSchedulerStrategyHostMock>(execNodes);

    auto rootElement = CreateTestRootElement(strategyHost.Get());

    auto operationX = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(3, jobResources));
    auto operationElementX = CreateTestOperationElement(strategyHost.Get(), operationX.Get(), rootElement.Get(), operationOptions);

    DoFairShareUpdate(strategyHost.Get(), rootElement);

    auto totalResources = nodeResourcesA * 2. + nodeResourcesB;
    auto demandShare = TResourceVector::FromJobResources(jobResources * 3., totalResources);
    auto fairShare = TResourceVector::FromJobResources(jobResources, totalResources);
    EXPECT_EQ(demandShare, operationElementX->Attributes().DemandShare);
    EXPECT_EQ(0.375, operationElementX->PersistentAttributes().BestAllocationShare[EJobResourceType::Memory]);
    EXPECT_RV_NEAR(fairShare, operationElementX->Attributes().FairShare.Total);
}

TEST_F(TFairShareTreeElementTest, TestOperationCountLimits)
{
    auto strategyHost = New<TSchedulerStrategyHostMock>();
    auto rootElement = CreateTestRootElement(strategyHost.Get());

    TSchedulerPoolElementPtr pools[3];
    for (int i = 0; i < 3; ++i) {
        pools[i] = CreateTestPool(strategyHost.Get(), "pool" + ToString(i));
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

TEST_F(TFairShareTreeElementTest, TestIncorrectStatusDueToPrecisionError)
{
    // Test is based on real circumstances, all resource amounts are not random.
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(74000);
    nodeResources.SetCpu(7994.4);
    nodeResources.SetMemory(72081411174707);
    nodeResources.SetNetwork(14800);
    nodeResources.SetGpu(1184);
    auto execNode = CreateTestExecNode(nodeResources);

    auto strategyHost = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(1, nodeResources));
    auto rootElement = CreateTestRootElement(strategyHost.Get());
    auto pool = CreateTestPool(strategyHost.Get(), "pool", CreateSimplePoolConfig());

    pool->AttachParent(rootElement.Get());

    TJobResources jobResourcesA;
    jobResourcesA.SetUserSlots(1);
    jobResourcesA.SetCpu(3.81);
    jobResourcesA.SetMemory(107340424507);
    jobResourcesA.SetNetwork(0);
    jobResourcesA.SetGpu(1);

    auto operationA = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList());
    auto operationElementA = CreateTestOperationElement(strategyHost.Get(), operationA.Get(), pool.Get());
    operationElementA->OnJobStarted(
        TGuid::Create(),
        jobResourcesA,
        /*precommitedResources*/ {},
        /*scheduleJobEpoch*/ 0);

    TJobResources jobResourcesB;
    jobResourcesB.SetUserSlots(1);
    jobResourcesB.SetCpu(4);
    jobResourcesB.SetMemory(107340424507);
    jobResourcesB.SetNetwork(0);
    jobResourcesB.SetGpu(1);

    auto operationB = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList());
    auto operationElementB = CreateTestOperationElement(strategyHost.Get(), operationB.Get(), pool.Get());
    operationElementB->OnJobStarted(
        TGuid::Create(),
        jobResourcesB,
        /*precommitedResources*/ {},
        /*scheduleJobEpoch*/ 0);

    DoFairShareUpdate(strategyHost.Get(), rootElement);

    EXPECT_EQ(pool->Attributes().UsageShare, pool->Attributes().DemandShare);
    EXPECT_TRUE(Dominates(
        pool->Attributes().DemandShare + TResourceVector::SmallEpsilon(),
        pool->Attributes().FairShare.Total));

    EXPECT_EQ(ESchedulableStatus::Normal, pool->GetStatus(/* atUpdate */ true));
}

////////////////////////////////////////////////////////////////////////////////

// Integral volume tests.

TEST_F(TFairShareTreeElementTest, TestVolumeOverflowDistributionSimple)
{
    auto strategyHost = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(strategyHost.Get());
    auto ancestor = CreateTestPool(strategyHost.Get(), "ancestor", CreateIntegralPoolConfig(
        EIntegralGuaranteeType::None,
        /*flowCpu*/ 100.0,
        /*burstCpu*/ 100.0));
    ancestor->AttachParent(rootElement.Get());

    // 1% of cluster.
    auto acceptablePool = CreateTestPool(strategyHost.Get(), "acceptablePool", CreateRelaxedPoolConfig(/*flowCpu*/ 1.0));
    acceptablePool->AttachParent(ancestor.Get());

    // 10% of cluster.
    auto overflowedPool = CreateTestPool(strategyHost.Get(), "overflowedPool", CreateRelaxedPoolConfig(/*flowCpu*/ 10.0));
    overflowedPool->AttachParent(ancestor.Get());

    acceptablePool->InitAccumulatedResourceVolume(TResourceVolume());
    overflowedPool->InitAccumulatedResourceVolume(GetHugeVolume());
    {
        auto updateTime = TInstant::Now();
        DoFairShareUpdate(
            strategyHost.Get(),
            rootElement,
            /*now*/ updateTime,
            /*previousUpdateTime*/ updateTime - TDuration::Seconds(10));

        // 1% of cluster for 10 seconds.
        auto selfVolume = TResourceVolume(GetOnePercentOfCluster(), TDuration::Seconds(10));
        // 10% of cluster for 10 seconds.
        auto overflowedVolume = TResourceVolume(GetOnePercentOfCluster() * 10.0, TDuration::Seconds(10));
        auto expectedVolume = selfVolume + overflowedVolume;
        EXPECT_RESOURCE_VOLUME_NEAR(expectedVolume, acceptablePool->GetAccumulatedResourceVolume());
    }
}

TEST_F(TFairShareTreeElementTest, TestVolumeOverflowDistributionWithMinimalVolumeShares)
{
    auto strategyHost = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(strategyHost.Get());
    auto ancestor = CreateTestPool(strategyHost.Get(), "ancestor", CreateIntegralPoolConfig(
        EIntegralGuaranteeType::None,
        /*flowCpu*/ 100.0,
        /*burstCpu*/ 100.0));
    ancestor->AttachParent(rootElement.Get());

    // 1% of cluster.
    auto acceptablePool = CreateTestPool(strategyHost.Get(), "acceptablePool", CreateRelaxedPoolConfig(/*flowCpu*/ 1.0));
    acceptablePool->AttachParent(ancestor.Get());

    // 10% of cluster.
    auto overflowedPool = CreateTestPool(strategyHost.Get(), "overflowedPool", CreateRelaxedPoolConfig(/*flowCpu*/ 10.0));
    overflowedPool->AttachParent(ancestor.Get());

    acceptablePool->InitAccumulatedResourceVolume(TResourceVolume());
    overflowedPool->InitAccumulatedResourceVolume(GetHugeVolume());
    {
        // Volume overflow will be very small due to 1 millisecond interval.
        auto updateTime = TInstant::Now();
        DoFairShareUpdate(
            strategyHost.Get(),
            rootElement,
            /*now*/ updateTime,
            /*previousUpdateTime*/ updateTime - TDuration::MilliSeconds(1));

        // 1% of cluster for 1 millisecond.
        auto selfVolume = TResourceVolume(GetOnePercentOfCluster(), TDuration::MilliSeconds(1));
        // 10% of cluster for 1 millisecond.
        auto overflowedVolume = TResourceVolume(GetOnePercentOfCluster() * 10., TDuration::MilliSeconds(1));
        auto expectedVolume = selfVolume + overflowedVolume;
        auto actualVolume = acceptablePool->GetAccumulatedResourceVolume() ;

        EXPECT_EQ(expectedVolume.GetCpu(), actualVolume.GetCpu());
        EXPECT_NEAR(expectedVolume.GetMemory(), actualVolume.GetMemory(), 1);
        EXPECT_NEAR(expectedVolume.GetUserSlots(), actualVolume.GetUserSlots(), 1E-6);
        EXPECT_NEAR(expectedVolume.GetNetwork(), actualVolume.GetNetwork(), 1E-6);
        EXPECT_NEAR(expectedVolume.GetGpu(), actualVolume.GetGpu(), 1E-6);
    }
}

TEST_F(TFairShareTreeElementTest, TestVolumeOverflowDistributionIfPoolDoesNotAcceptIt)
{
    auto strategyHost = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(strategyHost.Get());
    auto ancestor = CreateTestPool(strategyHost.Get(), "ancestor", CreateIntegralPoolConfig(
        EIntegralGuaranteeType::None,
        /*flowCpu*/ 100.0,
        /*burstCpu*/ 100.0));
    ancestor->AttachParent(rootElement.Get());

    auto poolConfig = CreateRelaxedPoolConfig(/*flowCpu*/ 1.0);
    poolConfig->IntegralGuarantees->CanAcceptFreeVolume = false;
    // 1% of cluster.
    auto notAcceptablePool = CreateTestPool(strategyHost.Get(), "notAcceptablePool", poolConfig);
    notAcceptablePool->AttachParent(ancestor.Get());

    // 10% of cluster.
    auto overflowedPool = CreateTestPool(strategyHost.Get(), "overflowedPool", CreateRelaxedPoolConfig(/*flowCpu*/ 10.0));
    overflowedPool->AttachParent(ancestor.Get());

    notAcceptablePool->InitAccumulatedResourceVolume(TResourceVolume());
    overflowedPool->InitAccumulatedResourceVolume(GetHugeVolume());
    {
        auto updateTime = TInstant::Now();
        DoFairShareUpdate(
            strategyHost.Get(),
            rootElement,
            /*now*/ updateTime,
            /*previousUpdateTime*/ updateTime - TDuration::Seconds(10));

        auto selfVolume = TResourceVolume(GetOnePercentOfCluster(), TDuration::Seconds(10));
        EXPECT_RESOURCE_VOLUME_NEAR(selfVolume, notAcceptablePool->GetAccumulatedResourceVolume());
    }
}

TEST_F(TFairShareTreeElementTest, TestVolumeOverflowDisributionWithLimitedAcceptablePool)
{
    auto strategyHost = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(strategyHost.Get());

    auto ancestor = CreateTestPool(strategyHost.Get(), "ancestor", CreateIntegralPoolConfig(
        EIntegralGuaranteeType::None,
        /*flowCpu*/ 100.0,
        /*burstCpu*/ 100.0));
    ancestor->AttachParent(rootElement.Get());

    auto emptyVolumePool = CreateTestPool(strategyHost.Get(), "fullyAcceptablePool", CreateRelaxedPoolConfig(/*flowCpu*/ 1.0));
    emptyVolumePool->AttachParent(ancestor.Get());

    auto limitedAcceptablePool = CreateTestPool(strategyHost.Get(), "limitedAcceptablePool", CreateRelaxedPoolConfig(/*flowCpu*/ 1.0));
    limitedAcceptablePool->AttachParent(ancestor.Get());

    auto overflowedPool = CreateTestPool(strategyHost.Get(), "overflowedPool", CreateRelaxedPoolConfig(/*flowCpu*/ 10.0));
    overflowedPool->AttachParent(ancestor.Get());

    // 1% of cluster for 10 seconds.
    auto smallVolumeUnit = TResourceVolume(GetOnePercentOfCluster(), TDuration::Seconds(10));

    emptyVolumePool->InitAccumulatedResourceVolume(TResourceVolume());
    overflowedPool->InitAccumulatedResourceVolume(GetHugeVolume());

    // 1 flow cpu. It is needed for integral pool capacity.
    limitedAcceptablePool->Attributes().ResourceFlowRatio = 0.01;

    auto fullCapacity = limitedAcceptablePool->GetIntegralPoolCapacity();
    // Can accept three units until full capacity.
    limitedAcceptablePool->InitAccumulatedResourceVolume(fullCapacity - smallVolumeUnit * 3);
    {
        auto updateTime = TInstant::Now();
        DoFairShareUpdate(
            strategyHost.Get(),
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

TEST_F(TFairShareTreeElementTest, TestAccumulatedResourceVolumeRatioBeforeFairShareUpdate)
{
    auto strategyHost = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(strategyHost.Get());

    auto relaxedPool = CreateTestPool(strategyHost.Get(), "relaxed", CreateRelaxedPoolConfig(/*flowCpu*/ 100.0));
    relaxedPool->AttachParent(rootElement.Get());
    EXPECT_EQ(0.0, relaxedPool->GetAccumulatedResourceRatioVolume());
}

TEST_F(TFairShareTreeElementTest, TestPoolCapacityDoesntDecreaseExistingAccumulatedVolume)
{
    auto strategyHost = CreateHostWith10NodesAnd10Cpu();
    auto rootElement = CreateTestRootElement(strategyHost.Get());

    auto relaxedPool = CreateTestPool(strategyHost.Get(), "relaxed", CreateRelaxedPoolConfig(/*flowCpu*/ 100));
    relaxedPool->AttachParent(rootElement.Get());

    auto hugeVolume = GetHugeVolume();
    relaxedPool->InitAccumulatedResourceVolume(hugeVolume);
    {
        auto now = TInstant::Now();
        // Enable refill of volume.
        DoFairShareUpdate(
            strategyHost.Get(),
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

////////////////////////////////////////////////////////////////////////////////

// Schedule jobs tests.

TEST_F(TFairShareTreeElementTest, TestUpdatePreemptableJobsList)
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

    auto strategyHost = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(10, nodeResources));

    auto rootElement = CreateTestRootElement(strategyHost.Get());

    auto operationX = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(10, jobResources));
    auto operationElementX = CreateTestOperationElement(strategyHost.Get(), operationX.Get(), rootElement.Get(), operationOptions);

    std::vector<TJobId> jobIds;
    for (int i = 0; i < 150; ++i) {
        auto jobId = TGuid::Create();
        jobIds.push_back(jobId);
        // LORDF: How to add jobs easily?
        operationElementX->OnJobStarted(
            jobId,
            jobResources.ToJobResources(),
            /*precommitedResources*/ {},
            /*scheduleJobEpoch*/ 0);
    }

    DoFairShareUpdate(strategyHost.Get(), rootElement);

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

TEST_F(TFairShareTreeElementTest, DontSuggestMoreResourcesThanOperationNeeds)
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

    auto strategyHost = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(execNodes.size(), nodeResources));

    auto rootElement = CreateTestRootElement(strategyHost.Get());

    // Create an operation with 2 jobs.
    TJobResourcesWithQuota operationJobResources;
    operationJobResources.SetCpu(10);
    operationJobResources.SetMemory(10);
    operationJobResources.SetDiskQuota(CreateDiskQuota(0));

    auto operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions->Weight = 1.0;
    auto operation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(2, operationJobResources));
    auto operationElement = CreateTestOperationElement(strategyHost.Get(), operation.Get(), rootElement.Get(), operationOptions);

    // We run operation with 2 jobs and simulate 3 concurrent heartbeats.
    // Two of them must succeed and call controller ScheduleJob,
    // the third one must skip ScheduleJob call since resource usage precommit is limited by operation demand.

    auto readyToGo = NewPromise<void>();
    auto& operationControllerStrategyHost = operation->GetOperationControllerStrategyHost();
    std::atomic<int> heartbeatsInScheduling(0);
    EXPECT_CALL(
        operationControllerStrategyHost,
        ScheduleJob(testing::_, testing::_, testing::_, testing::_, testing::_))
        .Times(2)
        .WillRepeatedly(testing::Invoke([&] (auto /*context*/, auto /*jobLimits*/, auto /*treeId*/, auto /*poolPath*/, auto /*treeConfig*/) {
            heartbeatsInScheduling.fetch_add(1);
            EXPECT_TRUE(NConcurrency::WaitFor(readyToGo.ToFuture()).IsOK());
            return MakeFuture<TControllerScheduleJobResultPtr>(
                TErrorOr<TControllerScheduleJobResultPtr>(New<TControllerScheduleJobResult>()));
        }));

    std::vector<TFuture<void>> futures;
    auto actionQueue = New<NConcurrency::TActionQueue>();
    for (int i = 0; i < 2; ++i) {
        auto future = BIND([&, i]() {
            DoTestSchedule(rootElement, operationElement, execNodes[i], strategyHost.Get());
        }).AsyncVia(actionQueue->GetInvoker()).Run();
        futures.push_back(std::move(future));
    }

    while (heartbeatsInScheduling.load() != 2) {
        // Actively waiting.
    }
    // Number of expected calls to `operationControllerStrategyHost.ScheduleJob(...)` is set to 2.
    // In this way, the mock object library checks that this heartbeat doesn't get to actual scheduling.
    DoTestSchedule(rootElement, operationElement, execNodes[2], strategyHost.Get());
    readyToGo.Set();

    EXPECT_TRUE(AllSucceeded(futures).WithTimeout(TDuration::Seconds(2)).Get().IsOK());
}

TEST_F(TFairShareTreeElementTest, DoNotPreemptJobsIfFairShareRatioEqualToDemandRatio)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(100);
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(100);
    nodeResources.SetDiskQuota(CreateDiskQuota(100));

    auto execNode = CreateTestExecNode(nodeResources);

    auto strategyHost = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(1, nodeResources));

    auto rootElement = CreateTestRootElement(strategyHost.Get());

    // Create an operation with 4 jobs.
    TJobResourcesWithQuota jobResources;
    jobResources.SetCpu(10);
    jobResources.SetMemory(10);
    jobResources.SetDiskQuota(CreateDiskQuota(0));

    auto operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions->Weight = 1.0;
    auto operation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList({}));
    auto operationElement = CreateTestOperationElement(strategyHost.Get(), operation.Get(), rootElement.Get(), operationOptions);

    std::vector<TJobId> jobIds;
    for (int i = 0; i < 4; ++i) {
        auto jobId = TGuid::Create();
        jobIds.push_back(jobId);
        operationElement->OnJobStarted(
            jobId,
            jobResources.ToJobResources(),
            /*precommitedResources*/ {},
            /*scheduleJobEpoch*/ 0);
    }

	DoFairShareUpdate(strategyHost.Get(), rootElement);

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

TEST_F(TFairShareTreeElementTest, TestConditionalPreemption)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(30);
    nodeResources.SetCpu(30);
    nodeResources.SetMemory(300_MB);
    auto execNode = CreateTestExecNode(nodeResources);

    auto strategyHost = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(1, nodeResources));
    auto rootElement = CreateTestRootElement(strategyHost.Get());
    auto blockingPool = CreateTestPool(strategyHost.Get(), "blocking", CreateSimplePoolConfig(/*strongGuaranteeCpu*/ 10.0));
    auto guaranteedPool = CreateTestPool(strategyHost.Get(), "guaranteed", CreateSimplePoolConfig(/*strongGuaranteeCpu*/ 20.0));

    blockingPool->AttachParent(rootElement.Get());
    guaranteedPool->AttachParent(rootElement.Get());

    TJobResources jobResources;
    jobResources.SetUserSlots(15);
    jobResources.SetCpu(15);
    jobResources.SetMemory(150_MB);

    auto blockingOperation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList());
    auto blockingOperationElement = CreateTestOperationElement(strategyHost.Get(), blockingOperation.Get(), blockingPool.Get());
    blockingOperationElement->OnJobStarted(
        TGuid::Create(),
        jobResources,
        /*precommitedResources*/ {},
        /*scheduleJobEpoch*/ 0);

    jobResources.SetUserSlots(1);
    jobResources.SetCpu(1);
    jobResources.SetMemory(10_MB);

    auto donorOperation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(5, jobResources));
    auto donorOperationSpec = New<TStrategyOperationSpec>();
    donorOperationSpec->MaxUnpreemptableRunningJobCount = 0;
    auto donorOperationElement = CreateTestOperationElement(
        strategyHost.Get(),
        donorOperation.Get(),
        guaranteedPool.Get(),
        /*operationOptions*/ nullptr,
        donorOperationSpec);

    auto now = TInstant::Now();

    std::vector<TJobPtr> donorJobs;
    for (int i = 0; i < 15; ++i) {
        auto job = CreateTestJob(TGuid::Create(), donorOperation->GetId(), execNode, now, jobResources);
        donorJobs.push_back(job);
        donorOperationElement->OnJobStarted(
            job->GetId(),
            job->ResourceLimits(),
            /*precommitedResources*/ {},
            /*scheduleJobEpoch*/ 0);
    }

    auto [starvingOperationElement, starvingOperation] = CreateOperationWithJobs(10, strategyHost.Get(), guaranteedPool.Get());

    {
        DoFairShareUpdate(strategyHost.Get(), rootElement, now, /*previousUpdateTime*/ std::nullopt, /*checkForStarvation*/ true);

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
        DoFairShareUpdate(strategyHost.Get(), rootElement, now, now - timeout, /*checkForStarvation*/ true);

        EXPECT_EQ(EStarvationStatus::NonStarving, donorOperationElement->GetStarvationStatus());
        EXPECT_EQ(EStarvationStatus::Starving, starvingOperationElement->GetStarvationStatus());
        EXPECT_EQ(EStarvationStatus::Starving, guaranteedPool->GetStarvationStatus());

        EXPECT_EQ(nullptr, blockingOperationElement->GetLowestStarvingAncestor());
        EXPECT_EQ(guaranteedPool.Get(), donorOperationElement->GetLowestStarvingAncestor());
        EXPECT_EQ(starvingOperationElement.Get(), starvingOperationElement->GetLowestStarvingAncestor());

        EXPECT_EQ(nullptr, blockingOperationElement->GetLowestAggressivelyStarvingAncestor());
        EXPECT_EQ(nullptr, donorOperationElement->GetLowestAggressivelyStarvingAncestor());
        EXPECT_EQ(nullptr, starvingOperationElement->GetLowestAggressivelyStarvingAncestor());
    }

    auto schedulingContext = CreateSchedulingContext(
        /*nodeShardId*/ 0,
        SchedulerConfig_,
        execNode,
        /*runningJobs*/ {},
        strategyHost->GetMediumDirectory());

    TScheduleJobsContext context(
        schedulingContext,
        /*registeredSchedulingTagFilters*/ {},
        /*enableSchedulingInfoLogging*/ true,
        SchedulerLogger);
    context.StartStage(&PreemptiveSchedulingStage_);
    context.PrepareForScheduling(rootElement);

    for (int jobIndex = 0; jobIndex < 10; ++jobIndex) {
        EXPECT_FALSE(donorOperationElement->IsJobPreemptable(donorJobs[jobIndex]->GetId(), /*aggressivePreemptionEnabled*/ false));
    }

    auto targetOperationPreemptionPriority = EOperationPreemptionPriority::Regular;
    EXPECT_EQ(guaranteedPool.Get(), donorOperationElement->FindPreemptionBlockingAncestor(
        targetOperationPreemptionPriority,
        context.DynamicAttributesList(),
        TreeConfig_));
    for (int jobIndex = 10; jobIndex < 15; ++jobIndex) {
        const auto& job = donorJobs[jobIndex];
        auto preemptionStatus = donorOperationElement->GetJobPreemptionStatus(job->GetId());
        EXPECT_EQ(EJobPreemptionStatus::Preemptable, preemptionStatus);
        context.ConditionallyPreemptableJobSetMap()[guaranteedPool->GetTreeIndex()].insert(TJobWithPreemptionInfo{
            .Job = job,
            .PreemptionStatus = preemptionStatus,
            .OperationElement = donorOperationElement
        });
    }

    context.PrepareConditionalUsageDiscounts(rootElement.Get(), targetOperationPreemptionPriority);

    auto jobs = context.GetConditionallyPreemptableJobsInPool(guaranteedPool.Get());
    EXPECT_EQ(5, std::ssize(jobs));
    for (int jobIndex = 10; jobIndex < 15; ++jobIndex) {
        const auto& job = donorJobs[jobIndex];
        EXPECT_TRUE(jobs.contains(TJobWithPreemptionInfo{
            .Job = job,
            .PreemptionStatus = donorOperationElement->GetJobPreemptionStatus(job->GetId()),
            .OperationElement = donorOperationElement,
        }));
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

TEST_F(TFairShareTreeElementTest, TestChildHeap)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(100);
    nodeResources.SetDiskQuota(CreateDiskQuota(100));
    auto execNode = CreateTestExecNode(nodeResources);

    auto strategyHost = New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(1, nodeResources));

    // Root element.
    auto rootElement = CreateTestRootElement(strategyHost.Get());

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
            strategyHost.Get(),
            operations[opIndex].Get(),
            rootElement.Get(),
            operationOptions);
    }

    // Expect 2 ScheduleJob calls for each operation.
    for (auto operation : operations) {
        auto& operationControllerStrategyHost = operation->GetOperationControllerStrategyHost();
        EXPECT_CALL(
            operationControllerStrategyHost,
            ScheduleJob(testing::_, testing::_, testing::_, testing::_, testing::_))
            .Times(2)
            .WillRepeatedly(testing::Invoke([&] (auto /*context*/, auto /*jobLimits*/, auto /*treeId*/, auto /*poolPath*/, auto /*treeConfig*/) {
                auto result = New<TControllerScheduleJobResult>();
                result->StartDescriptor.emplace(TGuid::Create(), EJobType::Vanilla, operationJobResources, /* interruptible */ false);
                return MakeFuture<TControllerScheduleJobResultPtr>(
                    TErrorOr<TControllerScheduleJobResultPtr>(result));
            }));
    }

	DoFairShareUpdate(strategyHost.Get(), rootElement);

    auto schedulingContext = CreateSchedulingContext(
        /* nodeShardId */ 0,
        SchedulerConfig_,
        execNode,
        /* runningJobs */ {},
        strategyHost->GetMediumDirectory());

    TScheduleJobsContext context(
        schedulingContext,
        /* registeredSchedulingTagFilters */ {},
        /* enableSchedulingInfoLogging */ true,
        SchedulerLogger);
    context.StartStage(&NonPreemptiveSchedulingStage_);
    context.PrepareForScheduling(rootElement);
    rootElement->FillResourceUsageInDynamicAttributes(&context.DynamicAttributesList(), /*resourceUsageSnapshot*/ nullptr);
    rootElement->PrescheduleJob(&context, /*targetOperationPreemptionPriority*/ {});

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
    context.StartStage(&NonPreemptiveSchedulingStage_);
    context.PrepareForScheduling(rootElement);
    rootElement->FillResourceUsageInDynamicAttributes(&context.DynamicAttributesList(), /*resourceUsageSnapshot*/ nullptr);
    rootElement->PrescheduleJob(&context, /*targetOperationPreemptionPriority*/ {});

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

////////////////////////////////////////////////////////////////////////////////

// Other tests.

TEST_F(TFairShareTreeElementTest, TestGetPoolPath)
{
    auto strategyHost = New<TSchedulerStrategyHostMock>();
    auto rootElement = CreateTestRootElement(strategyHost.Get());

    auto poolA = CreateTestPool(strategyHost.Get(), "PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateTestPool(strategyHost.Get(), "PoolB");
    poolB->AttachParent(poolA.Get());
    auto poolC = CreateTestPool(strategyHost.Get(), "PoolC");
    poolC->AttachParent(poolB.Get());

    EXPECT_EQ(poolC->GetFullPath(/*explicitOnly*/ true), "/default");
    EXPECT_EQ(poolC->GetFullPath(/*explicitOnly*/ false), "/default/PoolA/PoolB/PoolC");

    // NB. Setting non-default config makes pools explicit.
    poolA->SetConfig(New<TPoolConfig>());
    poolB->SetConfig(New<TPoolConfig>());
    poolC->SetConfig(New<TPoolConfig>());

    EXPECT_EQ(poolC->GetFullPath(/*explicitOnly*/ true), "/default/PoolA/PoolB/PoolC");
    EXPECT_EQ(poolC->GetFullPath(/*explicitOnly*/ false), "/default/PoolA/PoolB/PoolC");

    // NB. Setting default config makes pools non-explicit.
    poolC->SetDefaultConfig();

    EXPECT_EQ(poolC->GetFullPath(/*explicitOnly*/ true), "/default/PoolA/PoolB");
    EXPECT_EQ(poolC->GetFullPath(/*explicitOnly*/ false), "/default/PoolA/PoolB/PoolC");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NScheduler
