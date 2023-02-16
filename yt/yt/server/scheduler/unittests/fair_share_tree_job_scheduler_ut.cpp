#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/scheduler/fair_share_tree.h>
#include <yt/yt/server/scheduler/fair_share_tree_element.h>
#include <yt/yt/server/scheduler/fair_share_tree_job_scheduler.h>
#include <yt/yt/server/scheduler/operation_controller.h>
#include <yt/yt/server/scheduler/resource_tree.h>

#include <yt/yt/ytlib/chunk_client/proto/medium_directory.pb.h>

#include <yt/yt/core/concurrency/action_queue.h>

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
    TSchedulerStrategyHostMock(std::vector<IInvokerPtr> nodeShardInvokers, std::vector<TExecNodePtr> execNodes)
        : NodeShardInvokers_(std::move(nodeShardInvokers))
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

    IInvokerPtr GetControlInvoker(EControlQueue /*queue*/) const override
    {
        return GetCurrentInvoker();
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

    int GetNodeShardId(NNodeTrackerClient::TNodeId /*nodeId*/) const override
    {
        return 0;
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
        YT_UNIMPLEMENTED();
    }

    void Disconnect(const TError& /*error*/) override
    {
        YT_UNIMPLEMENTED();
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

    void AbortJobsAtNode(NNodeTrackerClient::TNodeId /*nodeId*/, EAbortReason /*reason*/) override
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

    void ValidatePoolPermission(
        NObjectClient::TObjectId /*poolObjectId*/,
        const TString& /*poolName*/,
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
        YT_UNIMPLEMENTED();
    }

    void SerializeDiskQuota(const TDiskQuota& /*diskQuota*/, NYson::IYsonConsumer* /*consumer*/) const override
    {
        YT_UNIMPLEMENTED();
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

    void InvokeStoringStrategyState(TPersistentStrategyStatePtr /*persistentStrategyState*/) override
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

class TFairShareTreeHostMock
    : public TRefCounted
    , public IFairShareTreeHost
{
public:

    bool IsConnected() const override
    {
        return true;
    }

    void SetSchedulerTreeAlert(const TString& /*treeId*/, ESchedulerAlertType /*alertType*/, const TError& /*alert*/) override
    { }
};

using TFairShareTreeHostMockPtr = TIntrusivePtr<TFairShareTreeHostMock>;

////////////////////////////////////////////////////////////////////////////////

class TFairShareTreeJobSchedulerHostMock
    : public IFairShareTreeJobSchedulerHost
{
public:
    // NB(eshcherbin): This is a little hack to ensure that periodic actions of the tree job scheduler do not outlive hosts.
    TFairShareTreeJobSchedulerHostMock(TSchedulerStrategyHostMockPtr strategyHost, TFairShareTreeHostMockPtr treeHost)
        : StrategyHost_(std::move(strategyHost))
        , TreeHost_(std::move(treeHost))
    { }

    TFairShareTreeSnapshotPtr GetTreeSnapshot() const noexcept override
    {
        return nullptr;
    }

private:
    TSchedulerStrategyHostMockPtr StrategyHost_;
    TFairShareTreeHostMockPtr TreeHost_;
};

using TFairShareTreeJobSchedulerHostMockPtr = TIntrusivePtr<TFairShareTreeJobSchedulerHostMock>;

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

    MOCK_METHOD(void, OnNonscheduledJobAborted, (TJobId, EAbortReason, TControllerEpoch), (override));

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

    void BuildElementLoggingStringAttributes(
        const TFairShareTreeSnapshotPtr& /*treeSnapshot*/,
        const TSchedulerElement* /*element*/,
        TDelimitedStringBuilderWrapper& /*delimitedBuilder*/) const override
    {
        YT_UNIMPLEMENTED();
    }

private:
    TResourceTreePtr ResourceTree_;
};

using TFairShareTreeElementHostMockPtr = TIntrusivePtr<TFairShareTreeElementHostMock>;

////////////////////////////////////////////////////////////////////////////////

class TFairShareTreeJobSchedulerTest
    : public testing::Test
{
public:
    TFairShareTreeJobSchedulerTest()
    {
        TreeConfig_->AggressivePreemptionSatisfactionThreshold = 0.5;
        TreeConfig_->MinChildHeapSize = 3;
        TreeConfig_->EnableConditionalPreemption = true;
        TreeConfig_->UseResourceUsageWithPrecommit = false;
        TreeConfig_->ShouldDistributeFreeVolumeAmongChildren = true;
    }

protected:
    TSchedulerConfigPtr SchedulerConfig_ = New<TSchedulerConfig>();
    TFairShareTreeHostMockPtr FairShareTreeHostMock_ = New<TFairShareTreeHostMock>();
    TFairShareStrategyTreeConfigPtr TreeConfig_ = New<TFairShareStrategyTreeConfig>();
    TFairShareTreeElementHostMockPtr FairShareTreeElementHostMock_ = New<TFairShareTreeElementHostMock>(TreeConfig_);
    NConcurrency::TActionQueuePtr NodeShardActionQueue_ = New<NConcurrency::TActionQueue>("NodeShard");

    TScheduleJobsStage NonPreemptiveSchedulingStage_{
        .Type = EJobSchedulingStage::NonPreemptive,
        .ProfilingCounters = TScheduleJobsProfilingCounters(NProfiling::TProfiler{"/non_preemptive_test_scheduling_stage"})};
    TScheduleJobsStage PreemptiveSchedulingStage_{
        .Type = EJobSchedulingStage::Preemptive,
        .ProfilingCounters = TScheduleJobsProfilingCounters(NProfiling::TProfiler{"/preemptive_test_scheduling_stage"})};

    int SlotIndex_ = 0;
    NNodeTrackerClient::TNodeId ExecNodeId_ = 0;

    void TearDown() override
    {
        // NB(eshcherbin): To prevent "Promise abandoned" exceptions in tree job scheduler's periodic activities.
        BIND([] { }).AsyncVia(NodeShardActionQueue_->GetInvoker()).Run().Get().ThrowOnError();
    }

    TFairShareTreeJobSchedulerPtr CreateTestTreeScheduler(TWeakPtr<IFairShareTreeJobSchedulerHost> host, ISchedulerStrategyHost* strategyHost)
    {
        return New<TFairShareTreeJobScheduler>(
            /*treeId*/ "default",
            StrategyLogger,
            std::move(host),
            FairShareTreeHostMock_.Get(),
            strategyHost,
            TreeConfig_,
            NProfiling::TProfiler());
    }

    TFairShareTreeJobSchedulerHostMockPtr CreateTestTreeJobSchedulerHost(TSchedulerStrategyHostMockPtr strategyHost)
    {
        return New<TFairShareTreeJobSchedulerHostMock>(std::move(strategyHost), FairShareTreeHostMock_);
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
            /*objectId*/ NObjectClient::TObjectId(),
            std::move(config),
            /*defaultConfigured*/ true,
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
        const TFairShareTreeJobSchedulerPtr& treeScheduler,
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
            New<TFairShareStrategyOperationState>(operation, SchedulerConfig_, strategyHost->GetNodeShardInvokers().size()),
            strategyHost,
            FairShareTreeElementHostMock_.Get(),
            operation,
            "default",
            SchedulerLogger);

        operationElement->AttachParent(parent, SlotIndex_++);
        parent->EnableChild(operationElement);

        treeScheduler->RegisterOperation(operationElement.Get());
        treeScheduler->EnableOperation(operationElement.Get());

        return operationElement;
    }

    std::pair<TSchedulerOperationElementPtr, TOperationStrategyHostMockPtr> CreateOperationWithJobs(
        int jobCount,
        ISchedulerStrategyHost* strategyHost,
        const TFairShareTreeJobSchedulerPtr& treeScheduler,
        TSchedulerCompositeElement* parent)
    {
        TJobResourcesWithQuota jobResources;
        jobResources.SetUserSlots(1);
        jobResources.SetCpu(1);
        jobResources.SetMemory(10_MB);

        auto operationHost = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(jobCount, jobResources));
        auto operationElement = CreateTestOperationElement(strategyHost, treeScheduler, operationHost.Get(), parent);
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

    TDiskQuota CreateDiskQuota(i64 diskSpace)
    {
        TDiskQuota diskQuota;
        diskQuota.DiskSpacePerMedium[NChunkClient::DefaultSlotsMediumIndex] = diskSpace;
        return diskQuota;
    }

    TSchedulerStrategyHostMockPtr CreateTestStrategyHost(std::vector<TExecNodePtr> execNodes)
    {
        return New<TSchedulerStrategyHostMock>(
            std::vector<IInvokerPtr>{NodeShardActionQueue_->GetInvoker()},
            std::move(execNodes));
    }

    TSchedulerStrategyHostMockPtr CreateHostWith10NodesAnd10Cpu()
    {
        TJobResourcesWithQuota nodeResources;
        nodeResources.SetUserSlots(10);
        nodeResources.SetCpu(10);
        nodeResources.SetMemory(100_MB);

        return CreateTestStrategyHost(CreateTestExecNodeList(10, nodeResources));
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

    struct TScheduleJobsContextWithDependencies
    {
        ISchedulingContextPtr SchedulingContext;
        TFairShareTreeSnapshotPtr TreeSnapshot;
        TScheduleJobsContext ScheduleJobsContext;
    };

    TFairShareTreeSnapshotPtr DoFairShareUpdate(
        const ISchedulerStrategyHost* strategyHost,
        const TFairShareTreeJobSchedulerPtr& treeScheduler,
        const TSchedulerRootElementPtr& rootElement,
        TInstant now = TInstant(),
        std::optional<TInstant> previousUpdateTime = std::nullopt)
    {
        ResetFairShareFunctionsRecursively(rootElement.Get());

        NVectorHdrf::TFairShareUpdateContext context(
            /*totalResourceLimits*/ strategyHost->GetResourceLimits(TreeConfig_->NodesFilter),
            TreeConfig_->MainResource,
            TreeConfig_->IntegralGuarantees->PoolCapacitySaturationPeriod,
            TreeConfig_->IntegralGuarantees->SmoothPeriod,
            now,
            previousUpdateTime);

        rootElement->PreUpdate(&context);

        NVectorHdrf::TFairShareUpdateExecutor updateExecutor(rootElement, &context);
        updateExecutor.Run();

        TFairSharePostUpdateContext fairSharePostUpdateContext{
            .TreeConfig = TreeConfig_,
        };
        auto jobSchedulerPostUpdateContext = treeScheduler->CreatePostUpdateContext(rootElement.Get());

        rootElement->PostUpdate(&fairSharePostUpdateContext);
        treeScheduler->PostUpdate(&fairSharePostUpdateContext, &jobSchedulerPostUpdateContext);

        rootElement->UpdateStarvationStatuses(now, /*enablePoolStarvation*/ true);

        // Resource usage and limits are only used for diagnostics, so we don't provide them here.
        auto treeSchedulingSnapshot = treeScheduler->CreateSchedulingSnapshot(&jobSchedulerPostUpdateContext);
        return New<TFairShareTreeSnapshot>(
            TTreeSnapshotId::Create(),
            rootElement,
            std::move(fairSharePostUpdateContext.EnabledOperationIdToElement),
            std::move(fairSharePostUpdateContext.DisabledOperationIdToElement),
            std::move(fairSharePostUpdateContext.PoolNameToElement),
            TreeConfig_,
            SchedulerConfig_,
            /*resourceUsage*/ TJobResources{},
            /*resourceLimits*/ TJobResources{},
            std::move(treeSchedulingSnapshot));
    }

    TScheduleJobsContextWithDependencies PrepareScheduleJobsContext(
        TSchedulerStrategyHostMock* strategyHost,
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TExecNodePtr& execNode)
    {
        auto schedulingContext = CreateSchedulingContext(
            /*nodeShardId*/ 0,
            SchedulerConfig_,
            execNode,
            /*runningJobs*/ {},
            strategyHost->GetMediumDirectory());

        TScheduleJobsContext scheduleJobsContext(
            schedulingContext,
            treeSnapshot,
            /*registeredSchedulingTagFilters*/ {},
            /*nodeSchedulingSegment*/ ESchedulingSegment::Default,
            /*operationCountByPreemptionPriority*/ {},
            /*enableSchedulingInfoLogging*/ true,
            strategyHost,
            SchedulerLogger);

        return TScheduleJobsContextWithDependencies{
            .SchedulingContext = std::move(schedulingContext),
            .TreeSnapshot = std::move(treeSnapshot),
            .ScheduleJobsContext = std::move(scheduleJobsContext),
        };
    }

    void DoTestSchedule(
        TSchedulerStrategyHostMock* strategyHost,
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TExecNodePtr& execNode,
        const TSchedulerOperationElementPtr& operationElement)
    {
        auto scheduleJobsContextWithDependencies = PrepareScheduleJobsContext(strategyHost, treeSnapshot, execNode);
        auto& context = scheduleJobsContextWithDependencies.ScheduleJobsContext;

        context.StartStage(&NonPreemptiveSchedulingStage_);

        context.PrepareForScheduling();
        context.PrescheduleJob();
        context.ScheduleJob(operationElement.Get(), /*ignorePacking*/ true);

        context.FinishStage();
    }

private:
    void ResetFairShareFunctionsRecursively(TSchedulerCompositeElement* compositeElement)
    {
        compositeElement->ResetFairShareFunctions();
        for (const auto& child : compositeElement->EnabledChildren()) {
            if (auto* childPool = dynamic_cast<TSchedulerCompositeElement*>(child.Get())) {
                ResetFairShareFunctionsRecursively(childPool);
            } else {
                child->ResetFairShareFunctions();
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

MATCHER_P2(ResourceVectorNear, vec, absError, "") {
    return TResourceVector::Near(arg, vec, absError);
}

#define EXPECT_RV_NEAR(vector1, vector2) \
    EXPECT_THAT(vector2, ResourceVectorNear(vector1, 1e-7))

////////////////////////////////////////////////////////////////////////////////

// Schedule jobs tests.

TEST_F(TFairShareTreeJobSchedulerTest, TestUpdatePreemptibleJobsList)
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

    auto strategyHost = CreateTestStrategyHost(CreateTestExecNodeList(10, nodeResources));
    auto treeSchedulerHost = CreateTestTreeJobSchedulerHost(strategyHost);
    auto treeScheduler = CreateTestTreeScheduler(treeSchedulerHost, strategyHost.Get());

    auto rootElement = CreateTestRootElement(strategyHost.Get());

    auto operationX = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(10, jobResources));
    auto operationElementX = CreateTestOperationElement(strategyHost.Get(), treeScheduler, operationX.Get(), rootElement.Get(), operationOptions);

    std::vector<TJobId> jobIds;
    for (int i = 0; i < 150; ++i) {
        auto jobId = TGuid::Create();
        jobIds.push_back(jobId);
        treeScheduler->OnJobStartedInTest(operationElementX.Get(), jobId, jobResources);
    }

    DoFairShareUpdate(strategyHost.Get(), treeScheduler, rootElement);

    EXPECT_EQ(1.6, MaxComponent(operationElementX->Attributes().DemandShare));
    EXPECT_EQ(1.0, MaxComponent(operationElementX->Attributes().FairShare.Total));

    for (int i = 0; i < 50; ++i) {
        EXPECT_EQ(EJobPreemptionStatus::NonPreemptible, treeScheduler->GetJobPreemptionStatusInTest(operationElementX.Get(), jobIds[i]));
    }
    for (int i = 50; i < 100; ++i) {
        EXPECT_EQ(EJobPreemptionStatus::AggressivelyPreemptible, treeScheduler->GetJobPreemptionStatusInTest(operationElementX.Get(), jobIds[i]));
    }
    for (int i = 100; i < 150; ++i) {
        EXPECT_EQ(EJobPreemptionStatus::Preemptible, treeScheduler->GetJobPreemptionStatusInTest(operationElementX.Get(), jobIds[i]));
    }
}

TEST_F(TFairShareTreeJobSchedulerTest, DontSuggestMoreResourcesThanOperationNeeds)
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

    auto strategyHost = CreateTestStrategyHost(CreateTestExecNodeList(execNodes.size(), nodeResources));
    auto treeSchedulerHost = CreateTestTreeJobSchedulerHost(strategyHost);
    auto treeScheduler = CreateTestTreeScheduler(treeSchedulerHost, strategyHost.Get());

    auto rootElement = CreateTestRootElement(strategyHost.Get());

    // Create an operation with 2 jobs.
    TJobResourcesWithQuota operationJobResources;
    operationJobResources.SetCpu(10);
    operationJobResources.SetMemory(10);
    operationJobResources.SetDiskQuota(CreateDiskQuota(0));

    auto operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions->Weight = 1.0;
    auto operation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(2, operationJobResources));
    auto operationElement = CreateTestOperationElement(strategyHost.Get(), treeScheduler, operation.Get(), rootElement.Get(), operationOptions);

    auto treeSnapshot = DoFairShareUpdate(strategyHost.Get(), treeScheduler, rootElement);

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
            DoTestSchedule(strategyHost.Get(), treeSnapshot, execNodes[i], operationElement);
        }).AsyncVia(actionQueue->GetInvoker()).Run();
        futures.push_back(std::move(future));
    }

    while (heartbeatsInScheduling.load() != 2) {
        // Actively waiting.
    }
    // Number of expected calls to `operationControllerStrategyHost.ScheduleJob(...)` is set to 2.
    // In this way, the mock object library checks that this heartbeat doesn't get to actual scheduling.
    DoTestSchedule(strategyHost.Get(), treeSnapshot, execNodes[2], operationElement);
    readyToGo.Set();

    EXPECT_TRUE(AllSucceeded(futures).WithTimeout(TDuration::Seconds(2)).Get().IsOK());
}

TEST_F(TFairShareTreeJobSchedulerTest, DoNotPreemptJobsIfFairShareRatioEqualToDemandRatio)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(100);
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(100);
    nodeResources.SetDiskQuota(CreateDiskQuota(100));

    auto execNode = CreateTestExecNode(nodeResources);

    auto strategyHost = CreateTestStrategyHost(CreateTestExecNodeList(1, nodeResources));
    auto treeSchedulerHost = CreateTestTreeJobSchedulerHost(strategyHost);
    auto treeScheduler = CreateTestTreeScheduler(treeSchedulerHost, strategyHost.Get());

    auto rootElement = CreateTestRootElement(strategyHost.Get());

    // Create an operation with 4 jobs.
    TJobResourcesWithQuota jobResources;
    jobResources.SetCpu(10);
    jobResources.SetMemory(10);
    jobResources.SetDiskQuota(CreateDiskQuota(0));

    auto operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions->Weight = 1.0;
    auto operation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList({}));
    auto operationElement = CreateTestOperationElement(strategyHost.Get(), treeScheduler, operation.Get(), rootElement.Get(), operationOptions);

    std::vector<TJobId> jobIds;
    for (int i = 0; i < 4; ++i) {
        auto jobId = TGuid::Create();
        jobIds.push_back(jobId);
        treeScheduler->OnJobStartedInTest(operationElement.Get(), jobId, jobResources);
    }

    DoFairShareUpdate(strategyHost.Get(), treeScheduler, rootElement);

    EXPECT_EQ(TResourceVector({0.0, 0.4, 0.0, 0.4, 0.0}), operationElement->Attributes().DemandShare);
    EXPECT_EQ(TResourceVector({0.0, 0.4, 0.0, 0.4, 0.0}), operationElement->Attributes().FairShare.Total);

    for (int i = 0; i < 2; ++i) {
        EXPECT_EQ(EJobPreemptionStatus::NonPreemptible, treeScheduler->GetJobPreemptionStatusInTest(operationElement.Get(), jobIds[i]));
    }
    for (int i = 2; i < 4; ++i) {
        EXPECT_EQ(EJobPreemptionStatus::AggressivelyPreemptible, treeScheduler->GetJobPreemptionStatusInTest(operationElement.Get(), jobIds[i]));
    }

    TJobResources newResources;
    newResources.SetCpu(20);
    newResources.SetMemory(20);
    // FairShare is now less than usage and we would start preempting jobs of this operation.
    treeScheduler->ProcessUpdatedJobInTest(operationElement.Get(), jobIds[0], newResources);

    for (int i = 0; i < 1; ++i) {
        EXPECT_EQ(EJobPreemptionStatus::NonPreemptible, treeScheduler->GetJobPreemptionStatusInTest(operationElement.Get(), jobIds[i]));
    }
    for (int i = 1; i < 4; ++i) {
        EXPECT_EQ(EJobPreemptionStatus::AggressivelyPreemptible, treeScheduler->GetJobPreemptionStatusInTest(operationElement.Get(), jobIds[i]));
    }
}

TEST_F(TFairShareTreeJobSchedulerTest, TestConditionalPreemption)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(30);
    nodeResources.SetCpu(30);
    nodeResources.SetMemory(300_MB);
    auto execNode = CreateTestExecNode(nodeResources);

    auto strategyHost = CreateTestStrategyHost(CreateTestExecNodeList(1, nodeResources));
    auto treeSchedulerHost = CreateTestTreeJobSchedulerHost(strategyHost);
    auto treeScheduler = CreateTestTreeScheduler(treeSchedulerHost, strategyHost.Get());

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
    auto blockingOperationElement = CreateTestOperationElement(strategyHost.Get(), treeScheduler, blockingOperation.Get(), blockingPool.Get());
    treeScheduler->OnJobStartedInTest(blockingOperationElement.Get(), TGuid::Create(), jobResources);

    jobResources.SetUserSlots(1);
    jobResources.SetCpu(1);
    jobResources.SetMemory(10_MB);

    auto donorOperation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(5, jobResources));
    auto donorOperationSpec = New<TStrategyOperationSpec>();
    donorOperationSpec->MaxUnpreemptibleRunningJobCount = 0;
    auto donorOperationElement = CreateTestOperationElement(
        strategyHost.Get(),
        treeScheduler,
        donorOperation.Get(),
        guaranteedPool.Get(),
        /*operationOptions*/ nullptr,
        donorOperationSpec);

    auto now = TInstant::Now();

    std::vector<TJobPtr> donorJobs;
    for (int i = 0; i < 15; ++i) {
        auto job = CreateTestJob(TGuid::Create(), donorOperation->GetId(), execNode, now, jobResources);
        donorJobs.push_back(job);
        treeScheduler->OnJobStartedInTest(donorOperationElement.Get(), job->GetId(), job->ResourceLimits());
    }

    auto [starvingOperationElement, starvingOperation] = CreateOperationWithJobs(10, strategyHost.Get(), treeScheduler, guaranteedPool.Get());

    {
        DoFairShareUpdate(strategyHost.Get(), treeScheduler, rootElement, now);

        TResourceVector unit = {1.0, 1.0, 0.0, 1.0, 0.0};
        EXPECT_RV_NEAR(unit / 3.0, blockingPool->Attributes().FairShare.Total);
        EXPECT_RV_NEAR(unit * 2.0 / 3.0, guaranteedPool->Attributes().FairShare.Total);
        EXPECT_NEAR(1.5, blockingPool->PostUpdateAttributes().LocalSatisfactionRatio, 1e-7);
        EXPECT_NEAR(0.75, guaranteedPool->PostUpdateAttributes().LocalSatisfactionRatio, 1e-7);

        EXPECT_RV_NEAR(unit / 3.0, blockingOperationElement->Attributes().FairShare.Total);
        EXPECT_RV_NEAR(unit / 3.0, donorOperationElement->Attributes().FairShare.Total);
        EXPECT_RV_NEAR(unit / 3.0, starvingOperationElement->Attributes().FairShare.Total);
        EXPECT_NEAR(1.5, blockingOperationElement->PostUpdateAttributes().LocalSatisfactionRatio, 1e-7);
        EXPECT_NEAR(1.5, donorOperationElement->PostUpdateAttributes().LocalSatisfactionRatio, 1e-7);
        EXPECT_NEAR(0.0, starvingOperationElement->PostUpdateAttributes().LocalSatisfactionRatio, 1e-7);

        EXPECT_NEAR(0.8, starvingOperationElement->GetEffectiveFairShareStarvationTolerance(), 1e-7);
        EXPECT_NEAR(0.8, guaranteedPool->GetEffectiveFairShareStarvationTolerance(), 1e-7);

        EXPECT_EQ(ESchedulableStatus::BelowFairShare, starvingOperationElement->GetStatus());
        EXPECT_EQ(ESchedulableStatus::BelowFairShare, guaranteedPool->GetStatus());

        EXPECT_EQ(now, starvingOperationElement->PersistentAttributes().BelowFairShareSince);
        EXPECT_EQ(now, guaranteedPool->PersistentAttributes().BelowFairShareSince);
    }

    {
        auto timeout = starvingOperationElement->GetEffectiveFairShareStarvationTimeout() + TDuration::MilliSeconds(100);
        now += timeout;
        DoFairShareUpdate(strategyHost.Get(), treeScheduler, rootElement, now, now - timeout);

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

    auto treeSnapshot = DoFairShareUpdate(strategyHost.Get(), treeScheduler, rootElement);
    auto scheduleJobsContextWithDependencies = PrepareScheduleJobsContext(strategyHost.Get(), treeSnapshot, execNode);
    auto& context = scheduleJobsContextWithDependencies.ScheduleJobsContext;

    context.StartStage(&PreemptiveSchedulingStage_);
    context.PrepareForScheduling();

    for (int jobIndex = 0; jobIndex < 10; ++jobIndex) {
        EXPECT_NE(EJobPreemptionStatus::Preemptible, treeScheduler->GetJobPreemptionStatusInTest(donorOperationElement.Get(), donorJobs[jobIndex]->GetId()));
    }

    auto targetOperationPreemptionPriority = EOperationPreemptionPriority::Regular;
    EXPECT_EQ(guaranteedPool.Get(), context.FindPreemptionBlockingAncestor(donorOperationElement.Get(), targetOperationPreemptionPriority));
    for (int jobIndex = 10; jobIndex < 15; ++jobIndex) {
        const auto& job = donorJobs[jobIndex];
        auto preemptionStatus = treeScheduler->GetJobPreemptionStatusInTest(donorOperationElement.Get(), job->GetId());
        EXPECT_EQ(EJobPreemptionStatus::Preemptible, preemptionStatus);
        context.ConditionallyPreemptibleJobSetMap()[guaranteedPool->GetTreeIndex()].insert(TJobWithPreemptionInfo{
            .Job = job,
            .PreemptionStatus = preemptionStatus,
            .OperationElement = donorOperationElement.Get(),
        });
    }

    {
        TScheduleJobsContext::TPrepareConditionalUsageDiscountsContext prepareConditionalUsageDiscountsContext{
            .TargetOperationPreemptionPriority = targetOperationPreemptionPriority,
        };
        context.PrepareConditionalUsageDiscounts(rootElement.Get(), &prepareConditionalUsageDiscountsContext);
    }

    auto jobs = context.GetConditionallyPreemptibleJobsInPool(guaranteedPool.Get());
    EXPECT_EQ(5, std::ssize(jobs));
    for (int jobIndex = 10; jobIndex < 15; ++jobIndex) {
        const auto& job = donorJobs[jobIndex];
        EXPECT_TRUE(jobs.contains(TJobWithPreemptionInfo{
            .Job = job,
            .PreemptionStatus = treeScheduler->GetJobPreemptionStatusInTest(donorOperationElement.Get(), job->GetId()),
            .OperationElement = donorOperationElement.Get(),
        }));
    }

    EXPECT_TRUE(context.GetConditionallyPreemptibleJobsInPool(blockingPool.Get()).empty());
    EXPECT_TRUE(context.GetConditionallyPreemptibleJobsInPool(rootElement.Get()).empty());

    TJobResources expectedDiscount;
    expectedDiscount.SetUserSlots(5);
    expectedDiscount.SetCpu(5);
    expectedDiscount.SetMemory(50_MB);

    const auto& schedulingContext = scheduleJobsContextWithDependencies.SchedulingContext;
    EXPECT_EQ(expectedDiscount, schedulingContext->GetMaxConditionalUsageDiscount());
    EXPECT_EQ(expectedDiscount, schedulingContext->GetConditionalDiscountForOperation(starvingOperation->GetId()));
    // It's a bit weird that a preemptible job's usage is added to the discount of its operation, but this is how we do it.
    EXPECT_EQ(expectedDiscount, schedulingContext->GetConditionalDiscountForOperation(donorOperation->GetId()));
    EXPECT_EQ(TJobResources(), schedulingContext->GetConditionalDiscountForOperation(blockingOperation->GetId()));
}

TEST_F(TFairShareTreeJobSchedulerTest, TestChildHeap)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(100);
    nodeResources.SetDiskQuota(CreateDiskQuota(100));
    auto execNode = CreateTestExecNode(nodeResources);

    auto strategyHost = CreateTestStrategyHost(CreateTestExecNodeList(1, nodeResources));
    auto treeSchedulerHost = CreateTestTreeJobSchedulerHost(strategyHost);
    auto treeScheduler = CreateTestTreeScheduler(treeSchedulerHost, strategyHost.Get());

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
            treeScheduler,
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
                result->StartDescriptor.emplace(TGuid::Create(), operationJobResources, /*interruptible*/ false);
                return MakeFuture<TControllerScheduleJobResultPtr>(
                    TErrorOr<TControllerScheduleJobResultPtr>(result));
            }));
    }

    auto treeSnapshot = DoFairShareUpdate(strategyHost.Get(), treeScheduler, rootElement);
    auto scheduleJobsContextWithDependencies = PrepareScheduleJobsContext(strategyHost.Get(), treeSnapshot, execNode);
    auto& context = scheduleJobsContextWithDependencies.ScheduleJobsContext;

    context.StartStage(&NonPreemptiveSchedulingStage_);
    context.PrepareForScheduling();
    context.PrescheduleJob();

    for (auto operationElement : operationElements) {
        const auto& dynamicAttributes = context.DynamicAttributesOf(rootElement.Get());
        ASSERT_TRUE(dynamicAttributes.Active);
    }

    for (int iter = 0; iter < 2; ++iter) {
        for (auto operationElement : operationElements) {
            auto scheduleJobResult = context.ScheduleJob(operationElement.Get(), /*ignorePacking*/ true);
            EXPECT_TRUE(scheduleJobResult.Scheduled);

            const auto& childHeapMap = context.GetChildHeapMapInTest();
            YT_VERIFY(childHeapMap.contains(rootElement->GetTreeIndex()));

            const auto& childHeap = GetOrCrash(childHeapMap, rootElement->GetTreeIndex());

            int heapIndex = 0;
            for (auto* element : childHeap.GetHeap()) {
                EXPECT_EQ(context.DynamicAttributesOf(element).HeapIndex, heapIndex);
                ++heapIndex;
            }
        }
    }
    context.FinishStage();

    // NB(eshcherbin): It is impossible to have two consecutive non-preemptive scheduling stages, however
    // here we only need to trigger the second PrescheduleJob call so that the child heap is rebuilt.
    context.StartStage(&NonPreemptiveSchedulingStage_);
    context.PrepareForScheduling();
    context.PrescheduleJob();

    for (auto operationElement : operationElements) {
        const auto& childHeapMap = context.GetChildHeapMapInTest();
        YT_VERIFY(childHeapMap.contains(rootElement->GetTreeIndex()));

        const auto& childHeap = GetOrCrash(childHeapMap, rootElement->GetTreeIndex());
        int heapIndex = 0;
        for (auto* element : childHeap.GetHeap()) {
            EXPECT_EQ(context.DynamicAttributesOf(element).HeapIndex, heapIndex);
            ++heapIndex;
        }
    }
}

TEST_F(TFairShareTreeJobSchedulerTest, TestBuildDynamicAttributesListFromSnapshot)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(100);
    nodeResources.SetDiskQuota(CreateDiskQuota(100));
    auto execNode = CreateTestExecNode(nodeResources);

    auto strategyHost = CreateTestStrategyHost(CreateTestExecNodeList(1, nodeResources));
    auto treeSchedulerHost = CreateTestTreeJobSchedulerHost(strategyHost);
    auto treeScheduler = CreateTestTreeScheduler(treeSchedulerHost, strategyHost.Get());

    // Pools.
    auto rootElement = CreateTestRootElement(strategyHost.Get());
    auto pool = CreateTestPool(strategyHost.Get(), "pool", CreateSimplePoolConfig());

    pool->AttachParent(rootElement.Get());

    // 1/10 of all resources.
    TJobResourcesWithQuota jobResources;
    jobResources.SetCpu(10);
    jobResources.SetMemory(10);
    jobResources.SetDiskQuota(CreateDiskQuota(0));

    // Operations.
    auto operationA = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList());
    auto operationElementA = CreateTestOperationElement(strategyHost.Get(), treeScheduler, operationA.Get(), pool.Get());

    auto operationB = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(1, jobResources));
    auto operationElementB = CreateTestOperationElement(strategyHost.Get(), treeScheduler, operationB.Get(), pool.Get());

    // Check function.
    struct TUsageWithSatisfactions
    {
        TJobResources ResourceUsage;
        double LocalSatisfactionRatio = 0.0;
    };

    auto checkDynamicAttributes = [&] (
        const TUsageWithSatisfactions& expectedPool,
        const TUsageWithSatisfactions& expectedOperationA,
        const TUsageWithSatisfactions& expectedOperationB,
        TFairShareTreeSnapshotPtr treeSnapshot = {},
        const TResourceUsageSnapshotPtr& resourceUsageSnapshot = {})
    {
        if (!treeSnapshot) {
            treeSnapshot = DoFairShareUpdate(strategyHost.Get(), treeScheduler, rootElement);
        }

        auto now = GetCpuInstant();
        auto dynamicAttributesList = TDynamicAttributesManager::BuildDynamicAttributesListFromSnapshot(
            treeSnapshot,
            resourceUsageSnapshot,
            now);

        const auto& poolAttributes = dynamicAttributesList.AttributesOf(pool.Get());
        EXPECT_EQ(expectedPool.ResourceUsage, poolAttributes.ResourceUsage);
        EXPECT_NEAR(expectedPool.LocalSatisfactionRatio, poolAttributes.LocalSatisfactionRatio, 1e-7);
        EXPECT_EQ(TCpuInstant(), poolAttributes.ResourceUsageUpdateTime);

        auto expectedOperationUsageUpdateTime = resourceUsageSnapshot
            ? resourceUsageSnapshot->BuildTime
            : now;

        const auto& operationAttributesA = dynamicAttributesList.AttributesOf(operationElementA.Get());
        EXPECT_EQ(expectedOperationA.ResourceUsage, operationAttributesA.ResourceUsage);
        EXPECT_NEAR(expectedOperationA.LocalSatisfactionRatio, operationAttributesA.LocalSatisfactionRatio, 1e-7);
        EXPECT_EQ(expectedOperationUsageUpdateTime, operationAttributesA.ResourceUsageUpdateTime);

        const auto& operationAttributesB = dynamicAttributesList.AttributesOf(operationElementB.Get());
        EXPECT_EQ(expectedOperationB.ResourceUsage, operationAttributesB.ResourceUsage);
        EXPECT_NEAR(expectedOperationB.LocalSatisfactionRatio, operationAttributesB.LocalSatisfactionRatio, 1e-7);
        EXPECT_EQ(expectedOperationUsageUpdateTime, operationAttributesB.ResourceUsageUpdateTime);
    };

    // First case: no usage.
    checkDynamicAttributes(
        TUsageWithSatisfactions{
            .ResourceUsage = TJobResources(),
            .LocalSatisfactionRatio = 0.0,
        },
        TUsageWithSatisfactions{
            .ResourceUsage = TJobResources(),
            .LocalSatisfactionRatio = InfiniteSatisfactionRatio,
        },
        TUsageWithSatisfactions{
            .ResourceUsage = TJobResources(),
            .LocalSatisfactionRatio = 0.0,
        });

    // Second case: one operation has a job.
    treeScheduler->OnJobStartedInTest(operationElementB.Get(), TJobId::Create(), jobResources);

    checkDynamicAttributes(
        TUsageWithSatisfactions{
            .ResourceUsage = jobResources,
            .LocalSatisfactionRatio = 0.5,
        },
        TUsageWithSatisfactions{
            .ResourceUsage = TJobResources(),
            .LocalSatisfactionRatio = InfiniteSatisfactionRatio,
        },
        TUsageWithSatisfactions{
            .ResourceUsage = jobResources,
            .LocalSatisfactionRatio = 0.5,
        });

    // Third case: with and without usage snapshot.
    treeScheduler->OnJobStartedInTest(operationElementA.Get(), TJobId::Create(), jobResources);

    auto treeSnapshot = DoFairShareUpdate(strategyHost.Get(), treeScheduler, rootElement);
    auto resourceUsageSnapshot = BuildResourceUsageSnapshot(treeSnapshot);

    treeScheduler->OnJobStartedInTest(operationElementA.Get(), TJobId::Create(), jobResources);

    checkDynamicAttributes(
        TUsageWithSatisfactions{
            .ResourceUsage = jobResources * 3.0,
            .LocalSatisfactionRatio = 1.0,
        },
        TUsageWithSatisfactions{
            .ResourceUsage = jobResources * 2.0,
            .LocalSatisfactionRatio = 2.0,
        },
        TUsageWithSatisfactions{
            .ResourceUsage = jobResources,
            .LocalSatisfactionRatio = 0.5,
        },
        treeSnapshot);

    checkDynamicAttributes(
        TUsageWithSatisfactions{
            .ResourceUsage = jobResources * 2.0,
            .LocalSatisfactionRatio = 2.0 / 3.0,
        },
        TUsageWithSatisfactions{
            .ResourceUsage = jobResources,
            .LocalSatisfactionRatio = 1.0,
        },
        TUsageWithSatisfactions{
            .ResourceUsage = jobResources,
            .LocalSatisfactionRatio = 0.5,
        },
        treeSnapshot,
        resourceUsageSnapshot);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NScheduler
