#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/scheduler/fair_share_tree.h>
#include <yt/yt/server/scheduler/fair_share_tree_element.h>
#include <yt/yt/server/scheduler/fair_share_tree_allocation_scheduler.h>
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

// NB(eshcherbin): Set to true, when in pain.
static constexpr bool EnableDebugLogging = false;
static const NLogging::TLogger Logger = EnableDebugLogging
    ? NLogging::TLogger("TestDebug")
    : NLogging::TLogger();

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

        for (const auto& node : ExecNodes_) {
            NodeToState_.emplace(node, TFairShareTreeJobSchedulerNodeState{});
        }
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

    TFairShareTreeJobSchedulerNodeState* GetNodeState(const TExecNodePtr node)
    {
        return &GetOrCrash(NodeToState_, node);
    }

private:
    std::vector<IInvokerPtr> NodeShardInvokers_;
    std::vector<TExecNodePtr> ExecNodes_;
    THashMap<TExecNodePtr, TFairShareTreeJobSchedulerNodeState> NodeToState_;
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

    const re2::RE2& GetEphemeralPoolNameRegex() const override
    {
        return RegexStub_;
    }

private:
    re2::RE2 RegexStub_ = re2::RE2(".*");
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

    TJobResourcesWithQuotaList GetInitialMinNeededJobResources() const override
    {
        return GetMinNeededJobResources();
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
    void SetUp() override
    {
        auto minSpareJobResources = New<TJobResourcesConfig>();
        minSpareJobResources->UserSlots = 1;
        minSpareJobResources->Cpu = 1.0;
        minSpareJobResources->Memory = 1;

        SchedulerConfig_->MinSpareJobResourcesOnNode = minSpareJobResources;

        TreeConfig_->AggressivePreemptionSatisfactionThreshold = 0.5;
        TreeConfig_->MinChildHeapSize = 3;
        TreeConfig_->EnableConditionalPreemption = true;
        TreeConfig_->UseResourceUsageWithPrecommit = false;
        TreeConfig_->ShouldDistributeFreeVolumeAmongChildren = true;

        TreeConfig_->BatchOperationScheduling = New<TBatchOperationSchedulingConfig>();
        TreeConfig_->BatchOperationScheduling->BatchSize = 3;
    }

protected:
    TSchedulerConfigPtr SchedulerConfig_ = New<TSchedulerConfig>();
    TFairShareTreeHostMockPtr FairShareTreeHostMock_ = New<TFairShareTreeHostMock>();
    TFairShareStrategyTreeConfigPtr TreeConfig_ = New<TFairShareStrategyTreeConfig>();
    TFairShareTreeElementHostMockPtr FairShareTreeElementHostMock_ = New<TFairShareTreeElementHostMock>(TreeConfig_);
    NConcurrency::TActionQueuePtr NodeShardActionQueue_ = New<NConcurrency::TActionQueue>("NodeShard");

    TSchedulingStageProfilingCounters RegularSchedulingProfilingCounters_{NProfiling::TProfiler("/regular_test_scheduling_stage")};
    TSchedulingStageProfilingCounters PreemptiveSchedulingProfilingCounters_{NProfiling::TProfiler("/preemptive_test_scheduling_stage")};

    int SlotIndex_ = 0;
    NNodeTrackerClient::TNodeId ExecNodeId_ = NNodeTrackerClient::TNodeId(0);

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
        diskResources.mutable_disk_location_resources(0)->set_limit(GetOrDefault(nodeResources.DiskQuota().DiskSpacePerMedium, NChunkClient::DefaultSlotsMediumIndex));

        auto nodeId = ExecNodeId_;
        ExecNodeId_ = NNodeTrackerClient::TNodeId(nodeId.Underlying() + 1);
        auto execNode = New<TExecNode>(nodeId, NNodeTrackerClient::TNodeDescriptor(), ENodeState::Online);
        execNode->SetResourceLimits(nodeResources.ToJobResources());
        execNode->SetDiskResources(diskResources);

        execNode->SetTags(std::move(tags));

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
        TScheduleJobsContextPtr ScheduleJobsContext;
    };

    TFairShareTreeSnapshotPtr DoFairShareUpdate(
        const ISchedulerStrategyHost* strategyHost,
        const TFairShareTreeJobSchedulerPtr& treeScheduler,
        const TSchedulerRootElementPtr& rootElement,
        TInstant now = TInstant(),
        std::optional<TInstant> previousUpdateTime = {})
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

        // Resource usage and limits and node count are only used for diagnostics, so we don't provide them here.
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
            /*nodeCount*/ 0,
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

        auto scheduleJobsContext = New<TScheduleJobsContext>(
            schedulingContext,
            treeSnapshot,
            strategyHost->GetNodeState(execNode),
            /*schedulingInfoLoggingEnabled*/ true,
            strategyHost,
            /*scheduleJobsDeadlineReachedCounter*/ NProfiling::TCounter{},
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
        auto context = scheduleJobsContextWithDependencies.ScheduleJobsContext;

        context->StartStage(EJobSchedulingStage::RegularMediumPriority, &RegularSchedulingProfilingCounters_);

        context->PrepareForScheduling();
        context->PrescheduleJob();
        context->ScheduleJobInTest(operationElement.Get(), /*ignorePacking*/ true);

        context->FinishStage();
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
        auto jobId = TJobId(TGuid::Create());
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
    nodeResources.DiskQuota() = CreateDiskQuota(100);

    std::vector<TExecNodePtr> execNodes(3);
    for (int i = 0; i < std::ssize(execNodes); ++i) {
        execNodes[i] = CreateTestExecNode(nodeResources);
    }

    auto strategyHost = CreateTestStrategyHost(execNodes);
    auto treeSchedulerHost = CreateTestTreeJobSchedulerHost(strategyHost);
    auto treeScheduler = CreateTestTreeScheduler(treeSchedulerHost, strategyHost.Get());

    auto rootElement = CreateTestRootElement(strategyHost.Get());

    // Create an operation with 2 jobs.
    TJobResourcesWithQuota operationJobResources;
    operationJobResources.SetCpu(10);
    operationJobResources.SetMemory(10);
    operationJobResources.DiskQuota() = CreateDiskQuota(0);

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
    nodeResources.DiskQuota() = CreateDiskQuota(100);

    auto execNode = CreateTestExecNode(nodeResources);

    auto strategyHost = CreateTestStrategyHost({execNode});
    auto treeSchedulerHost = CreateTestTreeJobSchedulerHost(strategyHost);
    auto treeScheduler = CreateTestTreeScheduler(treeSchedulerHost, strategyHost.Get());

    auto rootElement = CreateTestRootElement(strategyHost.Get());

    // Create an operation with 4 jobs.
    TJobResourcesWithQuota jobResources;
    jobResources.SetCpu(10);
    jobResources.SetMemory(10);
    jobResources.DiskQuota() = CreateDiskQuota(0);

    auto operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions->Weight = 1.0;
    auto operation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList({}));
    auto operationElement = CreateTestOperationElement(strategyHost.Get(), treeScheduler, operation.Get(), rootElement.Get(), operationOptions);

    std::vector<TJobId> jobIds;
    for (int i = 0; i < 4; ++i) {
        auto jobId = TJobId(TGuid::Create());
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

    auto strategyHost = CreateTestStrategyHost({execNode});
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
    treeScheduler->OnJobStartedInTest(blockingOperationElement.Get(), TJobId(TGuid::Create()), jobResources);

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
        auto job = CreateTestJob(TJobId(TGuid::Create()), donorOperation->GetId(), execNode, now, jobResources);
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
    auto context = scheduleJobsContextWithDependencies.ScheduleJobsContext;

    context->StartStage(EJobSchedulingStage::PreemptiveNormal, &PreemptiveSchedulingProfilingCounters_);
    context->PrepareForScheduling();

    for (int jobIndex = 0; jobIndex < 10; ++jobIndex) {
        EXPECT_NE(EJobPreemptionStatus::Preemptible, treeScheduler->GetJobPreemptionStatusInTest(donorOperationElement.Get(), donorJobs[jobIndex]->GetId()));
    }

    auto targetOperationPreemptionPriority = EOperationPreemptionPriority::Normal;
    EXPECT_EQ(guaranteedPool.Get(), context->FindPreemptionBlockingAncestor(donorOperationElement.Get(), EJobPreemptionLevel::Preemptible, targetOperationPreemptionPriority));
    for (int jobIndex = 10; jobIndex < 15; ++jobIndex) {
        const auto& job = donorJobs[jobIndex];
        auto preemptionStatus = treeScheduler->GetJobPreemptionStatusInTest(donorOperationElement.Get(), job->GetId());
        EXPECT_EQ(EJobPreemptionStatus::Preemptible, preemptionStatus);
        context->ConditionallyPreemptibleJobSetMap()[guaranteedPool->GetTreeIndex()].insert(TJobWithPreemptionInfo{
            .Job = job,
            .PreemptionStatus = preemptionStatus,
            .OperationElement = donorOperationElement.Get(),
        });
    }

    {
        TScheduleJobsContext::TPrepareConditionalUsageDiscountsContext prepareConditionalUsageDiscountsContext{
            .TargetOperationPreemptionPriority = targetOperationPreemptionPriority,
        };
        context->PrepareConditionalUsageDiscounts(rootElement.Get(), &prepareConditionalUsageDiscountsContext);
    }

    auto jobs = context->GetConditionallyPreemptibleJobsInPool(guaranteedPool.Get());
    EXPECT_EQ(5, std::ssize(jobs));
    for (int jobIndex = 10; jobIndex < 15; ++jobIndex) {
        const auto& job = donorJobs[jobIndex];
        EXPECT_TRUE(jobs.contains(TJobWithPreemptionInfo{
            .Job = job,
            .PreemptionStatus = treeScheduler->GetJobPreemptionStatusInTest(donorOperationElement.Get(), job->GetId()),
            .OperationElement = donorOperationElement.Get(),
        }));
    }

    EXPECT_TRUE(context->GetConditionallyPreemptibleJobsInPool(blockingPool.Get()).empty());
    EXPECT_TRUE(context->GetConditionallyPreemptibleJobsInPool(rootElement.Get()).empty());

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

TEST_F(TFairShareTreeJobSchedulerTest, TestSchedulableOperationsOrder)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(100);
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(100);
    nodeResources.DiskQuota() = CreateDiskQuota(100);
    auto execNode = CreateTestExecNode(nodeResources);

    auto strategyHost = CreateTestStrategyHost({execNode});
    auto treeSchedulerHost = CreateTestTreeJobSchedulerHost(strategyHost);
    auto treeScheduler = CreateTestTreeScheduler(treeSchedulerHost, strategyHost.Get());

    // Root element.
    auto rootElement = CreateTestRootElement(strategyHost.Get());

    // Pool.
    auto pool = CreateTestPool(strategyHost.Get(), "pool");
    pool->AttachParent(rootElement.Get());

    TJobResourcesWithQuota operationJobResources;
    operationJobResources.SetUserSlots(1);
    operationJobResources.SetCpu(1);
    operationJobResources.SetMemory(1);
    operationJobResources.DiskQuota() = CreateDiskQuota(0);

    // For both pools create 10 operations, each with 1 demanded job.
    constexpr int OperationCount = 10;
    static const std::vector<int> ExpectedOperationIndicesFifo{3, 5, 6, 7, 0, 1, 8, 4, 9, 2};
    static const std::vector<int> ExpectedOperationIndicesFairShare{7, 0, 8, 1, 5, 2, 9, 4, 6, 3};
    std::vector<TOperationStrategyHostMockPtr> operations;
    std::vector<TSchedulerOperationElementPtr> operationElements;
    TNonOwningOperationElementList nonOwningOperationElements;
    for (int opIndex = 0; opIndex < OperationCount; ++opIndex) {
        auto operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
        operationOptions->Weight = static_cast<double>(OperationCount - ExpectedOperationIndicesFifo[opIndex]);

        operations.push_back(New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(1, operationJobResources)));
        operationElements.push_back(CreateTestOperationElement(
            strategyHost.Get(),
            treeScheduler,
            operations.back().Get(),
            pool.Get(),
            std::move(operationOptions)));
        nonOwningOperationElements.push_back(operationElements.back().Get());
    }

    for (int opIndex = 0; opIndex < OperationCount; ++opIndex) {
        const int jobCount = ExpectedOperationIndicesFairShare[opIndex];
        for (int jobIndex = 0; jobIndex < jobCount; ++jobIndex) {
            treeScheduler->OnJobStartedInTest(operationElements[opIndex].Get(), TJobId(TGuid::Create()), operationJobResources);
        }
    }

    auto checkOrder = [&] (
        const std::optional<TNonOwningOperationElementList>& consideredOperations,
        const std::vector<int>& expectedOperationIndices) {
        // Here we check operations order three times using different methods.
        auto treeSnapshot = DoFairShareUpdate(strategyHost.Get(), treeScheduler, rootElement);

        // First, we start with the scheduling indices, which are computed during post update.
        for (int opIndex = 0; opIndex < OperationCount; ++opIndex) {
            const auto& element = operationElements[opIndex];

            YT_LOG_INFO("Checking operation index (ExpectedIndex: %v, ActualIndex: %v, Weight: %v)",
                expectedOperationIndices[opIndex],
                treeSnapshot->SchedulingSnapshot()->StaticAttributesList().AttributesOf(element.Get()).SchedulingIndex,
                element->GetWeight());

            EXPECT_EQ(
                expectedOperationIndices[opIndex],
                treeSnapshot->SchedulingSnapshot()->StaticAttributesList().AttributesOf(element.Get()).SchedulingIndex);
        }

        auto doCheckOrderDuringSchedulingStage = [&] (auto getBestOperation) {
            auto scheduleJobsContextWithDependencies = PrepareScheduleJobsContext(strategyHost.Get(), treeSnapshot, execNode);
            auto context = scheduleJobsContextWithDependencies.ScheduleJobsContext;
            context->StartStage(EJobSchedulingStage::RegularMediumPriority, &RegularSchedulingProfilingCounters_);
            context->PrepareForScheduling();
            context->PrescheduleJob(consideredOperations);
            auto finally = Finally([&] {
                context->FinishStage();
            });

            THashMap<TSchedulerElement*, int> operationToIndex;
            for (int opIndex = 0; opIndex < OperationCount; ++opIndex) {
                auto* element = getBestOperation(context->DynamicAttributesOf(pool.Get()));

                ASSERT_TRUE(element);

                EmplaceOrCrash(
                    operationToIndex,
                    element,
                    opIndex);

                context->DeactivateOperationInTest(static_cast<TSchedulerOperationElement*>(element));
            }

            for (int opIndex = 0; opIndex < OperationCount; ++opIndex) {
                const auto& element = operationElements[opIndex];

                YT_LOG_INFO("Checking operation index (ExpectedIndex: %v, ActualIndex: %v, Weight: %v, Satisfaction: %v)",
                    expectedOperationIndices[opIndex],
                    operationToIndex[element.Get()],
                    element->GetWeight(),
                    context->DynamicAttributesOf(element.Get()).LocalSatisfactionRatio);

                EXPECT_EQ(expectedOperationIndices[opIndex], operationToIndex[element.Get()]);
            }
        };

        // Second, we check the order given by getting the best leaf descendant and deactivating it
        // until no active operation remains.
        YT_LOG_INFO("Best leaf descendant");

        doCheckOrderDuringSchedulingStage([] (const TDynamicAttributes& attributes) {
            return attributes.BestLeafDescendant;
        });

        // Third, we check the order inside schedulable children set.
        if (consideredOperations) {
            YT_LOG_INFO("Schedulable children set");

            doCheckOrderDuringSchedulingStage([] (const TDynamicAttributes& attributes) {
                auto& childSet = attributes.SchedulableChildSet;
                return childSet->GetBestActiveChild();
            });
        }
    };

    auto fifoPoolConfig = New<TPoolConfig>();
    fifoPoolConfig->Mode = ESchedulingMode::Fifo;
    fifoPoolConfig->FifoSortParameters = {EFifoSortParameter::Weight};
    fifoPoolConfig->FifoPoolSchedulingOrder = EFifoPoolSchedulingOrder::Fifo;

    auto fifoPoolWithSatisfactionOrderConfig = NYTree::CloneYsonStruct(fifoPoolConfig);
    fifoPoolWithSatisfactionOrderConfig->FifoPoolSchedulingOrder = EFifoPoolSchedulingOrder::Satisfaction;

    for (const auto& poolConfig : {New<TPoolConfig>(), fifoPoolConfig, fifoPoolWithSatisfactionOrderConfig}) {
        pool->SetConfig(poolConfig);

        for (int minChildHeapSize : {3, 100}) {
            TreeConfig_->MinChildHeapSize = minChildHeapSize;

            for (const auto& consideredOperations : {{}, std::make_optional(nonOwningOperationElements)}) {
                YT_LOG_INFO(
                    "Testing schedulable operations order "
                    "(PoolMode: %v, FifoPoolSchedulingOrder: %v, MinChildHeapSize: %v, UseConsideredOperations: %v)",
                    pool->GetConfig()->Mode,
                    pool->GetConfig()->FifoPoolSchedulingOrder,
                    TreeConfig_->MinChildHeapSize,
                    consideredOperations.has_value());

                bool shouldUseFifoOrder = pool->GetConfig()->Mode == ESchedulingMode::Fifo &&
                    pool->GetConfig()->FifoPoolSchedulingOrder == EFifoPoolSchedulingOrder::Fifo;
                checkOrder(
                    consideredOperations,
                    shouldUseFifoOrder
                        ? ExpectedOperationIndicesFifo
                        : ExpectedOperationIndicesFairShare);
            }
        }
    }
}

TEST_F(TFairShareTreeJobSchedulerTest, TestSchedulableChildSetWithBatchScheduling)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(100);
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(100);
    nodeResources.DiskQuota() = CreateDiskQuota(100);
    auto execNode = CreateTestExecNode(nodeResources);

    auto strategyHost = CreateTestStrategyHost({execNode});
    auto treeSchedulerHost = CreateTestTreeJobSchedulerHost(strategyHost);
    auto treeScheduler = CreateTestTreeScheduler(treeSchedulerHost, strategyHost.Get());

    // Root element.
    auto rootElement = CreateTestRootElement(strategyHost.Get());

    // 1/10 of all resources.
    TJobResourcesWithQuota operationJobResources;
    operationJobResources.SetUserSlots(1);
    operationJobResources.SetCpu(10);
    operationJobResources.SetMemory(10);
    operationJobResources.DiskQuota() = CreateDiskQuota(0);

    // Create 5 operations, each with 2 jobs.
    constexpr int OperationCount = 5;
    std::vector<TOperationStrategyHostMockPtr> operations(OperationCount);
    std::vector<TSchedulerOperationElementPtr> operationElements(OperationCount);
    for (int opIndex = 0; opIndex < OperationCount; ++opIndex) {
        operations[opIndex] = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(2, operationJobResources));
        operationElements[opIndex] = CreateTestOperationElement(
            strategyHost.Get(),
            treeScheduler,
            operations[opIndex].Get(),
            rootElement.Get());
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
                result->StartDescriptor.emplace(TJobId(TGuid::Create()), operationJobResources, /*interruptible*/ false);
                return MakeFuture<TControllerScheduleJobResultPtr>(
                    TErrorOr<TControllerScheduleJobResultPtr>(result));
            }));
    }

    auto checkRootChildSet = [rootElement = rootElement.Get()] (
        const TScheduleJobsContextPtr& context,
        int expectedChildCount,
        bool expectedUsesHeap)
    {
        const auto& childSet = context->DynamicAttributesOf(rootElement).SchedulableChildSet;

        ASSERT_TRUE(childSet);
        EXPECT_EQ(expectedChildCount, std::ssize(childSet->GetChildren()));
        EXPECT_EQ(expectedUsesHeap, childSet->UsesHeapInTest());

        int childIndex = 0;
        for (auto* element : childSet->GetChildren()) {
            EXPECT_EQ(context->DynamicAttributesOf(element).SchedulableChildSetIndex, childIndex);
            ++childIndex;
        }
    };

    // With heap.

    const int FirstBatchOperationCount = TreeConfig_->BatchOperationScheduling->BatchSize;

    {
        auto treeSnapshot = DoFairShareUpdate(strategyHost.Get(), treeScheduler, rootElement);
        auto scheduleJobsContextWithDependencies = PrepareScheduleJobsContext(strategyHost.Get(), treeSnapshot, execNode);
        auto context = scheduleJobsContextWithDependencies.ScheduleJobsContext;

        auto sortedOperationElements = operationElements;
        SortBy(sortedOperationElements, [&] (const TSchedulerOperationElementPtr& element) {
            return treeSnapshot->SchedulingSnapshot()->StaticAttributesList().AttributesOf(element.Get()).SchedulingIndex;
        });

        const auto& schedulableOperations = treeSnapshot->SchedulingSnapshot()->SchedulableOperationsPerPriority()[EOperationSchedulingPriority::Medium];
        ASSERT_EQ(OperationCount, std::ssize(schedulableOperations));

        {
            // First batch.
            TNonOwningOperationElementList operationBatch(
                schedulableOperations.begin(),
                schedulableOperations.begin() + FirstBatchOperationCount);

            context->StartStage(
                EJobSchedulingStage::RegularMediumPriority,
                &RegularSchedulingProfilingCounters_,
                /*stageAttemptIndex*/ 0);
            context->PrepareForScheduling();
            context->PrescheduleJob(operationBatch);

            for (int i = 0; i < FirstBatchOperationCount; ++i) {
                const auto& operationElement = sortedOperationElements[i];
                EXPECT_EQ(schedulableOperations[i], operationElement.Get());

                const auto& dynamicAttributes = context->DynamicAttributesOf(operationElement.Get());
                ASSERT_TRUE(dynamicAttributes.Active);
            }
            for (int i = FirstBatchOperationCount; i < OperationCount; ++i) {
                const auto& operationElement = sortedOperationElements[i];
                EXPECT_EQ(schedulableOperations[i], operationElement.Get());

                const auto& dynamicAttributes = context->DynamicAttributesOf(operationElement.Get());
                ASSERT_FALSE(dynamicAttributes.Active);
            }

            for (int iter = 0; iter < 2; ++iter) {
                for (int i = 0; i < FirstBatchOperationCount; ++i) {
                    EXPECT_TRUE(context->SchedulingContext()->CanStartMoreJobs());

                    const auto& operationElement = sortedOperationElements[i];
                    bool scheduled = context->ScheduleJobInTest(operationElement.Get(), /*ignorePacking*/ true);
                    EXPECT_TRUE(scheduled);

                    checkRootChildSet(context, /*expectedChildCount*/ 3, /*expectedUsesHeap*/ true);
                }
            }

            context->FinishStage();
        }

        {
            // Second batch.
            TNonOwningOperationElementList operationBatch(
                schedulableOperations.begin() + FirstBatchOperationCount,
                schedulableOperations.end());

            context->StartStage(
                EJobSchedulingStage::RegularMediumPriority,
                &RegularSchedulingProfilingCounters_,
                /*stageAttemptIndex*/ 1);
            context->PrepareForScheduling();
            context->PrescheduleJob(operationBatch);

            for (int i = 0; i < FirstBatchOperationCount; ++i) {
                const auto& operationElement = sortedOperationElements[i];
                EXPECT_EQ(schedulableOperations[i], operationElement.Get());

                const auto& dynamicAttributes = context->DynamicAttributesOf(operationElement.Get());
                ASSERT_FALSE(dynamicAttributes.Active);
            }
            for (int i = FirstBatchOperationCount; i < OperationCount; ++i) {
                const auto& operationElement = sortedOperationElements[i];
                EXPECT_EQ(schedulableOperations[i], operationElement.Get());

                const auto& dynamicAttributes = context->DynamicAttributesOf(operationElement.Get());
                ASSERT_TRUE(dynamicAttributes.Active);
            }

            TJobResources FallbackMinSpareResources;
            FallbackMinSpareResources.SetCpu(50.0);

            for (int iter = 0; iter < 2; ++iter) {
                for (int i = FirstBatchOperationCount; i < OperationCount; ++i) {
                    EXPECT_FALSE(context->SchedulingContext()->CanStartMoreJobs(FallbackMinSpareResources));
                    EXPECT_TRUE(context->SchedulingContext()->CanStartMoreJobs());

                    const auto& operationElement = sortedOperationElements[i];
                    bool scheduled = context->ScheduleJobInTest(operationElement.Get(), /*ignorePacking*/ true);
                    EXPECT_TRUE(scheduled);

                    checkRootChildSet(context, /*expectedChildCount*/ 2, /*expectedUsesHeap*/ false);
                }
            }

            context->FinishStage();
        }
    }

    // Without heap.

    constexpr int NewOperationCount = 2;
    while (std::ssize(operations) > NewOperationCount) {
        const auto& operationElement = operationElements.back();
        operationElement->DetachParent();

        operationElements.pop_back();
        operations.pop_back();
    }

    {
        auto treeSnapshot = DoFairShareUpdate(strategyHost.Get(), treeScheduler, rootElement);
        auto scheduleJobsContextWithDependencies = PrepareScheduleJobsContext(strategyHost.Get(), treeSnapshot, execNode);
        auto context = scheduleJobsContextWithDependencies.ScheduleJobsContext;

        const auto& schedulableOperations = treeSnapshot->SchedulingSnapshot()->SchedulableOperationsPerPriority()[EOperationSchedulingPriority::Medium];
        ASSERT_EQ(NewOperationCount, std::ssize(schedulableOperations));

        {
            context->StartStage(EJobSchedulingStage::RegularMediumPriority, &RegularSchedulingProfilingCounters_);
            context->PrepareForScheduling();
            context->PrescheduleJob(schedulableOperations);

            checkRootChildSet(context, /*expectedChildCount*/ 2, /*expectedUsesHeap*/ false);

            context->FinishStage();
        }
    }
}

TEST_F(TFairShareTreeJobSchedulerTest, TestSchedulableChildSetWithoutBatchScheduling)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetUserSlots(100);
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(100);
    nodeResources.DiskQuota() = CreateDiskQuota(100);
    auto execNode = CreateTestExecNode(nodeResources);

    auto strategyHost = CreateTestStrategyHost({execNode});
    auto treeSchedulerHost = CreateTestTreeJobSchedulerHost(strategyHost);
    auto treeScheduler = CreateTestTreeScheduler(treeSchedulerHost, strategyHost.Get());

    // Root element.
    auto rootElement = CreateTestRootElement(strategyHost.Get());

    // 1/10 of all resources.
    TJobResourcesWithQuota operationJobResources;
    operationJobResources.SetUserSlots(1);
    operationJobResources.SetCpu(10);
    operationJobResources.SetMemory(10);
    operationJobResources.DiskQuota() = CreateDiskQuota(0);

    // Create 5 operations, each with 2 jobs.
    constexpr int OperationCount = 5;
    std::vector<TOperationStrategyHostMockPtr> operations(OperationCount);
    std::vector<TSchedulerOperationElementPtr> operationElements(OperationCount);
    for (int opIndex = 0; opIndex < OperationCount; ++opIndex) {
        operations[opIndex] = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(2, operationJobResources));
        operationElements[opIndex] = CreateTestOperationElement(
            strategyHost.Get(),
            treeScheduler,
            operations[opIndex].Get(),
            rootElement.Get());
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
                result->StartDescriptor.emplace(TJobId(TGuid::Create()), operationJobResources, /*interruptible*/ false);
                return MakeFuture<TControllerScheduleJobResultPtr>(
                    TErrorOr<TControllerScheduleJobResultPtr>(result));
            }));
    }

    // With heap.

    {
        auto treeSnapshot = DoFairShareUpdate(strategyHost.Get(), treeScheduler, rootElement);
        auto scheduleJobsContextWithDependencies = PrepareScheduleJobsContext(strategyHost.Get(), treeSnapshot, execNode);
        auto context = scheduleJobsContextWithDependencies.ScheduleJobsContext;

        context->StartStage(EJobSchedulingStage::RegularMediumPriority, &RegularSchedulingProfilingCounters_);
        context->PrepareForScheduling();
        context->PrescheduleJob();

        for (const auto& operationElement : operationElements) {
            const auto& dynamicAttributes = context->DynamicAttributesOf(operationElement.Get());
            ASSERT_TRUE(dynamicAttributes.Active);
        }

        for (int iter = 0; iter < 2; ++iter) {
            for (auto operationElement : operationElements) {
                YT_VERIFY(context->SchedulingContext()->CanStartMoreJobs());

                bool scheduled = context->ScheduleJobInTest(operationElement.Get(), /*ignorePacking*/ true);
                EXPECT_TRUE(scheduled);

                const auto& childSet = context->DynamicAttributesOf(rootElement.Get()).SchedulableChildSet;

                ASSERT_TRUE(childSet);
                EXPECT_TRUE(childSet->UsesHeapInTest());

                int childIndex = 0;
                for (auto* element : childSet->GetChildren()) {
                    EXPECT_EQ(context->DynamicAttributesOf(element).SchedulableChildSetIndex, childIndex);
                    ++childIndex;
                }
            }
        }

        context->FinishStage();

        // NB(eshcherbin): It is impossible to have two consecutive non-preemptive scheduling stages, however
        // here we only need to trigger the second PrescheduleJob call so that the child heap is rebuilt.
        context->StartStage(EJobSchedulingStage::RegularMediumPriority, &RegularSchedulingProfilingCounters_);
        context->PrepareForScheduling();
        context->PrescheduleJob();

        const auto& childSet = context->DynamicAttributesOf(rootElement.Get()).SchedulableChildSet;

        ASSERT_TRUE(childSet);
        EXPECT_TRUE(childSet->UsesHeapInTest());

        int childIndex = 0;
        for (auto* element : childSet->GetChildren()) {
            EXPECT_EQ(context->DynamicAttributesOf(element).SchedulableChildSetIndex, childIndex);
            ++childIndex;
        }

        context->FinishStage();
    }

    // Without heap.

    constexpr int NewOperationCount = 2;
    while (std::ssize(operations) > NewOperationCount) {
        const auto& operationElement = operationElements.back();
        operationElement->DetachParent();

        operationElements.pop_back();
        operations.pop_back();
    }

    {
        auto treeSnapshot = DoFairShareUpdate(strategyHost.Get(), treeScheduler, rootElement);
        auto scheduleJobsContextWithDependencies = PrepareScheduleJobsContext(strategyHost.Get(), treeSnapshot, execNode);
        auto context = scheduleJobsContextWithDependencies.ScheduleJobsContext;

        context->StartStage(EJobSchedulingStage::RegularMediumPriority, &RegularSchedulingProfilingCounters_);
        context->PrepareForScheduling();
        context->PrescheduleJob();

        const auto& childSet = context->DynamicAttributesOf(rootElement.Get()).SchedulableChildSet;

        EXPECT_FALSE(childSet);

        context->FinishStage();
    }
}

TEST_F(TFairShareTreeJobSchedulerTest, TestCollectConsideredSchedulableChildrenPerPool)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(100);
    nodeResources.DiskQuota() = CreateDiskQuota(100);
    auto execNode = CreateTestExecNode(nodeResources);

    auto strategyHost = CreateTestStrategyHost({execNode});
    auto treeSchedulerHost = CreateTestTreeJobSchedulerHost(strategyHost);
    auto treeScheduler = CreateTestTreeScheduler(treeSchedulerHost, strategyHost.Get());

    // Create pools.
    auto rootElement = CreateTestRootElement(strategyHost.Get());
    auto poolA = CreateTestPool(strategyHost.Get(), "poolA");
    auto poolB = CreateTestPool(strategyHost.Get(), "poolB");
    auto poolBX = CreateTestPool(strategyHost.Get(), "poolBX");
    auto poolBY = CreateTestPool(strategyHost.Get(), "poolBY");

    poolA->AttachParent(rootElement.Get());
    poolB->AttachParent(rootElement.Get());
    poolBX->AttachParent(poolB.Get());
    poolBY->AttachParent(poolB.Get());

    // Create operations.
    auto operationRoot = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList());
    auto operationElementRoot = CreateTestOperationElement(strategyHost.Get(), treeScheduler, operationRoot.Get(), rootElement.Get());
    auto operationA = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList());
    auto operationElementA = CreateTestOperationElement(strategyHost.Get(), treeScheduler, operationA.Get(), poolA.Get());
    auto operationB = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList());
    auto operationElementB = CreateTestOperationElement(strategyHost.Get(), treeScheduler, operationB.Get(), poolB.Get());
    auto operationBX = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList());
    auto operationElementBX = CreateTestOperationElement(strategyHost.Get(), treeScheduler, operationBX.Get(), poolBX.Get());
    auto operationBY = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList());
    auto operationElementBY = CreateTestOperationElement(strategyHost.Get(), treeScheduler, operationBY.Get(), poolBY.Get());

    const std::vector<TSchedulerElement*> treeElements{
        static_cast<TSchedulerElement*>(rootElement.Get()),
        static_cast<TSchedulerElement*>(poolA.Get()),
        static_cast<TSchedulerElement*>(poolB.Get()),
        static_cast<TSchedulerElement*>(poolBX.Get()),
        static_cast<TSchedulerElement*>(poolBY.Get()),
        static_cast<TSchedulerElement*>(operationElementRoot.Get()),
        static_cast<TSchedulerElement*>(operationElementA.Get()),
        static_cast<TSchedulerElement*>(operationElementB.Get()),
        static_cast<TSchedulerElement*>(operationElementBX.Get()),
        static_cast<TSchedulerElement*>(operationElementBY.Get()),
    };

    auto doTestCase = [&] (
        const THashSet<TSchedulerOperationElement*>& consideredOperations,
        const THashSet<TSchedulerCompositeElement*>& expectedActivePools)
    {
        THashSet<TSchedulerElement*> expectedActiveElements;
        for (auto* pool : expectedActivePools) {
            expectedActiveElements.insert(pool);
        }

        std::vector<TSchedulerOperationElement*> consideredOperationsList;
        for (auto* operation : consideredOperations) {
            consideredOperationsList.push_back(operation);
            expectedActiveElements.insert(operation);
        }

        auto treeSnapshot = DoFairShareUpdate(strategyHost.Get(), treeScheduler, rootElement);
        auto scheduleJobsContextWithDependencies = PrepareScheduleJobsContext(strategyHost.Get(), treeSnapshot, execNode);
        auto context = scheduleJobsContextWithDependencies.ScheduleJobsContext;
        context->StartStage(EJobSchedulingStage::RegularMediumPriority, &RegularSchedulingProfilingCounters_);
        auto finally = Finally([&] {
            context->FinishStage();
        });

        context->PrepareForScheduling();
        context->PrescheduleJob(consideredOperationsList);

        for (auto* element : treeElements) {
            YT_LOG_INFO("Testing element activeness (ElementId: %v, ExpectedActive: %v, ActualActive: %v)",
                element->GetId(),
                expectedActiveElements.contains(element),
                context->DynamicAttributesOf(element).Active);

            EXPECT_EQ(expectedActiveElements.contains(element), context->DynamicAttributesOf(element).Active);

            if (auto* pool = dynamic_cast<TSchedulerCompositeElement*>(element)) {
                YT_LOG_INFO("Testing pool's child set presence: (ExpectedPresent: %v, ActualPresent: %v)",
                    expectedActiveElements.contains(pool),
                    context->DynamicAttributesOf(pool).SchedulableChildSet.has_value());

                ASSERT_EQ(
                    expectedActiveElements.contains(pool),
                    context->DynamicAttributesOf(pool).SchedulableChildSet.has_value());
            }
        }

        for (auto* pool : expectedActivePools) {
            const auto& childSet = context->DynamicAttributesOf(pool).SchedulableChildSet;
            auto isChildInSet = [&childSet] (TSchedulerElement* child) {
                const auto& children = childSet->GetChildren();
                return std::find(children.begin(), children.end(), child) != children.end();
            };

            for (auto* child : pool->SchedulableChildren()) {
                EXPECT_EQ(expectedActiveElements.contains(child), isChildInSet(child));
            }
        }
    };

    YT_LOG_INFO("All operations");
    doTestCase(
        /*consideredOperations*/ {
            operationElementRoot.Get(),
            operationElementA.Get(),
            operationElementB.Get(),
            operationElementBX.Get(),
            operationElementBY.Get(),
        },
        /*expectedActivePools*/ {
            rootElement.Get(),
            poolA.Get(),
            poolB.Get(),
            poolBX.Get(),
            poolBY.Get(),
        });

    YT_LOG_INFO("== Root operation");
    doTestCase(
        /*consideredOperations*/ {
            operationElementRoot.Get(),
        },
        /*expectedActivePools*/ {
            rootElement.Get(),
        });

    YT_LOG_INFO("== Operation A");
    doTestCase(
        /*consideredOperations*/ {
            operationElementA.Get(),
        },
        /*expectedActivePools*/ {
            rootElement.Get(),
            poolA.Get(),
        });

    YT_LOG_INFO("== OperationB");
    doTestCase(
        /*consideredOperations*/ {
            operationElementB.Get(),
        },
        /*expectedActivePools*/ {
            rootElement.Get(),
            poolB.Get(),
        });

    YT_LOG_INFO("== OperationBX");
    doTestCase(
        /*consideredOperations*/ {
            operationElementBX.Get(),
        },
        /*expectedActivePools*/ {
            rootElement.Get(),
            poolB.Get(),
            poolBX.Get(),
        });

    YT_LOG_INFO("== Operations A, B");
    doTestCase(
        /*consideredOperations*/ {
            operationElementA.Get(),
            operationElementB.Get(),
        },
        /*expectedActivePools*/ {
            rootElement.Get(),
            poolA.Get(),
            poolB.Get(),
        });

    YT_LOG_INFO("== Operations A, BX");
    doTestCase(
        /*consideredOperations*/ {
            operationElementA.Get(),
            operationElementBX.Get(),
        },
        /*expectedActivePools*/ {
            rootElement.Get(),
            poolA.Get(),
            poolB.Get(),
            poolBX.Get(),
        });

    YT_LOG_INFO("== Operations BX, BY");
    doTestCase(
        /*consideredOperations*/ {
            operationElementBX.Get(),
            operationElementBY.Get(),
        },
        /*expectedActivePools*/ {
            rootElement.Get(),
            poolB.Get(),
            poolBX.Get(),
            poolBY.Get(),
        });

    YT_LOG_INFO("== Operations B, BY");
    doTestCase(
        /*consideredOperations*/ {
            operationElementB.Get(),
            operationElementBY.Get(),
        },
        /*expectedActivePools*/ {
            rootElement.Get(),
            poolB.Get(),
            poolBY.Get(),
        });

    YT_LOG_INFO("== Operations A, B, BY");
    doTestCase(
        /*consideredOperations*/ {
            operationElementA.Get(),
            operationElementB.Get(),
            operationElementBY.Get(),
        },
        /*expectedActivePools*/ {
            rootElement.Get(),
            poolA.Get(),
            poolB.Get(),
            poolBY.Get(),
        });

    // Corner cases.
    {
        YT_LOG_INFO("== No operations");

        auto treeSnapshot = DoFairShareUpdate(strategyHost.Get(), treeScheduler, rootElement);
        auto scheduleJobsContextWithDependencies = PrepareScheduleJobsContext(strategyHost.Get(), treeSnapshot, execNode);
        auto context = scheduleJobsContextWithDependencies.ScheduleJobsContext;
        context->StartStage(EJobSchedulingStage::RegularMediumPriority, &RegularSchedulingProfilingCounters_);
        auto finally = Finally([&] {
            context->FinishStage();
        });

        context->PrepareForScheduling();
        context->PrescheduleJob(TNonOwningOperationElementList{});

        for (auto* element : treeElements) {
            EXPECT_FALSE(context->DynamicAttributesOf(element).Active);
        }

        const auto& childSet = context->DynamicAttributesOf(rootElement.Get()).SchedulableChildSet;
        EXPECT_TRUE(childSet.has_value());
        EXPECT_TRUE(childSet->GetChildren().empty());
    }
}

TEST_F(TFairShareTreeJobSchedulerTest, TestGuaranteePriorityScheduling)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(100);
    nodeResources.DiskQuota() = CreateDiskQuota(100);
    auto execNode = CreateTestExecNode(nodeResources);

    auto strategyHost = CreateTestStrategyHost({execNode});
    auto treeSchedulerHost = CreateTestTreeJobSchedulerHost(strategyHost);
    auto treeScheduler = CreateTestTreeScheduler(treeSchedulerHost, strategyHost.Get());

    // Create pools.
    auto rootElement = CreateTestRootElement(strategyHost.Get());
    auto poolA = CreateTestPool(strategyHost.Get(), "poolA");

    poolA->AttachParent(rootElement.Get());

    TJobResourcesConfigPtr poolBGuaranteeConfig = New<TJobResourcesConfig>();
    poolBGuaranteeConfig->Cpu = 70;

    auto poolConfig = New<TPoolConfig>();
    poolConfig->ComputePromisedGuaranteeFairShare = true;
    poolConfig->StrongGuaranteeResources = poolBGuaranteeConfig;
    auto poolB = CreateTestPool(strategyHost.Get(), "poolB", poolConfig);

    poolB->AttachParent(rootElement.Get());

    // Create operations.
    TJobResourcesWithQuota jobResources;
    jobResources.SetCpu(10);
    jobResources.SetMemory(10);
    jobResources.DiskQuota() = CreateDiskQuota(0);

    auto operationA1 = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(1, jobResources));
    auto operationElementA1 = CreateTestOperationElement(strategyHost.Get(), treeScheduler, operationA1.Get(), poolA.Get());
    auto operationA2 = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(1, jobResources));
    auto operationElementA2 = CreateTestOperationElement(strategyHost.Get(), treeScheduler, operationA2.Get(), poolA.Get());
    auto operationB1 = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(1, jobResources));
    auto operationElementB1 = CreateTestOperationElement(strategyHost.Get(), treeScheduler, operationB1.Get(), poolB.Get());
    auto operationB2 = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(1, jobResources));
    auto operationElementB2 = CreateTestOperationElement(strategyHost.Get(), treeScheduler, operationB2.Get(), poolB.Get());

    // Create usage for operations.
    int jobCount = 0;
    for (const auto& operationElement : {operationElementA1, operationElementA2, operationElementB1, operationElementB2}) {
        for (int jobIndex = 0; jobIndex < jobCount; ++jobIndex) {
            treeScheduler->OnJobStartedInTest(operationElement.Get(), TJobId(TGuid::Create()), jobResources);
        }

        ++jobCount;
    }

    auto vectorContains = [&] (const auto& vector, const auto& value) {
        return std::find(vector.begin(), vector.end(), value) != vector.end();
    };

    auto doTestCase = [&] (
        bool enableGuaranteePriorityScheduling,
        const std::vector<TSchedulerOperationElementPtr> expectedHighPriorityOperations,
        const std::vector<TSchedulerOperationElementPtr> expectedMediumPriorityOperations)
    {
        TreeConfig_->EnableGuaranteePriorityScheduling = enableGuaranteePriorityScheduling;

        auto treeSnapshot = DoFairShareUpdate(strategyHost.Get(), treeScheduler, rootElement);
        auto scheduleJobsContextWithDependencies = PrepareScheduleJobsContext(strategyHost.Get(), treeSnapshot, execNode);
        auto context = scheduleJobsContextWithDependencies.ScheduleJobsContext;

        const auto& staticAttributesList = treeSnapshot->SchedulingSnapshot()->StaticAttributesList();
        const auto& schedulableOperationsHigh = treeSnapshot->SchedulingSnapshot()->SchedulableOperationsPerPriority()[EOperationSchedulingPriority::High];
        const auto& schedulableOperationsMedium = treeSnapshot->SchedulingSnapshot()->SchedulableOperationsPerPriority()[EOperationSchedulingPriority::Medium];

        EXPECT_EQ(std::ssize(expectedHighPriorityOperations), std::ssize(schedulableOperationsHigh));
        for (const auto& operationElement : expectedHighPriorityOperations) {
            EXPECT_TRUE(vectorContains(schedulableOperationsHigh, operationElement));
            EXPECT_EQ(EOperationSchedulingPriority::High, staticAttributesList.AttributesOf(operationElement.Get()).SchedulingPriority);
        }

        EXPECT_EQ(std::ssize(expectedMediumPriorityOperations), std::ssize(schedulableOperationsMedium));
        for (const auto& operationElement : expectedMediumPriorityOperations) {
            EXPECT_TRUE(vectorContains(schedulableOperationsMedium, operationElement));
            EXPECT_EQ(EOperationSchedulingPriority::Medium, staticAttributesList.AttributesOf(operationElement.Get()).SchedulingPriority);
        }

        int expectedSchedulingIndex = 0;
        for (const auto& operationElement : expectedHighPriorityOperations) {
            EXPECT_EQ(expectedSchedulingIndex, staticAttributesList.AttributesOf(operationElement.Get()).SchedulingIndex);
            ++expectedSchedulingIndex;
        }
    };

    // Scheduling with priority.
    doTestCase(
        /*enableGuaranteePriorityScheduling*/ true,
        /*expectedHighPriorityOperations*/ {operationElementB1, operationElementB2},
        /*expectedMediumPriorityOperations*/ {operationElementA1, operationElementA2});

    // Scheduling without priority.
    doTestCase(
        /*enableGuaranteePriorityScheduling*/ false,
        /*expectedHighPriorityOperations*/ {},
        /*expectedMediumPriorityOperations*/ {operationElementA1, operationElementA2, operationElementB1, operationElementB2});
}

TEST_F(TFairShareTreeJobSchedulerTest, TestBuildDynamicAttributesListFromSnapshot)
{
    TJobResourcesWithQuota nodeResources;
    nodeResources.SetCpu(100);
    nodeResources.SetMemory(100);
    nodeResources.DiskQuota() = CreateDiskQuota(100);
    auto execNode = CreateTestExecNode(nodeResources);

    auto strategyHost = CreateTestStrategyHost({execNode});
    auto treeSchedulerHost = CreateTestTreeJobSchedulerHost(strategyHost);
    auto treeScheduler = CreateTestTreeScheduler(treeSchedulerHost, strategyHost.Get());

    // Pools.
    auto rootElement = CreateTestRootElement(strategyHost.Get());
    auto pool = CreateTestPool(strategyHost.Get(), "pool");

    pool->AttachParent(rootElement.Get());

    // 1/10 of all resources.
    TJobResourcesWithQuota jobResources;
    jobResources.SetCpu(10);
    jobResources.SetMemory(10);
    jobResources.DiskQuota() = CreateDiskQuota(0);

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
    treeScheduler->OnJobStartedInTest(operationElementB.Get(), TJobId(TGuid::Create()), jobResources);

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
    treeScheduler->OnJobStartedInTest(operationElementA.Get(), TJobId(TGuid::Create()), jobResources);

    auto treeSnapshot = DoFairShareUpdate(strategyHost.Get(), treeScheduler, rootElement);
    auto resourceUsageSnapshot = BuildResourceUsageSnapshot(treeSnapshot);

    treeScheduler->OnJobStartedInTest(operationElementA.Get(), TJobId(TGuid::Create()), jobResources);

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
