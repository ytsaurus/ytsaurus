#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/scheduler/fair_share_tree.h>
#include <yt/yt/server/scheduler/fair_share_tree_element.h>
#include <yt/yt/server/scheduler/operation_controller.h>
#include <yt/yt/server/scheduler/resource_tree.h>

#include <yt/yt/ytlib/chunk_client/proto/medium_directory.pb.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/digest.h>

#include <yt/yt/core/yson/null_consumer.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Extract common mocks to a separate file?
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

    void AbortAllocationsAtNode(NNodeTrackerClient::TNodeId /*nodeId*/, EAbortReason /*reason*/) override
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

class TOperationControllerStrategyHostMock
    : public IOperationControllerStrategyHost
{
public:
    explicit TOperationControllerStrategyHostMock(TJobResourcesWithQuotaList allocationResourcesList)
        : AllocationResourcesList_(std::move(allocationResourcesList))
    { }

    TControllerEpoch GetEpoch() const override
    {
        return TControllerEpoch(0);
    }

    MOCK_METHOD(TFuture<TControllerScheduleAllocationResultPtr>, ScheduleAllocation, (
        const ISchedulingContextPtr& context,
        const TJobResources& allocationLimits,
        const TDiskResources& diskResourceLimits,
        const TString& treeId,
        const TString& poolPath,
        const TFairShareStrategyTreeConfigPtr& treeConfig), (override));

    MOCK_METHOD(void, OnNonscheduledAllocationAborted, (TAllocationId, EAbortReason, TControllerEpoch), (override));

    TCompositeNeededResources GetNeededResources() const override
    {
        TJobResources totalResources;
        for (const auto& resources : AllocationResourcesList_) {
            totalResources += resources.ToJobResources();
        }
        return TCompositeNeededResources{.DefaultResources = totalResources};
    }

    void UpdateMinNeededAllocationResources() override
    { }

    TJobResourcesWithQuotaList GetMinNeededAllocationResources() const override
    {
        TJobResourcesWithQuotaList minNeededResourcesList;
        for (const auto& resources : AllocationResourcesList_) {
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
    TJobResourcesWithQuotaList AllocationResourcesList_;
};

using TOperationControllerStrategyHostMockPtr = TIntrusivePtr<TOperationControllerStrategyHostMock>;

////////////////////////////////////////////////////////////////////////////////

class TOperationStrategyHostMock
    : public TRefCounted
    , public IOperationStrategyHost
{
public:
    explicit TOperationStrategyHostMock(const TJobResourcesWithQuotaList& allocationResourcesList)
        : StartTime_(TInstant::Now())
        , Id_(TGuid::Create())
        , Controller_(New<TOperationControllerStrategyHostMock>(allocationResourcesList))
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

class TFairShareTreeElementTest
    : public testing::Test
{
public:
    void SetUp() override
    {
        TreeConfig_->AggressivePreemptionSatisfactionThreshold = 0.5;
        TreeConfig_->MinChildHeapSize = 3;
        TreeConfig_->EnableConditionalPreemption = true;
        TreeConfig_->UseResourceUsageWithPrecommit = false;
        TreeConfig_->ShouldDistributeFreeVolumeAmongChildren = true;
        TreeConfig_->BestAllocationShareUpdatePeriod = TDuration::Zero();
        TreeConfig_->NodeReconnectionTimeout = TDuration::Zero();
    }

protected:
    TSchedulerConfigPtr SchedulerConfig_ = New<TSchedulerConfig>();
    TFairShareStrategyTreeConfigPtr TreeConfig_ = New<TFairShareStrategyTreeConfig>();
    TFairShareTreeElementHostMockPtr FairShareTreeElementHostMock_ = New<TFairShareTreeElementHostMock>(TreeConfig_);

    int SlotIndex_ = 0;
    NNodeTrackerClient::TNodeId ExecNodeId_ = NNodeTrackerClient::TNodeId(0);

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
        return operationElement;
    }

    std::pair<TSchedulerOperationElementPtr, TOperationStrategyHostMockPtr> CreateOperationWithAllocations(
        int allocationCount,
        ISchedulerStrategyHost* strategyHost,
        TSchedulerCompositeElement* parent)
    {
        TJobResourcesWithQuota allocationResources;
        allocationResources.SetUserSlots(1);
        allocationResources.SetCpu(1);
        allocationResources.SetMemory(10_MB);

        auto operationHost = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(allocationCount, allocationResources));
        auto operationElement = CreateTestOperationElement(strategyHost, operationHost.Get(), parent);
        return {operationElement, operationHost};
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

    std::vector<TExecNodePtr> CreateTestExecNodeList(int count, const TJobResourcesWithQuota& nodeResources)
    {
        std::vector<TExecNodePtr> execNodes;
        for (int i = 0; i < count; i++) {
            execNodes.push_back(CreateTestExecNode(nodeResources));
        }
        return execNodes;
    }

    TSchedulerStrategyHostMockPtr CreateHostWith10NodesAnd10Cpu()
    {
        TJobResourcesWithQuota nodeResources;
        nodeResources.SetUserSlots(10);
        nodeResources.SetCpu(10);
        nodeResources.SetMemory(100_MB);

        return New<TSchedulerStrategyHostMock>(CreateTestExecNodeList(10, nodeResources));
    }

    void IncreaseOperationResourceUsage(const TSchedulerOperationElementPtr& operationElement, TJobResources resourceUsageDelta)
    {
        operationElement->CommitHierarchicalResourceUsage(resourceUsageDelta, /*precommitedResources*/ {});
    }

    void DoFairShareUpdate(
        const ISchedulerStrategyHost* strategyHost,
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

        TFairSharePostUpdateContext fairSharePostUpdateContext{
            .TreeConfig = TreeConfig_,
        };

        rootElement->PreUpdate(&context);

        NVectorHdrf::TFairShareUpdateExecutor updateExecutor(rootElement, &context);
        updateExecutor.Run();

        rootElement->PostUpdate(&fairSharePostUpdateContext);
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

    TJobResourcesWithQuota allocationResources;
    allocationResources.SetUserSlots(1);
    allocationResources.SetCpu(1);
    allocationResources.SetMemory(10);

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

    std::array<TOperationStrategyHostMockPtr, OperationCount> operations;
    std::array<TSchedulerOperationElementPtr, OperationCount> operationElements;

    for (auto& operation : operations) {
        operation = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(10, allocationResources));
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
        IncreaseOperationResourceUsage(operationElements[0], allocationResources.ToJobResources());
        IncreaseOperationResourceUsage(operationElements[2], allocationResources.ToJobResources());
    }

    {
        DoFairShareUpdate(strategyHost.Get(), rootElement);

        // Demand increased to 0.2 due to started allocations, so did fair share.
        // usage(0.1) / fair_share(0.2) = 0.5
        EXPECT_EQ(0.5, operationElements[0]->PostUpdateAttributes().SatisfactionRatio);
        EXPECT_EQ(0.0, operationElements[1]->PostUpdateAttributes().SatisfactionRatio);
        EXPECT_EQ(0.5, operationElements[2]->PostUpdateAttributes().SatisfactionRatio);
        EXPECT_EQ(0.0, operationElements[3]->PostUpdateAttributes().SatisfactionRatio);
        EXPECT_EQ(0.0, poolA->PostUpdateAttributes().SatisfactionRatio);
        EXPECT_EQ(InfiniteSatisfactionRatio, poolB->PostUpdateAttributes().SatisfactionRatio);
        EXPECT_EQ(0.0, poolC->PostUpdateAttributes().SatisfactionRatio);
        EXPECT_EQ(InfiniteSatisfactionRatio, poolD->PostUpdateAttributes().SatisfactionRatio);

        const auto& digest = rootElement->PostUpdateAttributes().SatisfactionDigest;
        ASSERT_TRUE(digest);
        EXPECT_NEAR(0.0, digest->GetQuantile(0.0), 1e-7);
        EXPECT_NEAR(0.0, digest->GetQuantile(0.25), 1e-7);
        EXPECT_NEAR(0.0, digest->GetQuantile(0.5), 1e-7);
        EXPECT_NEAR(0.5, digest->GetQuantile(0.51), 1e-7);
        EXPECT_NEAR(0.5, digest->GetQuantile(0.75), 1e-7);
        EXPECT_NEAR(0.5, digest->GetQuantile(1.0), 1e-7);
    }

    for (int i = 0; i < 10; ++i) {
        IncreaseOperationResourceUsage(operationElements[1], allocationResources.ToJobResources());
        IncreaseOperationResourceUsage(operationElements[3], allocationResources.ToJobResources());
    }

    {
        DoFairShareUpdate(strategyHost.Get(), rootElement);

        // Demand increased to 0.2 due to started allocations, so did fair share.
        // usage(0.1) / fair_share(0.2) = 0.5
        EXPECT_EQ(0.5, operationElements[0]->PostUpdateAttributes().SatisfactionRatio);
        EXPECT_EQ(0.5, operationElements[1]->PostUpdateAttributes().SatisfactionRatio);
        EXPECT_EQ(0.5, operationElements[2]->PostUpdateAttributes().SatisfactionRatio);
        EXPECT_EQ(0.5, operationElements[3]->PostUpdateAttributes().SatisfactionRatio);
        EXPECT_EQ(0.5, poolA->PostUpdateAttributes().SatisfactionRatio);
        EXPECT_EQ(InfiniteSatisfactionRatio, poolB->PostUpdateAttributes().SatisfactionRatio);
        EXPECT_EQ(0.5, poolC->PostUpdateAttributes().SatisfactionRatio);
        EXPECT_EQ(InfiniteSatisfactionRatio, poolD->PostUpdateAttributes().SatisfactionRatio);

        const auto& digest = rootElement->PostUpdateAttributes().SatisfactionDigest;
        ASSERT_TRUE(digest);
        EXPECT_NEAR(0.0, digest->GetQuantile(0.0), 1e-7);
        EXPECT_NEAR(0.5, digest->GetQuantile(0.25), 1e-7);
        EXPECT_NEAR(0.5, digest->GetQuantile(0.5), 1e-7);
        EXPECT_NEAR(0.5, digest->GetQuantile(0.51), 1e-7);
        EXPECT_NEAR(0.5, digest->GetQuantile(0.75), 1e-7);
        EXPECT_NEAR(0.5, digest->GetQuantile(1.0), 1e-7);
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

    TJobResourcesWithQuota allocationResources;
    allocationResources.SetUserSlots(1);
    allocationResources.SetCpu(1);
    allocationResources.SetMemory(10);

    auto operationX = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(10, allocationResources));
    auto specX = New<TStrategyOperationSpec>();
    specX->SchedulingTagFilter = MakeBooleanFormula("tag_1 | tag_2 | tag_5");
    auto operationElementX = CreateTestOperationElement(strategyHost.Get(), operationX.Get(), rootElement.Get(), operationOptions, specX);

    auto operationY = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(5, allocationResources));
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

    TJobResourcesWithQuota allocationResources;
    allocationResources.SetUserSlots(1);
    allocationResources.SetCpu(1);
    allocationResources.SetMemory(150);

    auto operationOptions = New<TOperationFairShareTreeRuntimeParameters>();
    operationOptions->Weight = 1.0;

    auto execNodes = CreateTestExecNodeList(2, nodeResourcesA);
    execNodes.push_back(CreateTestExecNode(nodeResourcesB));

    auto strategyHost = New<TSchedulerStrategyHostMock>(execNodes);

    auto rootElement = CreateTestRootElement(strategyHost.Get());

    auto operationX = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList(3, allocationResources));
    auto operationElementX = CreateTestOperationElement(strategyHost.Get(), operationX.Get(), rootElement.Get(), operationOptions);

    DoFairShareUpdate(strategyHost.Get(), rootElement);

    auto totalResources = nodeResourcesA * 2. + nodeResourcesB.ToJobResources();
    auto demandShare = TResourceVector::FromJobResources(allocationResources * 3., totalResources);
    auto fairShare = TResourceVector::FromJobResources(allocationResources, totalResources);
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
    pools[2]->IncreaseLightweightRunningOperationCount(1);

    EXPECT_EQ(1, rootElement->OperationCount());
    EXPECT_EQ(1, rootElement->RunningOperationCount());
    EXPECT_EQ(1, rootElement->LightweightRunningOperationCount());

    EXPECT_EQ(1, pools[1]->OperationCount());
    EXPECT_EQ(1, pools[1]->RunningOperationCount());
    EXPECT_EQ(1, pools[1]->LightweightRunningOperationCount());

    pools[1]->IncreaseOperationCount(5);
    EXPECT_EQ(6, rootElement->OperationCount());
    for (int i = 0; i < 5; ++i) {
        pools[1]->IncreaseOperationCount(-1);
    }
    EXPECT_EQ(1, rootElement->OperationCount());

    pools[2]->IncreaseOperationCount(-1);
    pools[2]->IncreaseRunningOperationCount(-1);
    pools[2]->IncreaseLightweightRunningOperationCount(-1);

    EXPECT_EQ(0, rootElement->OperationCount());
    EXPECT_EQ(0, rootElement->RunningOperationCount());
    EXPECT_EQ(0, rootElement->LightweightRunningOperationCount());
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

    TJobResources allocationResourcesA;
    allocationResourcesA.SetUserSlots(1);
    allocationResourcesA.SetCpu(3.81);
    allocationResourcesA.SetMemory(107340424507);
    allocationResourcesA.SetNetwork(0);
    allocationResourcesA.SetGpu(1);

    auto operationA = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList());
    auto operationElementA = CreateTestOperationElement(strategyHost.Get(), operationA.Get(), pool.Get());
    IncreaseOperationResourceUsage(operationElementA, allocationResourcesA);

    TJobResources allocationResourcesB;
    allocationResourcesB.SetUserSlots(1);
    allocationResourcesB.SetCpu(4);
    allocationResourcesB.SetMemory(107340424507);
    allocationResourcesB.SetNetwork(0);
    allocationResourcesB.SetGpu(1);

    auto operationB = New<TOperationStrategyHostMock>(TJobResourcesWithQuotaList());
    auto operationElementB = CreateTestOperationElement(strategyHost.Get(), operationB.Get(), pool.Get());
    IncreaseOperationResourceUsage(operationElementB, allocationResourcesB);

    DoFairShareUpdate(strategyHost.Get(), rootElement);

    EXPECT_EQ(pool->Attributes().UsageShare, pool->Attributes().DemandShare);
    EXPECT_TRUE(Dominates(
        pool->Attributes().DemandShare + TResourceVector::SmallEpsilon(),
        pool->Attributes().FairShare.Total));

    EXPECT_EQ(ESchedulableStatus::Normal, pool->GetStatus());
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

TEST_F(TFairShareTreeElementTest, TestVolumeOverflowDistributionWithLimitedAcceptablePool)
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
