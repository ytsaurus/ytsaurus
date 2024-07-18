#include <yt/yt/library/vector_hdrf/fair_share_update.h>
#include <yt/yt/library/vector_hdrf/resource_helpers.h>
#include <yt/yt/library/vector_hdrf/private.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////

TJobResources CreateCpuResourceLimits(double cpu)
{
    auto result = TJobResources::Infinite();
    result.SetCpu(cpu);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TElementMock;
class TCompositeElementMock;
class TPoolElementMock;
class TRootElementMock;
class TOperationElementMock;

////////////////////////////////////////////////////////////////////////////////

class TTestJobResourcesConfig
    : public TRefCounted
    , public NVectorHdrf::TJobResourcesConfig
{ };

using TTestJobResourcesConfigPtr = TIntrusivePtr<TTestJobResourcesConfig>;

////////////////////////////////////////////////////////////////////////////////

class TPoolIntegralGuaranteesConfig
    : public TRefCounted
{
public:
    NVectorHdrf::EIntegralGuaranteeType GuaranteeType;

    TTestJobResourcesConfigPtr ResourceFlow = New<TTestJobResourcesConfig>();
    TTestJobResourcesConfigPtr BurstGuaranteeResources = New<TTestJobResourcesConfig>();

    double RelaxedShareMultiplierLimit = 3.0;

    bool CanAcceptFreeVolume = true;
    bool ShouldDistributeFreeVolumeAmongChildren = false;
};

using TPoolIntegralGuaranteesConfigPtr = TIntrusivePtr<TPoolIntegralGuaranteesConfig>;

////////////////////////////////////////////////////////////////////////////////

class TElementMock
    : public virtual TElement
{
public:
    explicit TElementMock(TString id)
        : Id_(std::move(id))
    { }

    const TJobResources& GetResourceDemand() const override
    {
        return ResourceDemand_;
    }

    const TJobResources& GetResourceUsageAtUpdate() const override
    {
        return ResourceUsage_;
    }

    const TJobResources& GetResourceLimits() const override
    {
        return ResourceLimits_;
    }

    const TJobResourcesConfig* GetStrongGuaranteeResourcesConfig() const override
    {
        return StrongGuaranteeResourcesConfig_.Get();
    }

    double GetWeight() const override
    {
        return Weight_;
    }

    TSchedulableAttributes& Attributes() override
    {
        return Attributes_;
    }
    const TSchedulableAttributes& Attributes() const override
    {
        return Attributes_;
    }

    TCompositeElement* GetParentElement() const override
    {
        return Parent_;
    }

    TString GetId() const override
    {
        return Id_;
    }

    const NLogging::TLogger& GetLogger() const override
    {
        return FairShareLogger;
    }

    bool AreDetailedLogsEnabled() const override
    {
        return true;
    }

    virtual void AttachParent(TCompositeElementMock* parent);

    virtual void PreUpdate(const TJobResources& totalResourceLimits)
    {
        TotalResourceLimits_ = totalResourceLimits;
    }

    void SetWeight(double weight)
    {
        Weight_ = weight;
    }

    void SetStrongGuaranteeResourcesConfig(const TTestJobResourcesConfigPtr& strongGuaranteeResourcesConfig)
    {
        StrongGuaranteeResourcesConfig_ = strongGuaranteeResourcesConfig;
    }

    void SetResourceLimits(const TJobResources& resourceLimits)
    {
        ResourceLimits_ = resourceLimits;
    }

protected:
    const TString Id_;
    TCompositeElement* Parent_ = nullptr;

    TJobResources ResourceDemand_;
    TJobResources ResourceUsage_;
    TJobResources TotalResourceLimits_;

    // These attributes are calculated during fair share update and further used in schedule jobs.
    NVectorHdrf::TSchedulableAttributes Attributes_;

private:
    double Weight_ = 1.0;
    TJobResources ResourceLimits_ = TJobResources::Infinite();
    TTestJobResourcesConfigPtr StrongGuaranteeResourcesConfig_ = New<TTestJobResourcesConfig>();
};

using TElementMockPtr = TIntrusivePtr<TElementMock>;

////////////////////////////////////////////////////////////////////////////////

class TCompositeElementMock
    : public virtual TCompositeElement
    , public TElementMock
{
public:
    DEFINE_BYREF_RW_PROPERTY(TPoolIntegralGuaranteesConfigPtr, IntegralGuaranteesConfig, New<TPoolIntegralGuaranteesConfig>());

public:
    explicit TCompositeElementMock(TString id)
        : TElementMock(std::move(id))
    { }

    TElement* GetChild(int index) override
    {
        return Children_[index].Get();
    }

    const TElement* GetChild(int index) const override
    {
        return Children_[index].Get();
    }

    int GetChildCount() const override
    {
        return Children_.size();
    }

    ESchedulingMode GetMode() const override
    {
        return Mode_;
    }

    bool HasHigherPriorityInFifoMode(const TElement* lhs, const TElement* rhs) const override
    {
        if (lhs->GetWeight() != rhs->GetWeight()) {
            return lhs->GetWeight() > rhs->GetWeight();
        }
        return false;
    }

    double GetSpecifiedBurstRatio() const override
    {
        if (IntegralGuaranteesConfig_->GuaranteeType == EIntegralGuaranteeType::None) {
            return 0;
        }
        return GetMaxResourceRatio(ToJobResources(*IntegralGuaranteesConfig_->BurstGuaranteeResources, {}), TotalResourceLimits_);
    }

    double GetSpecifiedResourceFlowRatio() const override
    {
        if (IntegralGuaranteesConfig_->GuaranteeType == EIntegralGuaranteeType::None) {
            return 0;
        }
        return GetMaxResourceRatio(ToJobResources(*IntegralGuaranteesConfig_->ResourceFlow, {}), TotalResourceLimits_);
    }

    bool IsFairShareTruncationInFifoPoolEnabled() const override
    {
        return FairShareTruncationInFifoPoolEnabled_;
    }

    bool CanAcceptFreeVolume() const override
    {
        return IntegralGuaranteesConfig_->CanAcceptFreeVolume;
    }

    bool ShouldDistributeFreeVolumeAmongChildren() const override
    {
        return IntegralGuaranteesConfig_->ShouldDistributeFreeVolumeAmongChildren;
    }

    bool ShouldComputePromisedGuaranteeFairShare() const override
    {
        return PromisedGuaranteeFairShareComputationEnabled_;
    }

    bool IsPriorityStrongGuaranteeAdjustmentEnabled() const override
    {
        return false;
    }

    bool IsPriorityStrongGuaranteeAdjustmentDonorshipEnabled() const override
    {
        return false;
    }

    void AddChild(TElementMock* child)
    {
        Children_.push_back(child);
    }

    void PreUpdate(const TJobResources& totalResourceLimits) override
    {
        ResourceDemand_ = {};

        for (const auto& child : Children_) {
            child->PreUpdate(totalResourceLimits);

            ResourceUsage_ += child->GetResourceUsageAtUpdate();
            ResourceDemand_ += child->GetResourceDemand();
        }

        TElementMock::PreUpdate(totalResourceLimits);
    }

    void SetMode(ESchedulingMode mode)
    {
        Mode_ = mode;
    }

    void SetFairShareTruncationInFifoPoolEnabled(bool enabled)
    {
        FairShareTruncationInFifoPoolEnabled_ = enabled;
    }

    void SetPromisedGuaranteeFairShareComputationEnabled(bool enabled)
    {
        PromisedGuaranteeFairShareComputationEnabled_ = enabled;
    }

private:
    std::vector<TElementMockPtr> Children_;

    ESchedulingMode Mode_ = ESchedulingMode::FairShare;
    bool FairShareTruncationInFifoPoolEnabled_ = false;
    bool PromisedGuaranteeFairShareComputationEnabled_ = false;
};

using TCompositeElementMockPtr = TIntrusivePtr<TCompositeElementMock>;

////////////////////////////////////////////////////////////////////////////////

class TPoolElementMock
    : public TPool
    , public TCompositeElementMock
{
public:
    TPoolElementMock(TString id)
        : TCompositeElementMock(std::move(id))
    { }

    TResourceVector GetIntegralShareLimitForRelaxedPool() const override
    {
        YT_VERIFY(GetIntegralGuaranteeType() == EIntegralGuaranteeType::Relaxed);
        auto multiplier = IntegralGuaranteesConfig_->RelaxedShareMultiplierLimit;
        return TResourceVector::FromDouble(Attributes_.ResourceFlowRatio) * multiplier;
    }

    const TIntegralResourcesState& IntegralResourcesState() const override
    {
        return IntegralResourcesState_;
    }
    TIntegralResourcesState& IntegralResourcesState() override
    {
        return IntegralResourcesState_;
    }

    EIntegralGuaranteeType GetIntegralGuaranteeType() const override
    {
        return IntegralGuaranteesConfig_->GuaranteeType;
    }

    void InitAccumulatedResourceVolume(TResourceVolume resourceVolume)
    {
        YT_VERIFY(IntegralResourcesState_.AccumulatedVolume == TResourceVolume());
        IntegralResourcesState_.AccumulatedVolume = resourceVolume;
    }

private:
    TIntegralResourcesState IntegralResourcesState_;
};

using TPoolElementMockPtr = TIntrusivePtr<TPoolElementMock>;

////////////////////////////////////////////////////////////////////////////////

class TOperationElementMock
    : public TOperationElement
    , public TElementMock
{
public:
    TOperationElementMock(TString id)
        : TElementMock(std::move(id))
    { }

    void SetResourceDemand(const TJobResources& resourceDemand)
    {
        ResourceDemand_ = resourceDemand;
    }

    void SetResourceUsage(const TJobResources& resourceUsage)
    {
        YT_VERIFY(Dominates(ResourceDemand_, resourceUsage));

        ResourceUsage_ = resourceUsage;
    }

    void IncreaseResourceUsageAndDemand(const TJobResources& delta)
    {
        ResourceDemand_ += delta;
        ResourceUsage_ += delta;
    }

    TResourceVector GetBestAllocationShare() const override
    {
        return TResourceVector::Ones();
    }

    void SetIsGang(bool isGang)
    {
        IsGang_ = isGang;
    }

    bool IsGang() const override
    {
        return IsGang_;
    }

private:
    bool IsGang_ = false;
};

using TOperationElementMockPtr = TIntrusivePtr<TOperationElementMock>;

////////////////////////////////////////////////////////////////////////////////

class TRootElementMock
    : public TRootElement
    , public TCompositeElementMock
{
public:
    TRootElementMock()
        : TCompositeElementMock("<Root>")
    { }
};

using TRootElementMockPtr = TIntrusivePtr<TRootElementMock>;

////////////////////////////////////////////////////////////////////////////////

void TElementMock::AttachParent(TCompositeElementMock* parent)
{
    parent->AddChild(this);
    Parent_ = parent;
}

////////////////////////////////////////////////////////////////////////////////

class TFairShareUpdateTest
    : public testing::Test
{
protected:
    TRootElementMockPtr CreateRootElement()
    {
        return New<TRootElementMock>();
    }

    TPoolElementMockPtr CreateSimplePool(
        TString id,
        std::optional<double> strongGuaranteeCpu = std::nullopt,
        double weight = 1.0)
    {
        auto strongGuaranteeResourcesConfig = New<TTestJobResourcesConfig>();
        strongGuaranteeResourcesConfig->Cpu = strongGuaranteeCpu;

        auto pool = New<TPoolElementMock>(std::move(id));
        pool->SetStrongGuaranteeResourcesConfig(strongGuaranteeResourcesConfig);
        pool->SetWeight(weight);
        return pool;
    }

    TPoolElementMockPtr CreateIntegralPool(
        TString id,
        EIntegralGuaranteeType type,
        double flowCpu,
        std::optional<double> burstCpu = std::nullopt,
        std::optional<double> strongGuaranteeCpu = std::nullopt,
        double weight = 1.0)
    {
        auto strongGuaranteeResourcesConfig = New<TTestJobResourcesConfig>();
        strongGuaranteeResourcesConfig->Cpu = strongGuaranteeCpu;

        auto integralGuaranteesConfig = New<TPoolIntegralGuaranteesConfig>();
        integralGuaranteesConfig->GuaranteeType = type;
        integralGuaranteesConfig->ResourceFlow->Cpu = flowCpu;
        if (burstCpu) {
            integralGuaranteesConfig->BurstGuaranteeResources->Cpu = *burstCpu;
        }

        auto pool = New<TPoolElementMock>(std::move(id));
        pool->SetStrongGuaranteeResourcesConfig(strongGuaranteeResourcesConfig);
        pool->SetWeight(weight);
        pool->IntegralGuaranteesConfig() = integralGuaranteesConfig;
        return pool;
    }

    TPoolElementMockPtr CreateBurstPool(
        TString id,
        double flowCpu,
        double burstCpu,
        std::optional<double> strongGuaranteeCpu = std::nullopt,
        double weight = 1.0)
    {
        return CreateIntegralPool(
            std::move(id),
            EIntegralGuaranteeType::Burst,
            flowCpu,
            burstCpu,
            strongGuaranteeCpu,
            weight);
    }

    TPoolElementMockPtr CreateRelaxedPool(
        TString id,
        double flowCpu,
        std::optional<double> strongGuaranteeCpu = std::nullopt,
        double weight = 1.0)
    {
        return CreateIntegralPool(
            std::move(id),
            EIntegralGuaranteeType::Relaxed,
            flowCpu,
            /*burstCpu*/ std::nullopt,
            strongGuaranteeCpu,
            weight);
    }

    TOperationElementMockPtr CreateOperation(
        TCompositeElementMock* parent,
        const TJobResources& resourceDemand = {},
        const TJobResources& resourceUsage = {})
    {
        auto operation = New<TOperationElementMock>(Format("Operation%v", OperationIndex_++));
        operation->SetResourceDemand(resourceDemand);
        operation->SetResourceUsage(resourceUsage);
        operation->AttachParent(parent);
        return operation;
    }

    TJobResources CreateTotalResourceLimitsWith100CPU()
    {
        TJobResources totalResourceLimits;
        totalResourceLimits.SetUserSlots(100);
        totalResourceLimits.SetCpu(100);
        totalResourceLimits.SetMemory(1000_MB);
        return totalResourceLimits;
    }

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

    TFairShareUpdateContext DoFairShareUpdate(
        const TJobResources& totalResourceLimits,
        const TRootElementMockPtr& rootElement,
        TInstant now = TInstant(),
        std::optional<TInstant> previousUpdateTime = std::nullopt)
    {
        ResetFairShareFunctionsRecursively(rootElement.Get());

        TFairShareUpdateContext context(
            TFairShareUpdateOptions{
                .MainResource = EJobResourceType::Cpu,
                .IntegralPoolCapacitySaturationPeriod = TDuration::Days(1),
                .IntegralSmoothPeriod = TDuration::Minutes(1),
                .EnableFastChildFunctionSummationInFifoPools = true,
            },
            totalResourceLimits,
            now,
            previousUpdateTime);

        rootElement->PreUpdate(totalResourceLimits);

        TFairShareUpdateExecutor updateExecutor(rootElement, &context);
        updateExecutor.Run();

        return context;
    }

private:
    int OperationIndex_ = 0;

    void ResetFairShareFunctionsRecursively(TCompositeElementMock* compositeElement)
    {
        compositeElement->ResetFairShareFunctions();
        for (int childIndex = 0; childIndex < compositeElement->GetChildCount(); ++childIndex) {
            auto* child = compositeElement->GetChild(childIndex);
            if (auto* childPool = dynamic_cast<TCompositeElementMock*>(child)) {
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

MATCHER_P2(ResourceVolumeNear, vec, absError, "") {
    bool result = true;
    TResourceVolume::ForEachResource([&] (EJobResourceType /*resourceType*/, auto TResourceVolume::* resourceDataMember) {
        result = result && std::abs(static_cast<double>(arg.*resourceDataMember - vec.*resourceDataMember)) < absError;
    });
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TFairShareUpdateTest, TestSimple)
{
    constexpr int OperationCount = 4;

    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto poolA = CreateSimplePool("PoolA");
    auto poolB = CreateSimplePool("PoolB");
    auto poolC = CreateSimplePool("PoolC");
    auto poolD = CreateSimplePool("PoolD");

    poolC->SetMode(ESchedulingMode::Fifo);
    poolD->SetMode(ESchedulingMode::Fifo);

    poolA->AttachParent(rootElement.Get());
    poolB->AttachParent(rootElement.Get());
    poolC->AttachParent(rootElement.Get());
    poolD->AttachParent(rootElement.Get());

    TJobResources operationDemand;
    operationDemand.SetUserSlots(10);
    operationDemand.SetCpu(10);
    operationDemand.SetMemory(100);

    std::array<TOperationElementMockPtr, OperationCount> operations;
    for (int i = 0; i < OperationCount; ++i) {

        TCompositeElementMock* parent = i < 2
            ? poolA.Get()
            : poolC.Get();
        operations[i] = CreateOperation(parent, operationDemand);

        if (i == 2) {
            // We need this to ensure FIFO order of operations 2 and 3.
            operations[i]->SetWeight(10.0);
        }
    }

    {
        DoFairShareUpdate(totalResourceLimits, rootElement);

        auto expectedOperationDemand = TResourceVector::FromJobResources(operationDemand, totalResourceLimits);
        auto poolExpectedDemand = expectedOperationDemand * (OperationCount / 2.0);
        auto totalExpectedDemand = expectedOperationDemand * OperationCount;

        EXPECT_THAT(totalExpectedDemand, ResourceVectorNear(rootElement->Attributes().DemandShare, 1e-7));
        EXPECT_THAT(poolExpectedDemand, ResourceVectorNear(poolA->Attributes().DemandShare, 1e-7));
        EXPECT_THAT(TResourceVector::Zero(), ResourceVectorNear(poolB->Attributes().DemandShare, 1e-7));
        EXPECT_THAT(poolExpectedDemand, ResourceVectorNear(poolC->Attributes().DemandShare, 1e-7));
        EXPECT_THAT(TResourceVector::Zero(), ResourceVectorNear(poolD->Attributes().DemandShare, 1e-7));
        for (const auto& operation : operations) {
            EXPECT_THAT(expectedOperationDemand, ResourceVectorNear(operation->Attributes().DemandShare, 1e-7));
        }

        EXPECT_THAT(totalExpectedDemand, ResourceVectorNear(rootElement->Attributes().FairShare.Total, 1e-7));
        EXPECT_THAT(poolExpectedDemand, ResourceVectorNear(poolA->Attributes().FairShare.Total, 1e-7));
        EXPECT_THAT(TResourceVector::Zero(), ResourceVectorNear(poolB->Attributes().FairShare.Total, 1e-7));
        EXPECT_THAT(poolExpectedDemand, ResourceVectorNear(poolC->Attributes().FairShare.Total, 1e-7));
        EXPECT_THAT(TResourceVector::Zero(), ResourceVectorNear(poolD->Attributes().FairShare.Total, 1e-7));
        for (const auto& operation : operations) {
            EXPECT_THAT(expectedOperationDemand, ResourceVectorNear(operation->Attributes().FairShare.Total, 1e-7));
        }
    }
}

TEST_F(TFairShareUpdateTest, TestResourceLimits)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto poolA = CreateSimplePool("PoolA");
    poolA->AttachParent(rootElement.Get());

    auto poolB = CreateSimplePool("PoolB");
    poolB->AttachParent(poolA.Get());

    auto totalLimitsShare = TResourceVector::FromJobResources(totalResourceLimits, totalResourceLimits);
    {
        DoFairShareUpdate(totalResourceLimits, rootElement);

        EXPECT_EQ(totalLimitsShare, rootElement->Attributes().LimitsShare);
        EXPECT_EQ(totalLimitsShare, poolA->Attributes().LimitsShare);
        EXPECT_EQ(totalLimitsShare, poolB->Attributes().LimitsShare);
    }

    TJobResources poolAResourceLimits;
    poolAResourceLimits.SetUserSlots(60);
    poolAResourceLimits.SetCpu(70);
    poolAResourceLimits.SetMemory(800);

    poolA->SetResourceLimits(poolAResourceLimits);

    {
        DoFairShareUpdate(totalResourceLimits, rootElement);

        EXPECT_EQ(totalLimitsShare, rootElement->Attributes().LimitsShare);

        auto poolALimitsShare = TResourceVector::FromJobResources(poolAResourceLimits, totalResourceLimits);
        EXPECT_EQ(poolALimitsShare, poolA->Attributes().LimitsShare);

        EXPECT_EQ(totalLimitsShare, poolB->Attributes().LimitsShare);
    }
}

TEST_F(TFairShareUpdateTest, TestFractionalResourceLimits)
{
    TJobResources totalResourceLimits;
    totalResourceLimits.SetUserSlots(10);
    totalResourceLimits.SetCpu(11.17);
    totalResourceLimits.SetMemory(100_MB);

    auto rootElement = CreateRootElement();

    auto poolA = CreateSimplePool("PoolA");
    poolA->AttachParent(rootElement.Get());

    TJobResources poolResourceLimits;
    poolResourceLimits.SetUserSlots(10);
    poolResourceLimits.SetCpu(11.06);
    poolResourceLimits.SetMemory(99_MB);
    poolA->SetResourceLimits(poolResourceLimits);

    auto totalLimitsShare = TResourceVector::FromJobResources(totalResourceLimits, totalResourceLimits);
    {
        DoFairShareUpdate(totalResourceLimits, rootElement);

        EXPECT_EQ(totalLimitsShare, rootElement->Attributes().LimitsShare);

        auto poolLimitsShare = TResourceVector::FromJobResources(poolResourceLimits, totalResourceLimits);
        EXPECT_EQ(poolLimitsShare, poolA->Attributes().LimitsShare);
    }
}

TEST_F(TFairShareUpdateTest, TestEmptyTree)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();

    // Create a tree with 2 pools
    auto rootElement = CreateRootElement();
    auto poolA = CreateSimplePool("PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateSimplePool("PoolB");
    poolB->AttachParent(rootElement.Get());

    DoFairShareUpdate(totalResourceLimits, rootElement);

    // Check the values
    EXPECT_EQ(TResourceVector::Zero(), rootElement->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector::Zero(), poolA->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector::Zero(), poolB->Attributes().FairShare.Total);
}

TEST_F(TFairShareUpdateTest, TestOneLargeOperation)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();

    // Create a tree with 2 pools
    auto rootElement = CreateRootElement();
    auto poolA = CreateSimplePool("PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateSimplePool("PoolB");
    poolB->AttachParent(rootElement.Get());

    // Create operation with demand larger than the available resources
    TJobResources resourceDemand;
    resourceDemand.SetUserSlots(200);
    resourceDemand.SetCpu(200);
    resourceDemand.SetMemory(4000_MB);

    auto operationX = CreateOperation(poolA.Get(), resourceDemand);

    DoFairShareUpdate(totalResourceLimits, rootElement);

    // Check the values
    TResourceVector expectedFairShare = {0.5, 0.5, 0.0, 1.0, 0.0};
    EXPECT_EQ(expectedFairShare, rootElement->Attributes().FairShare.Total);
    EXPECT_EQ(expectedFairShare, poolA->Attributes().FairShare.Total);
    EXPECT_EQ(expectedFairShare, operationX->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector::Zero(), poolB->Attributes().FairShare.Total);
}

TEST_F(TFairShareUpdateTest, TestOneSmallOperation)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();

    // Create a tree with 2 pools
    auto rootElement = CreateRootElement();
    auto poolA = CreateSimplePool("PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateSimplePool("PoolB");
    poolB->AttachParent(rootElement.Get());

    // Create operation with demand smaller than the available resources
    TJobResources resourceDemand;
    resourceDemand.SetUserSlots(30);
    resourceDemand.SetCpu(30);
    resourceDemand.SetMemory(600_MB);

    auto operationX = CreateOperation(poolA.Get(), resourceDemand);

    DoFairShareUpdate(totalResourceLimits, rootElement);

    // Check the values
    EXPECT_EQ(TResourceVector({0.3, 0.3, 0.0, 0.6, 0.0}), rootElement->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector({0.3, 0.3, 0.0, 0.6, 0.0}), poolA->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector({0.3, 0.3, 0.0, 0.6, 0.0}), operationX->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector::Zero(), poolB->Attributes().FairShare.Total);
}

TEST_F(TFairShareUpdateTest, TestTwoComplementaryOperations)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();

    // Create a tree with 2 pools
    auto rootElement = CreateRootElement();
    auto poolA = CreateSimplePool("PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateSimplePool("PoolB");
    poolB->AttachParent(rootElement.Get());

    TJobResources resourceDemandX;
    resourceDemandX.SetUserSlots(100);
    resourceDemandX.SetCpu(100);
    resourceDemandX.SetMemory(2000_MB);

    auto operationX = CreateOperation(poolA.Get(), resourceDemandX);

    // Second operation with symmetric resource demand
    TJobResources resourceDemandY;
    resourceDemandY.SetUserSlots(100);
    resourceDemandY.SetCpu(200);
    resourceDemandY.SetMemory(1000_MB);

    auto operationY = CreateOperation(poolA.Get(), resourceDemandY);

    DoFairShareUpdate(totalResourceLimits, rootElement);

    // Check the values
    EXPECT_EQ(TResourceVector({2.0 / 3, 1.0, 0.0, 1.0, 0.0}), rootElement->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector({2.0 / 3, 1.0, 0.0, 1.0, 0.0}), poolA->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector({1.0 / 3, 1.0 / 3, 0.0, 2.0 / 3, 0.0}), operationX->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector({1.0 / 3, 2.0 / 3, 0.0, 1.0 / 3, 0.0}), operationY->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector::Zero(), poolB->Attributes().FairShare.Total);
}

TEST_F(TFairShareUpdateTest, TestComplexCase)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();

    // Create a tree with 2 pools
    auto rootElement = CreateRootElement();
    auto poolA = CreateSimplePool("PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateSimplePool("PoolB");
    poolB->AttachParent(rootElement.Get());

    // Create an operation with resource demand proportion <1, 2> and small jobCount in PoolA
    TJobResources resourceDemandX;
    resourceDemandX.SetUserSlots(10);
    resourceDemandX.SetCpu(10);
    resourceDemandX.SetMemory(200_MB);

    auto operationX = CreateOperation(poolA.Get(), resourceDemandX);

    // Create an operation with resource demand proportion <3, 1> and large jobCount in PoolA
    TJobResources resourceDemandY;
    resourceDemandY.SetUserSlots(1000);
    resourceDemandY.SetCpu(3000);
    resourceDemandY.SetMemory(10000_MB);

    auto operationY = CreateOperation(poolA.Get(), resourceDemandY);

    // Create operation with resource demand proportion <1, 5> and large jobCount in PoolB
    TJobResources resourceDemandZ;
    resourceDemandZ.SetUserSlots(2000);
    resourceDemandZ.SetCpu(2000);
    resourceDemandZ.SetMemory(100000_MB);

    auto operationZ = CreateOperation(poolB.Get(), resourceDemandZ);

    DoFairShareUpdate(totalResourceLimits, rootElement);

    // Check the values

    // Memory will be saturated first (see the usages of operations bellow)
    EXPECT_RV_NEAR(rootElement->Attributes().FairShare.Total, TResourceVector({16.0 / 40, 30.0 / 40, 0.0, 40.0 / 40, 0.0}));
    EXPECT_RV_NEAR(poolA->Attributes().FairShare.Total, TResourceVector({11.0 / 40, 25.0 / 40, 0.0, 15.0 / 40, 0.0}));
    EXPECT_RV_NEAR(poolB->Attributes().FairShare.Total, TResourceVector({5.0 / 40, 5.0 / 40, 0.0, 25.0 / 40, 0.0}));

    // operation1 uses 4/40 CPU and 8/40 Memory
    EXPECT_RV_NEAR(operationX->Attributes().FairShare.Total, TResourceVector({4.0 / 40, 4.0 / 40, 0.0, 8.0 / 40, 0.0}));
    // operation2 uses 21/40 CPU and 7/40 Memory
    EXPECT_RV_NEAR(operationY->Attributes().FairShare.Total, TResourceVector({7.0 / 40, 21.0 / 40, 0.0, 7.0 / 40, 0.0}));
    // operation3 uses 5/40 CPU and 25/40 Memory
    EXPECT_RV_NEAR(operationZ->Attributes().FairShare.Total, TResourceVector({5.0 / 40, 5.0 / 40, 0.0, 25.0 / 40, 0.0}));
}

TEST_F(TFairShareUpdateTest, TestNonContinuousFairShare)
{
    TJobResources totalResourceLimits;
    totalResourceLimits.SetUserSlots(100'000);
    totalResourceLimits.SetCpu(100);
    totalResourceLimits.SetMemory(100_GB);
    totalResourceLimits.SetNetwork(100);

    // Create a tree with 2 pools
    auto rootElement = CreateRootElement();
    auto poolA = CreateSimplePool("PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateSimplePool("PoolB");
    poolB->AttachParent(rootElement.Get());

    // Create an operation with resource demand proportion <1, 1, 4>, weight=10, and small jobCount in PoolA
    TJobResources resourceDemandX;
    resourceDemandX.SetUserSlots(10);
    resourceDemandX.SetCpu(10);
    resourceDemandX.SetMemory(10_GB);
    resourceDemandX.SetNetwork(40);

    auto operationX = CreateOperation(poolA.Get(), resourceDemandX);
    operationX->SetWeight(10.0);

    // Create an operation with resource demand proportion <1, 1, 0>, weight=1, and large jobCount in PoolA
    TJobResources resourceDemandY;
    resourceDemandY.SetUserSlots(1000);
    resourceDemandY.SetCpu(1000);
    resourceDemandY.SetMemory(1000_GB);
    resourceDemandY.SetNetwork(0);

    auto operationY = CreateOperation(poolA.Get(), resourceDemandY);

    DoFairShareUpdate(totalResourceLimits, rootElement);

    // Check the values

    // Memory will be saturated first (see the usages of operations bellow)
    EXPECT_RV_NEAR(rootElement->Attributes().FairShare.Total, TResourceVector({0.001, 1.0, 0.0, 1.0, 0.4}));
    EXPECT_RV_NEAR(poolA->Attributes().FairShare.Total, TResourceVector({0.001, 1.0, 0.0, 1.0, 0.4}));
    EXPECT_RV_NEAR(poolB->Attributes().FairShare.Total, TResourceVector::Zero());

    // operation1 uses 0.1 CPU, 0.1 Memory, and 0.4 Network
    EXPECT_RV_NEAR(operationX->Attributes().FairShare.Total, TResourceVector({0.0001, 0.1, 0.0, 0.1, 0.4}));
    // operation2 uses 0.9 CPU, 0.9 Memory, and 0 Network
    EXPECT_RV_NEAR(operationY->Attributes().FairShare.Total, TResourceVector({0.0009, 0.9, 0.0, 0.9, 0.0}));
}

TEST_F(TFairShareUpdateTest, TestNonContinuousFairShareFunctionIsLeftContinuous)
{
    // Create a cluster with 1 large node.
    TJobResources totalResourceLimits;
    totalResourceLimits.SetUserSlots(100'000);
    totalResourceLimits.SetCpu(100);
    totalResourceLimits.SetMemory(100_GB);
    totalResourceLimits.SetNetwork(100);

    // Create a tree with 2 pools.
    auto rootElement = CreateRootElement();
    // Use fake root to be able to set a CPU limit.
    auto fakeRootElement = CreateSimplePool("FakeRoot");
    fakeRootElement->AttachParent(rootElement.Get());
    auto poolA = CreateSimplePool("PoolA");
    poolA->AttachParent(fakeRootElement.Get());
    auto poolB = CreateSimplePool("PoolB");
    poolB->AttachParent(fakeRootElement.Get());

    // Set CPU limit for fake root.
    fakeRootElement->SetResourceLimits(CreateCpuResourceLimits(40.0));

    // Create an operation with resource demand proportion <1, 1, 4>, weight=10, and small jobCount in PoolA.
    TJobResources resourceDemandX;
    resourceDemandX.SetUserSlots(10);
    resourceDemandX.SetCpu(10);
    resourceDemandX.SetMemory(10_GB);
    resourceDemandX.SetNetwork(40);

    auto operationX = CreateOperation(poolA.Get(), resourceDemandX);
    operationX->SetWeight(10.0);

    // Create an operation with resource demand proportion <1, 1, 0>, weight=1, and large jobCount in PoolA.
    TJobResources resourceDemandY;
    resourceDemandY.SetUserSlots(1000);
    resourceDemandY.SetCpu(1000);
    resourceDemandY.SetMemory(1000_GB);
    resourceDemandY.SetNetwork(0);

    auto operationY = CreateOperation(poolA.Get(), resourceDemandY);

    DoFairShareUpdate(totalResourceLimits, rootElement);

    // Check the values.
    // 0.4 is a discontinuity point of root's FSBS, so the amount of fair share given to poolA equals to
    // the left limit of FSBS at 0.4, even though we have enough resources to allocate the right limit at 0.4.
    // This is a fundamental property of our strategy.
    EXPECT_RV_NEAR(rootElement->Attributes().FairShare.Total, TResourceVector({0.00014, 0.14, 0.0, 0.14, 0.4}));
    EXPECT_RV_NEAR(fakeRootElement->Attributes().FairShare.Total, TResourceVector({0.00014, 0.14, 0.0, 0.14, 0.4}));
    EXPECT_RV_NEAR(poolA->Attributes().FairShare.Total, TResourceVector({0.00014, 0.14, 0.0, 0.14, 0.4}));
    EXPECT_RV_NEAR(poolB->Attributes().FairShare.Total, TResourceVector::Zero());

    // Operation 1 uses 0.1 CPU, 0.1 Memory, and 0.4 Network.
    EXPECT_RV_NEAR(operationX->Attributes().FairShare.Total, TResourceVector({0.0001, 0.1, 0.0, 0.1, 0.4}));
    // Operation 2 uses 0.04 CPU, 0.04 Memory, and 0.0 Network.
    EXPECT_RV_NEAR(operationY->Attributes().FairShare.Total, TResourceVector({0.00004, 0.04, 0.0, 0.04, 0.0}));
}

TEST_F(TFairShareUpdateTest, TestImpreciseComposition)
{
    // NB: This test is reconstructed from a core dump. Don't be surprised by precise resource demands. See YT-13864.

    // Create a cluster with 1 large node.
    TJobResources totalResourceLimits;
    totalResourceLimits.SetUserSlots(3);
    totalResourceLimits.SetCpu(3);
    totalResourceLimits.SetMemory(8316576848);

    auto rootElement = CreateRootElement();

    auto pool = CreateSimplePool("Pool", /*strongGuaranteeCpu*/ 3.0);
    pool->AttachParent(rootElement.Get());

    TJobResources jobResourcesX;
    jobResourcesX.SetUserSlots(2);
    jobResourcesX.SetCpu(2);
    jobResourcesX.SetMemory(805306368);
    jobResourcesX.SetNetwork(0);

    auto operationA = CreateOperation(pool.Get());

    TJobResources resourceDemandB;
    resourceDemandB.SetUserSlots(3);
    resourceDemandB.SetCpu(3);
    resourceDemandB.SetMemory(1207959552);

    auto operationB = CreateOperation(pool.Get(), resourceDemandB);

    operationA->IncreaseResourceUsageAndDemand(jobResourcesX);

    DoFairShareUpdate(totalResourceLimits, rootElement);

    EXPECT_FALSE(Dominates(TResourceVector::Ones(), pool->Attributes().FairShare.Total));
}

TEST_F(TFairShareUpdateTest, TestTruncateUnsatisfiedChildFairShareInFifoPools)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();

    auto rootElement = CreateRootElement();
    auto poolA = CreateSimplePool("poolA");
    auto poolB = CreateSimplePool("poolB");
    poolA->AttachParent(rootElement.Get());
    poolB->AttachParent(rootElement.Get());

    poolA->SetMode(ESchedulingMode::Fifo);
    poolA->SetFairShareTruncationInFifoPoolEnabled(true);
    poolB->SetMode(ESchedulingMode::Fifo);
    poolB->SetFairShareTruncationInFifoPoolEnabled(true);

    TJobResources resourceDemand;
    resourceDemand.SetUserSlots(30);
    resourceDemand.SetCpu(30);
    resourceDemand.SetMemory(300_MB);

    auto operationAFirst = CreateOperation(poolA.Get(), resourceDemand);

    auto operationASecond = CreateOperation(poolA.Get(), resourceDemand);
    operationASecond->SetIsGang(true);

    auto operationBFirst = CreateOperation(poolB.Get(), resourceDemand);

    auto operationBSecond = CreateOperation(poolB.Get(), resourceDemand);

    DoFairShareUpdate(totalResourceLimits, rootElement);

    const TResourceVector unit = {1.0, 1.0, 0.0, 1.0, 0.0};
    EXPECT_RV_NEAR(TResourceVector(unit * 0.3), operationAFirst->Attributes().DemandShare);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.3), operationASecond->Attributes().DemandShare);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.3), operationBFirst->Attributes().DemandShare);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.3), operationBSecond->Attributes().DemandShare);

    EXPECT_RV_NEAR(TResourceVector(unit * 0.3), operationAFirst->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.0), operationASecond->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.3), operationBFirst->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.2), operationBSecond->Attributes().FairShare.Total);

    EXPECT_RV_NEAR(TResourceVector(unit * 0.3), poolA->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.5), poolB->Attributes().FairShare.Total);
}

TEST_F(TFairShareUpdateTest, TestPromisedGuaranteeFairShare)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();

    auto rootElement = CreateRootElement();
    auto poolA = CreateSimplePool("poolA", /*strongGuaranteeCpu*/ 30.0);
    auto poolA1 = CreateSimplePool("poolA1", /*strongGuaranteeCpu*/ 10.0);
    auto poolA2 = CreateSimplePool("poolA2");
    auto poolA3 = CreateSimplePool("poolA3");
    auto poolB = CreateSimplePool("poolB", /*strongGuaranteeCpu*/ 30.0);
    poolA->AttachParent(rootElement.Get());
    poolA1->AttachParent(poolA.Get());
    poolA2->AttachParent(poolA.Get());
    poolA3->AttachParent(poolA.Get());
    poolB->AttachParent(rootElement.Get());

    poolA->SetPromisedGuaranteeFairShareComputationEnabled(true);
    // Test that only the uppermost pool's config takes effect.
    poolA2->SetPromisedGuaranteeFairShareComputationEnabled(true);

    auto largeResourceDemand = totalResourceLimits;
    auto operationA1 = CreateOperation(poolA1.Get(), largeResourceDemand);
    auto operationA2 = CreateOperation(poolA2.Get(), largeResourceDemand);
    auto operationB = CreateOperation(poolB.Get(), largeResourceDemand);

    TJobResources smallResourceDemand;
    smallResourceDemand.SetUserSlots(2);
    smallResourceDemand.SetCpu(2);
    smallResourceDemand.SetMemory(20_MB);

    auto operationA3 = CreateOperation(poolA3.Get(), smallResourceDemand);

    DoFairShareUpdate(totalResourceLimits, rootElement);

    const TResourceVector unit = {1.0, 1.0, 0.0, 1.0, 0.0};
    EXPECT_RV_NEAR(TResourceVector(unit * 0.29), operationA1->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.19), operationA2->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.02), operationA3->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.5), operationB->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.5), poolA->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.29), poolA1->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.19), poolA2->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.02), poolA3->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.5), poolB->Attributes().FairShare.Total);

    EXPECT_RV_NEAR(TResourceVector(unit * 0.19), operationA1->Attributes().PromisedGuaranteeFairShare.Total);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.09), operationA2->Attributes().PromisedGuaranteeFairShare.Total);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.02), operationA3->Attributes().PromisedGuaranteeFairShare.Total);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.0), operationB->Attributes().PromisedGuaranteeFairShare.Total);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.3), poolA->Attributes().PromisedGuaranteeFairShare.Total);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.19), poolA1->Attributes().PromisedGuaranteeFairShare.Total);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.09), poolA2->Attributes().PromisedGuaranteeFairShare.Total);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.02), poolA3->Attributes().PromisedGuaranteeFairShare.Total);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.0), poolB->Attributes().PromisedGuaranteeFairShare.Total);
    EXPECT_RV_NEAR(TResourceVector(unit * 0.0), rootElement->Attributes().PromisedGuaranteeFairShare.Total);
}

TEST_F(TFairShareUpdateTest, TestNestedPromisedGuaranteeFairSharePools)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();

    auto rootElement = CreateRootElement();
    auto poolA = CreateSimplePool("poolA");
    auto poolA1 = CreateSimplePool("poolA1");
    poolA->AttachParent(rootElement.Get());
    poolA1->AttachParent(poolA.Get());

    poolA->SetPromisedGuaranteeFairShareComputationEnabled(true);
    poolA1->SetPromisedGuaranteeFairShareComputationEnabled(true);

    auto checkErrors = [&] (const auto& errors) {
        for (const auto& error : errors) {
            if (error.FindMatching(EErrorCode::NestedPromisedGuaranteeFairSharePools)) {
                return true;
            }
        }
        return false;
    };

    {
        auto context = DoFairShareUpdate(totalResourceLimits, rootElement);
        EXPECT_TRUE(checkErrors(context.Errors));
    }

    poolA1->SetPromisedGuaranteeFairShareComputationEnabled(false);

    {
        auto context = DoFairShareUpdate(totalResourceLimits, rootElement);
        EXPECT_FALSE(checkErrors(context.Errors));
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TFairShareUpdateTest, TestRelaxedPoolFairShareSimple)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto relaxedPool = CreateRelaxedPool("relaxed", /*flowCpu*/ 10.0, /*strongGuaranteeCpu*/ 10.0);
    relaxedPool->AttachParent(rootElement.Get());

    auto operation = CreateOperation(relaxedPool.Get(), totalResourceLimits * 0.3);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_RV_NEAR(unit * 3, operation->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(unit * 3, operation->Attributes().FairShare.Total);

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

TEST_F(TFairShareUpdateTest, TestRelaxedPoolWithIncreasedMultiplierLimit)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto defaultRelaxedPool = CreateRelaxedPool("defaultRelaxed", /*flowCpu*/ 10.0);
    defaultRelaxedPool->AttachParent(rootElement.Get());

    auto increasedLimitRelaxedPool = CreateRelaxedPool("increasedLimitRelaxed", /*flowCpu*/ 10.0);
    increasedLimitRelaxedPool->IntegralGuaranteesConfig()->RelaxedShareMultiplierLimit = 5.0;
    increasedLimitRelaxedPool->AttachParent(rootElement.Get());

    auto operationX = CreateOperation(defaultRelaxedPool.Get(), /*resourceDemand*/ totalResourceLimits);
    auto operationY = CreateOperation(increasedLimitRelaxedPool.Get(), /*resourceDemand*/ totalResourceLimits);

    defaultRelaxedPool->InitAccumulatedResourceVolume(GetHugeVolume());
    increasedLimitRelaxedPool->InitAccumulatedResourceVolume(GetHugeVolume());

    {
        DoFairShareUpdate(totalResourceLimits, rootElement);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        // Default multiplier is 3.
        EXPECT_EQ(unit * 3, defaultRelaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_EQ(unit * 5, increasedLimitRelaxedPool->Attributes().FairShare.IntegralGuarantee);
    }
}

TEST_F(TFairShareUpdateTest, TestBurstPoolFairShareSimple)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto burstPool = CreateBurstPool("burst", /*flowCpu*/ 10.0, /*burstCpu*/ 10.0, /*strongGuaranteeCpu*/ 10.0);
    burstPool->AttachParent(rootElement.Get());

    auto operation = CreateOperation(burstPool.Get(), totalResourceLimits * 0.3);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_RV_NEAR(unit * 3, operation->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(unit * 3, operation->Attributes().FairShare.Total);

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

TEST_F(TFairShareUpdateTest, TestAccumulatedVolumeProvidesMore)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto relaxedPool = CreateRelaxedPool("relaxed", /*flowCpu*/ 10.0);
    relaxedPool->AttachParent(rootElement.Get());

    auto firstUpdateTime = TInstant::Now();
    {
        // Make first update to accumulate volume
        DoFairShareUpdate(
            totalResourceLimits,
            rootElement,
            /*now*/ firstUpdateTime,
            /*previousUpdateTime*/ firstUpdateTime - TDuration::Minutes(1));
    }

    auto operation = CreateOperation(relaxedPool.Get(), totalResourceLimits * 0.3);

    auto secondUpdateTime = firstUpdateTime + TDuration::Minutes(1);
    {
        DoFairShareUpdate(
            totalResourceLimits,
            rootElement,
            /*now*/ secondUpdateTime,
            /*previousUpdateTime*/ firstUpdateTime);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_RV_NEAR(unit * 3, operation->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(unit * 3, operation->Attributes().FairShare.Total);

        EXPECT_EQ(TResourceVector::Zero(), relaxedPool->Attributes().FairShare.StrongGuarantee);
        // Here we get two times more share ratio than guaranteed by flow.
        EXPECT_RV_NEAR(unit * 2, relaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit, relaxedPool->Attributes().FairShare.WeightProportional);
    }
}

TEST_F(TFairShareUpdateTest, TestStrongGuaranteePoolVsBurstPool)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto burstPool = CreateBurstPool("burst", /*flowCpu*/ 100.0, /*burstCpu*/ 50.0);
    burstPool->AttachParent(rootElement.Get());

    auto strongPool = CreateSimplePool("strong", /*strongGuaranteeCpu*/ 50.0);
    strongPool->AttachParent(rootElement.Get());

    auto burstOperation = CreateOperation(burstPool.Get(), totalResourceLimits);
    auto strongOperation = CreateOperation(strongPool.Get(), totalResourceLimits);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

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

TEST_F(TFairShareUpdateTest, TestStrongGuaranteePoolVsRelaxedPool)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto strongPool = CreateSimplePool("strong", /*strongGuaranteeCpu*/ 50.0);
    strongPool->AttachParent(rootElement.Get());

    auto relaxedPool = CreateRelaxedPool("relaxed", /*flowCpu*/ 100.0);
    relaxedPool->AttachParent(rootElement.Get());

    auto strongOperation = CreateOperation(strongPool.Get(), /*resourceDemand*/ totalResourceLimits);
    auto relaxedOperation = CreateOperation(relaxedPool.Get(), /*resourceDemand*/ totalResourceLimits);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

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

TEST_F(TFairShareUpdateTest, TestBurstGetsAll_RelaxedNone)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto burstPool = CreateBurstPool("burst", /*flowCpu*/ 100.0, /*burstCpu*/ 100.0);
    burstPool->AttachParent(rootElement.Get());

    auto relaxedPool = CreateRelaxedPool("relaxed", /*flowCpu*/ 100.0);
    relaxedPool->AttachParent(rootElement.Get());

    auto burstOperation = CreateOperation(burstPool.Get(), totalResourceLimits);
    auto relaxedOperation = CreateOperation(relaxedPool.Get(), totalResourceLimits);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

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

TEST_F(TFairShareUpdateTest, TestBurstGetsBurstGuaranteeOnly_RelaxedGetsRemaining)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto burstPool = CreateBurstPool("burst", /*flowCpu*/ 100.0, /*burstCpu*/ 50.0);
    burstPool->AttachParent(rootElement.Get());

    auto relaxedPool = CreateRelaxedPool("relaxed", /*flowCpu*/ 100);
    relaxedPool->AttachParent(rootElement.Get());

    auto burstOperation = CreateOperation(burstPool.Get(), totalResourceLimits);
    auto relaxedOperation = CreateOperation(relaxedPool.Get(), totalResourceLimits);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

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

TEST_F(TFairShareUpdateTest, TestAllKindsOfPoolsShareWeightProportionalComponent)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto strongPool = CreateSimplePool("strong", /*strongGuaranteeCpu*/ 10.0);
    strongPool->AttachParent(rootElement.Get());

    auto burstPool = CreateBurstPool("burst", /*flowCpu*/ 10.0, /*burstCpu*/ 10.0, /*strongGuaranteeCpu*/ 0.0);
    burstPool->AttachParent(rootElement.Get());

    auto relaxedPool = CreateRelaxedPool("relaxed", /*flowCpu*/ 10.0, /*strongGuaranteeCpu*/ 0.0, /*weight*/ 2.0);
    relaxedPool->AttachParent(rootElement.Get());

    auto noGuaranteePool = CreateSimplePool("noGuarantee", /*strongGuaranteeCpu*/ 0.0, /*weight*/ 3.0);
    noGuaranteePool->AttachParent(rootElement.Get());

    auto strongOperation = CreateOperation(strongPool.Get(), totalResourceLimits);
    auto burstOperation = CreateOperation(burstPool.Get(), totalResourceLimits);
    auto relaxedOperation = CreateOperation(relaxedPool.Get(), totalResourceLimits);
    auto noGuaranteeOperation = CreateOperation(noGuaranteePool.Get(), totalResourceLimits);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

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

TEST_F(TFairShareUpdateTest, TestTwoRelaxedPoolsGetShareRatioProportionalToVolume)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto relaxedPoolA = CreateRelaxedPool("relaxedA", /*flowCpu*/ 100.0);
    relaxedPoolA->AttachParent(rootElement.Get());

    auto relaxedPoolB = CreateRelaxedPool("relaxedB", /*flowCpu*/ 100.0);
    relaxedPoolB->AttachParent(rootElement.Get());

    auto relaxedOperationA = CreateOperation(relaxedPoolA.Get(), totalResourceLimits);
    auto relaxedOperationB = CreateOperation(relaxedPoolB.Get(), totalResourceLimits);

    // 10% of cluster for 1 minute.
    auto volume1 = TResourceVolume(GetOnePercentOfCluster() * 10.0, TDuration::Minutes(1));
    // 30% of cluster for 1 minute.
    auto volume2 = TResourceVolume(GetOnePercentOfCluster() * 30.0, TDuration::Minutes(1));
    relaxedPoolA->InitAccumulatedResourceVolume(volume1);
    relaxedPoolB->InitAccumulatedResourceVolume(volume2);
    {
        DoFairShareUpdate(totalResourceLimits, rootElement);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 0, relaxedPoolA->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 1, relaxedPoolA->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 3, relaxedPoolA->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 0, relaxedPoolB->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 3, relaxedPoolB->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 3, relaxedPoolB->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(unit * 0, rootElement->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 4, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 6, rootElement->Attributes().FairShare.WeightProportional);
    }
}

TEST_F(TFairShareUpdateTest, TestStrongGuaranteeAdjustmentToTotalResources)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto strongPoolA = CreateSimplePool("strongA", /*strongGuaranteeCpu*/ 30.0);
    strongPoolA->AttachParent(rootElement.Get());

    auto strongPoolB = CreateSimplePool("strongB", /*strongGuaranteeCpu*/ 90.0);
    strongPoolB->AttachParent(rootElement.Get());

    auto strongOperationA = CreateOperation(strongPoolA.Get(), totalResourceLimits);
    auto strongOperationB = CreateOperation(strongPoolB.Get(), totalResourceLimits);

    {
        DoFairShareUpdate(totalResourceLimits, rootElement);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 2.5, strongPoolA->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 0, strongPoolA->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, strongPoolA->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 7.5, strongPoolB->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit * 0, strongPoolB->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, strongPoolB->Attributes().FairShare.WeightProportional);
    }
}

TEST_F(TFairShareUpdateTest, TestStrongGuaranteePlusBurstGuaranteeAdjustmentToTotalResources)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto strongPool = CreateSimplePool("strong", /*strongGuaranteeCpu*/ 90.0);
    strongPool->AttachParent(rootElement.Get());

    auto burstPool = CreateBurstPool("burst", /*flowCpu*/ 60.0, /*burstCpu*/ 60.0);
    burstPool->AttachParent(rootElement.Get());

    auto strongOperation = CreateOperation(strongPool.Get(), totalResourceLimits);
    auto burstOperation = CreateOperation(burstPool.Get(), totalResourceLimits);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_RV_NEAR(unit * 6, strongPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 0, strongPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, strongPool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(unit * 0, burstPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 4, burstPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, burstPool->Attributes().FairShare.WeightProportional);
    }
}

TEST_F(TFairShareUpdateTest, TestLimitsLowerThanStrongGuarantee)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto strongPoolParent = CreateSimplePool("strongParent", /*strongGuaranteeCpu*/ 100.0);
    strongPoolParent->SetResourceLimits(CreateCpuResourceLimits(50.0));
    strongPoolParent->AttachParent(rootElement.Get());

    auto strongPoolChild = CreateSimplePool("strongChild", /*strongGuaranteeCpu*/ 100.0);
    strongPoolChild->AttachParent(strongPoolParent.Get());

    auto operation = CreateOperation(strongPoolChild.Get(), /*resourceDemand*/ totalResourceLimits);

    {
        DoFairShareUpdate(totalResourceLimits, rootElement);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 5, strongPoolParent->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 5, strongPoolParent->Attributes().FairShare.Total);

        EXPECT_EQ(unit * 5, strongPoolChild->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 5, strongPoolChild->Attributes().FairShare.Total);
    }
}

TEST_F(TFairShareUpdateTest, TestParentWithoutGuaranteeAndHisLimitsLowerThanChildBurstShare)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto limitedParent = CreateSimplePool("limitedParent", /*strongGuaranteeCpu*/ 0.0);
    limitedParent->SetResourceLimits(CreateCpuResourceLimits(50.0));
    limitedParent->AttachParent(rootElement.Get());

    auto burstChild = CreateBurstPool("burst", /*flowCpu*/ 100.0, /*burstCpu*/ 100.0, /*strongGuaranteeCpu*/ 0.0);
    burstChild->AttachParent(limitedParent.Get());

    auto operation = CreateOperation(burstChild.Get(), /*resourceDemand*/ totalResourceLimits);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 5, burstChild->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 5, burstChild->Attributes().FairShare.Total);
    }
}

TEST_F(TFairShareUpdateTest, TestParentWithStrongGuaranteeAndHisLimitsLowerThanChildBurstShare)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto limitedParent = CreateSimplePool("limitedParent", /*strongGuaranteeCpu*/ 50.0);
    limitedParent->SetResourceLimits(CreateCpuResourceLimits(50.0));
    limitedParent->AttachParent(rootElement.Get());

    auto burstChild = CreateBurstPool("burst", /*flowCpu*/ 10.0, /*burstCpu*/ 10.0, /*strongGuaranteeCpu*/ 0.0);
    burstChild->AttachParent(limitedParent.Get());

    auto operation = CreateOperation(burstChild.Get(), /*resourceDemand*/ totalResourceLimits);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 0, burstChild->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit * 0, burstChild->Attributes().FairShare.IntegralGuarantee);  // Integral share wasn't given due to violation of parent limits.
        EXPECT_RV_NEAR(unit * 5, burstChild->Attributes().FairShare.WeightProportional);
    }
}

TEST_F(TFairShareUpdateTest, TestStrongGuaranteeAndRelaxedPoolVsRelaxedPool)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto strongAndRelaxedPool = CreateRelaxedPool("min_share_and_relaxed", /*flowCpu*/ 100.0, /*strongGuaranteeCpu*/ 40.0);
    strongAndRelaxedPool->AttachParent(rootElement.Get());

    auto relaxedPool = CreateRelaxedPool("relaxed", /*flowCpu*/ 100.0);
    relaxedPool->AttachParent(rootElement.Get());

    auto strongAndRelaxedOperation = CreateOperation(strongAndRelaxedPool.Get(), /*resourceDemand*/ totalResourceLimits);
    auto relaxedOperation = CreateOperation(relaxedPool.Get(), /*resourceDemand*/ totalResourceLimits);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

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

TEST_F(TFairShareUpdateTest, PromisedFairShareOfIntegralPools)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto burstPoolParent = CreateSimplePool("burstParent");
    burstPoolParent->AttachParent(rootElement.Get());

    auto burstPool = CreateBurstPool("burst", /*flowCpu*/ 30.0, /*burstCpu*/ 100.0);
    burstPool->AttachParent(burstPoolParent.Get());

    auto relaxedPoolParent = CreateSimplePool("relaxedParent");
    relaxedPoolParent->AttachParent(rootElement.Get());

    auto relaxedPool = CreateRelaxedPool("relaxed", /*flowCpu*/ 70.0);
    relaxedPool->AttachParent(relaxedPoolParent.Get());

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_RV_NEAR(unit * 3, burstPool->Attributes().PromisedFairShare);
        EXPECT_RV_NEAR(unit * 3, burstPoolParent->Attributes().PromisedFairShare);

        EXPECT_RV_NEAR(unit * 7, relaxedPool->Attributes().PromisedFairShare);
        EXPECT_RV_NEAR(unit * 7, relaxedPoolParent->Attributes().PromisedFairShare);

        EXPECT_EQ(unit * 10, rootElement->Attributes().PromisedFairShare);
    }
}

TEST_F(TFairShareUpdateTest, TestIntegralPoolsWithParent)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto limitedParent = CreateIntegralPool("parent", EIntegralGuaranteeType::None, /*flowCpu*/ 100.0, /*burstCpu*/ 100.0);
    limitedParent->AttachParent(rootElement.Get());

    auto burstPool = CreateBurstPool("burst", /*flowCpu*/ 50.0, /*burstCpu*/ 100.0);
    burstPool->AttachParent(limitedParent.Get());

    auto relaxedPool = CreateRelaxedPool("relaxed", /*flowCpu*/ 50.0);
    relaxedPool->AttachParent(limitedParent.Get());

    auto burstOperation = CreateOperation(burstPool.Get(), totalResourceLimits);
    auto relaxedOperation = CreateOperation(relaxedPool.Get(), totalResourceLimits);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(
            totalResourceLimits,
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

TEST_F(TFairShareUpdateTest, TestProposedIntegralSharePrecisionError)
{
    // This test is based on real circumstances, nothing below is random or weird.
    // It works, and this is the most important thing. Enjoy.
    // See: YT-16653.
    TJobResources totalResourceLimits;
    totalResourceLimits.SetUserSlots(2404350);
    totalResourceLimits.SetCpu(285040.54);
    totalResourceLimits.SetMemory(1139022499379170);
    totalResourceLimits.SetNetwork(534300);
    totalResourceLimits.SetGpu(0);

    auto rootElement = CreateRootElement();
    auto integralRootPool = CreateSimplePool("integralRoot", /*strongGuaranteeCpu*/ 473159.00);
    auto burstPool = CreateBurstPool(
        "burstPool",
        /*flowCpu*/ 525.0,
        /*burstCpu*/ 3150.0,
        /*strongGuaranteeCpu*/ 0.0);
    auto firstRelaxedPool = CreateRelaxedPool(
        "firstRelaxedPool",
        /*flowCpu*/ 3500.0,
        /*burstCpu*/ {},
        /*strongGuaranteeCpu*/ 5158.0);

    auto secondRelaxedPool = CreateRelaxedPool(
        "secondRelaxedPool",
        // This is the only random value in this test.
        /*flowCpu*/ 117.0);

    // We need these two additional pools for correct guarantee adjustments.
    auto normalPool = CreateSimplePool("normalPool", /*strongGuaranteeCpu*/ 23071.0);
    auto fakeBurstPool = CreateBurstPool(
        "fakeBurstPool",
        /*flowCpu*/ 0.0,
        /*burstCpu*/ 11700.0);

    integralRootPool->AttachParent(rootElement.Get());
    burstPool->AttachParent(integralRootPool.Get());
    firstRelaxedPool->AttachParent(integralRootPool.Get());
    secondRelaxedPool->AttachParent(integralRootPool.Get());
    normalPool->AttachParent(rootElement.Get());
    fakeBurstPool->AttachParent(rootElement.Get());

    integralRootPool->SetResourceLimits(CreateCpuResourceLimits(50000.00));

    // Don't think we need exact amounts, because any large enough accumulated volume should work.
    TResourceVolume burstPoolAccumulatedVolume;
    burstPoolAccumulatedVolume.SetUserSlots(413098315.744941);
    burstPoolAccumulatedVolume.SetCpu(45360000.00);
    burstPoolAccumulatedVolume.SetMemory(2.1341920521797664e+17);
    burstPoolAccumulatedVolume.SetNetwork(91794223.939665511);
    burstPoolAccumulatedVolume.SetGpu(0);
    burstPool->InitAccumulatedResourceVolume(burstPoolAccumulatedVolume);

    TResourceVolume relaxedPoolAccumulatedVolume;
    relaxedPoolAccumulatedVolume.SetUserSlots(2541466175.1648531);
    relaxedPoolAccumulatedVolume.SetCpu(302400000.00);
    relaxedPoolAccumulatedVolume.SetMemory(1.3032554324247532e+18);
    relaxedPoolAccumulatedVolume.SetNetwork(564751061.14774621);
    relaxedPoolAccumulatedVolume.SetGpu(0);
    firstRelaxedPool->InitAccumulatedResourceVolume(relaxedPoolAccumulatedVolume);

    // Yes, we do need these 12 operations.
    // If we simply create a single operation with the total demand of 506 CPU, fair share wouldn't be the same.
    // Again, user slots and memory are unnecessary, but why not.
    std::vector<double> burstOperationCpuDemands = {60, 30, 14, 18, 60, 60, 140, 18, 18, 28, 18, 42};
    std::vector<int> burstOperationUserSlotDemands = {2, 1, 1, 2, 2, 2, 2, 5, 2, 2, 2, 2};
    std::vector<i64> burstOperationMemoryDemands = {3813532248, 1823451694, 1570583626, 2972646113, 382413122943, 3813532249, 11268640098, 2972646107, 2972646122, 3141167254, 2972646125, 3090478642};
    std::vector<TOperationElementMockPtr> burstOperations;
    for (int index = 0; index < std::ssize(burstOperationCpuDemands); ++index) {
        TJobResources operationDemand;
        operationDemand.SetUserSlots(burstOperationUserSlotDemands[index]);
        operationDemand.SetCpu(burstOperationCpuDemands[index]);
        operationDemand.SetMemory(burstOperationMemoryDemands[index]);
        operationDemand.SetNetwork(0);
        operationDemand.SetGpu(0);
        burstOperations.push_back(CreateOperation(burstPool.Get(), operationDemand));
    }

    TJobResources relaxedOperationDemand;
    relaxedOperationDemand.SetUserSlots(101836);
    relaxedOperationDemand.SetCpu(99835.63);
    relaxedOperationDemand.SetMemory(99376091257090);
    relaxedOperationDemand.SetNetwork(0);
    relaxedOperationDemand.SetGpu(0);
    auto relaxedOperation = CreateOperation(firstRelaxedPool.Get(), relaxedOperationDemand);

    {
        DoFairShareUpdate(totalResourceLimits, rootElement);

        const auto& attributes = integralRootPool->Attributes();
        EXPECT_TRUE(Dominates(attributes.LimitsShare, attributes.GetGuaranteeShare()));
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TFairShareUpdateTest, TestCrashInAdjustProposedIntegralShareOnUpdateBurstPoolIntegralShares)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto limitedStrongGuaranteeParent = CreateSimplePool("parent", /*strongGuaranteeCpu*/ 50.0);
    limitedStrongGuaranteeParent->AttachParent(rootElement.Get());

    TJobResources parentResourceLimits;
    parentResourceLimits.SetCpu(55);
    parentResourceLimits.SetMemory(totalResourceLimits.GetMemory());
    parentResourceLimits.SetUserSlots(totalResourceLimits.GetUserSlots());
    limitedStrongGuaranteeParent->SetResourceLimits(parentResourceLimits);


    auto burstChild1 = CreateIntegralPool("pool1", EIntegralGuaranteeType::Burst, /*flowCpu*/ 10.0, /*burstCpu*/ 10.0);
    burstChild1->AttachParent(limitedStrongGuaranteeParent.Get());

    auto burstChild2 = CreateIntegralPool("pool2", EIntegralGuaranteeType::Burst, /*flowCpu*/ 10.0, /*burstCpu*/ 10.0);
    burstChild2->AttachParent(limitedStrongGuaranteeParent.Get());

    TJobResources resourceDemand;
    resourceDemand.SetUserSlots(5);
    resourceDemand.SetCpu(5);
    resourceDemand.SetMemory(50_MB);

    auto op1 = CreateOperation(burstChild1.Get(), resourceDemand);
    auto op2 = CreateOperation(burstChild2.Get(), resourceDemand);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};

        // First pool gets integral guarantees until the gap between parent's limit and strong guarantee is filled.
        EXPECT_EQ(unit * 0, burstChild1->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit * 0.5, burstChild1->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0, burstChild1->Attributes().FairShare.WeightProportional);

        // The gap is filled by first pool. Second pool gets only weight proportional share.
        EXPECT_EQ(unit * 0, burstChild2->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(unit * 0, burstChild2->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(unit * 0.5, burstChild2->Attributes().FairShare.WeightProportional);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf
