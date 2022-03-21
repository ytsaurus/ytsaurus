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

DECLARE_REFCOUNTED_CLASS(TTestJobResourcesConfig)
DEFINE_REFCOUNTED_TYPE(TTestJobResourcesConfig)

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

DECLARE_REFCOUNTED_CLASS(TPoolIntegralGuaranteesConfig)
DEFINE_REFCOUNTED_TYPE(TPoolIntegralGuaranteesConfig)

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

    TElement* GetParentElement() const override
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

    NLogging::TLogger Logger_;

private:
    double Weight_ = 1.0;
    TJobResources ResourceLimits_ = TJobResources::Infinite();
    TTestJobResourcesConfigPtr StrongGuaranteeResourcesConfig_ = New<TTestJobResourcesConfig>();

};

DECLARE_REFCOUNTED_CLASS(TElementMock)
DEFINE_REFCOUNTED_TYPE(TElementMock)

////////////////////////////////////////////////////////////////////////////////

class TCompositeElementMock
    : public virtual TCompositeElement
    , public TElementMock
{
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

    int GetChildrenCount() const override
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

    DEFINE_BYREF_RW_PROPERTY(TPoolIntegralGuaranteesConfigPtr, IntegralGuaranteesConfig, New<TPoolIntegralGuaranteesConfig>());

private:
    std::vector<TElementMockPtr> Children_;
    
    ESchedulingMode Mode_ = ESchedulingMode::FairShare;
    bool FairShareTruncationInFifoPoolEnabled_ = false;
};

DECLARE_REFCOUNTED_CLASS(TCompositeElementMock)
DEFINE_REFCOUNTED_TYPE(TCompositeElementMock)

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

DECLARE_REFCOUNTED_CLASS(TPoolElementMock)
DEFINE_REFCOUNTED_TYPE(TPoolElementMock)

////////////////////////////////////////////////////////////////////////////////

class TOperationElementMock
    : public TOperationElement
    , public TElementMock
{
public:
    TOperationElementMock(
        TString id,
        const TJobResources& resourceDemand,
        const TJobResources& resourceUsage = TJobResources())
        : TElementMock(std::move(id))
    {
        ResourceDemand_ = resourceDemand;
        ResourceUsage_ = resourceUsage;
    }

    TResourceVector GetBestAllocationShare() const override
    {
        return TResourceVector::Ones();
    }

    bool IsGang() const override
    {
        return false;
    }
};

DECLARE_REFCOUNTED_CLASS(TOperationElementMock)
DEFINE_REFCOUNTED_TYPE(TOperationElementMock)

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

DECLARE_REFCOUNTED_CLASS(TRootElementMock)
DEFINE_REFCOUNTED_TYPE(TRootElementMock)

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
    TRootElementMockPtr CreateTestRootElement()
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

        TPoolIntegralGuaranteesConfigPtr integralGuaranteesConfig = New<TPoolIntegralGuaranteesConfig>();
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
        const TJobResources& resourceDemand,
        TCompositeElementMock* parent)
    {
        auto operationElement = New<TOperationElementMock>(Format("Operation_%v", OperationIndex_++), resourceDemand);
        operationElement->AttachParent(parent);
        return operationElement;
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

    void DoFairShareUpdate(
        const TJobResources& totalResourceLimits,
        const TRootElementMockPtr& rootElement,
        TInstant now = TInstant(),
        std::optional<TInstant> previousUpdateTime = std::nullopt)
    {
		NVectorHdrf::TFairShareUpdateContext context(
            totalResourceLimits,
            /*mainResource*/ EJobResourceType::Cpu,
            /*integralPoolCapacitySaturationPeriod*/ TDuration::Days(1),
            /*integralSmoothPeriod*/ TDuration::Minutes(1),
            now,
            previousUpdateTime);

        rootElement->PreUpdate(totalResourceLimits);

		NVectorHdrf::TFairShareUpdateExecutor updateExecutor(rootElement, &context);
		updateExecutor.Run();
    }

private:
    int OperationIndex_ = 0;
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

#define EXPECT_RESOURCE_VOLUME_NEAR(vector1, vector2) \
    EXPECT_THAT(vector2, ResourceVolumeNear(vector1, 1e-7))

////////////////////////////////////////////////////////////////////////////////

TEST_F(TFairShareUpdateTest, TestRelaxedPoolFairShareSimple)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateTestRootElement();

    auto relaxedPool = CreateRelaxedPool("relaxed", /*flowCpu*/ 10.0, /*strongGuaranteeCpu*/ 10.0);
    relaxedPool->AttachParent(rootElement.Get());

    auto operationElement = CreateOperation(totalResourceLimits * 0.3, relaxedPool.Get());

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

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

TEST_F(TFairShareUpdateTest, TestRelaxedPoolWithIncreasedMultiplierLimit)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateTestRootElement();

    auto defaultRelaxedPool = CreateRelaxedPool("defaultRelaxed", /*flowCpu*/ 10.0);
    defaultRelaxedPool->AttachParent(rootElement.Get());

    auto increasedLimitRelaxedPool = CreateRelaxedPool("increasedLimitRelaxed", /*flowCpu*/ 10.0);
    increasedLimitRelaxedPool->IntegralGuaranteesConfig()->RelaxedShareMultiplierLimit = 5.0;
    increasedLimitRelaxedPool->AttachParent(rootElement.Get());

    auto operation1 = CreateOperation(/*resourceDemand*/ totalResourceLimits, defaultRelaxedPool.Get());
    auto operation2 = CreateOperation(/*resourceDemand*/ totalResourceLimits, increasedLimitRelaxedPool.Get());

    defaultRelaxedPool->InitAccumulatedResourceVolume(GetHugeVolume());
    increasedLimitRelaxedPool->InitAccumulatedResourceVolume(GetHugeVolume());

    {
        DoFairShareUpdate(totalResourceLimits, rootElement);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 3, defaultRelaxedPool->Attributes().FairShare.IntegralGuarantee);  // Default multiplier is 3.
        EXPECT_EQ(unit * 5, increasedLimitRelaxedPool->Attributes().FairShare.IntegralGuarantee);
    }
}

TEST_F(TFairShareUpdateTest, TestStrongGuaranteePoolVsRelaxedPool)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateTestRootElement();

    auto strongPool = CreateSimplePool("strong", /*strongGuaranteeCpu*/ 50.0);
    strongPool->AttachParent(rootElement.Get());

    auto relaxedPool = CreateRelaxedPool("relaxed", /*flowCpu*/ 100.0);
    relaxedPool->AttachParent(rootElement.Get());

    auto strongOperation = CreateOperation(/*resourceDemand*/ totalResourceLimits, strongPool.Get());
    auto relaxedOperation = CreateOperation(/*resourceDemand*/ totalResourceLimits, relaxedPool.Get());

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

TEST_F(TFairShareUpdateTest, TestLimitsLowerThanStrongGuarantee)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateTestRootElement();

    auto strongPoolParent = CreateSimplePool("strongParent", /*strongGuaranteeCpu*/ 100.0);
    strongPoolParent->AttachParent(rootElement.Get());
    strongPoolParent->SetResourceLimits(CreateCpuResourceLimits(50));
    
    auto strongPoolChild = CreateSimplePool("strongChild", /*strongGuaranteeCpu*/ 100.0);
    strongPoolChild->AttachParent(strongPoolParent.Get());

    auto operation = CreateOperation(/*resourceDemand*/ totalResourceLimits, strongPoolChild.Get());

    {
        DoFairShareUpdate(totalResourceLimits, rootElement);

        TResourceVector unit = {0.1, 0.1, 0.0, 0.1, 0.0};
        EXPECT_EQ(unit * 5, strongPoolParent->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 5, strongPoolParent->Attributes().FairShare.Total);

        EXPECT_EQ(unit * 5, strongPoolChild->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(unit * 5, strongPoolChild->Attributes().FairShare.Total);
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

    auto rootElement = CreateTestRootElement();
    auto integralRootPool = CreateSimplePool("integralRoot", /*strongGuaranteeCpu*/ 473159.00);
    auto burstPool = CreateIntegralPool(
        "burstPool",
        EIntegralGuaranteeType::Burst,
        /*flowCpu*/ 525.0,
        /*burstCpu*/ 3150.0,
        /*strongGuaranteeCpu*/ 0.0);
    auto firstRelaxedPool = CreateIntegralPool(
        "firstRelaxedPool",
        EIntegralGuaranteeType::Relaxed,
        /*flowCpu*/ 3500.0,
        /*burstCpu*/ {},
        /*strongGuaranteeCpu*/ 5158.0);

    auto secondRelaxedPool = CreateIntegralPool(
        "secondRelaxedPool",
        EIntegralGuaranteeType::Relaxed,
        // This is the only random value in this test.
        /*flowCpu*/ 117.0);

    // We need these two additional pools for correct guarantee adjustments.
    auto normalPool = CreateSimplePool("normalPool", /*strongGuaranteeCpu*/ 23071.0);
    auto fakeBurstPool = CreateIntegralPool(
        "fakeBurstPool",
        EIntegralGuaranteeType::Burst,
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
        burstOperations.push_back(CreateOperation(operationDemand, burstPool.Get()));
    }

    TJobResources relaxedOperationDemand;
    relaxedOperationDemand.SetUserSlots(101836);
    relaxedOperationDemand.SetCpu(99835.63);
    relaxedOperationDemand.SetMemory(99376091257090);
    relaxedOperationDemand.SetNetwork(0);
    relaxedOperationDemand.SetGpu(0);
    auto relaxedOperation = CreateOperation(relaxedOperationDemand, firstRelaxedPool.Get());

    {
        DoFairShareUpdate(totalResourceLimits, rootElement);

        const auto& attributes = integralRootPool->Attributes();
        EXPECT_TRUE(Dominates(attributes.LimitsShare, attributes.GetGuaranteeShare()));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf
