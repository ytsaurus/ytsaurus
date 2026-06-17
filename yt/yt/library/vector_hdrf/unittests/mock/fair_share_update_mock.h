#pragma once

#include <yt/yt/library/vector_hdrf/fair_share_update.h>
#include <yt/yt/library/vector_hdrf/private.h>

#include <library/cpp/yt/memory/new.h>

#include <util/datetime/base.h>

namespace NYT::NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EElementType,
    (Operation)
    (Pool)
);

////////////////////////////////////////////////////////////////////////////////

TJobResources CreateCpuResourceLimits(double cpu);

////////////////////////////////////////////////////////////////////////////////

class TElementMock;
class TCompositeElementMock;
class TPoolElementMock;
class TRootElementMock;
class TOperationElementMock;

////////////////////////////////////////////////////////////////////////////////

struct TTestJobResourcesConfig
    : public TRefCounted
    , public NVectorHdrf::TJobResourcesConfig
{ };

using TTestJobResourcesConfigPtr = TIntrusivePtr<TTestJobResourcesConfig>;

////////////////////////////////////////////////////////////////////////////////

struct TPoolIntegralGuaranteesConfig
    : public TRefCounted
{
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
    explicit TElementMock(std::string id);

    const TJobResources& GetResourceDemand() const override;
    const TJobResources& GetResourceUsageAtUpdate() const override;
    const TJobResources& GetResourceLimits() const override;
    const TJobResourcesConfig* GetStrongGuaranteeResourcesConfig() const override;
    double GetWeight() const override;

    TSchedulableAttributes& Attributes() override;
    const TSchedulableAttributes& Attributes() const override;

    TCompositeElement* GetParentElement() const override;
    std::string GetId() const override;
    const NLogging::TLogger& GetLogger() const override;
    bool AreDetailedLogsEnabled() const override;

    virtual void AttachParent(TCompositeElementMock* parent);

    virtual void PreUpdate(const TJobResources& totalResourceLimits);

    void SetWeight(double weight);
    void SetStrongGuaranteeResourcesConfig(const TTestJobResourcesConfigPtr& strongGuaranteeResourcesConfig);
    void SetResourceLimits(const TJobResources& resourceLimits);

protected:
    const TString Id_;
    TCompositeElement* Parent_ = nullptr;

    const NLogging::TLogger Logger;

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
    explicit TCompositeElementMock(std::string id);

    TElement* GetChild(int index) override;
    const TElement* GetChild(int index) const override;
    int GetChildCount() const override;
    ESchedulingMode GetMode() const override;

    bool HasHigherPriorityInFifoMode(const TElement* lhs, const TElement* rhs) const override;

    double GetSpecifiedBurstRatio() const override;
    double GetSpecifiedResourceFlowRatio() const override;

    bool IsStepFunctionForGangOperationsEnabled() const override;
    bool CanAcceptFreeVolume() const override;
    bool ShouldDistributeFreeVolumeAmongChildren() const override;
    bool ShouldComputePromisedGuaranteeFairShare() const override;
    bool IsPriorityStrongGuaranteeAdjustmentEnabled() const override;
    bool IsPriorityStrongGuaranteeAdjustmentDonorshipEnabled() const override;

    void AddChild(TElementMock* child);

    void PreUpdate(const TJobResources& totalResourceLimits) override;

    void SetMode(ESchedulingMode mode);
    void SetPromisedGuaranteeFairShareComputationEnabled(bool enabled);

private:
    std::vector<TElementMockPtr> Children_;

    ESchedulingMode Mode_ = ESchedulingMode::FairShare;
    bool PromisedGuaranteeFairShareComputationEnabled_ = false;
};

using TCompositeElementMockPtr = TIntrusivePtr<TCompositeElementMock>;

////////////////////////////////////////////////////////////////////////////////

class TPoolElementMock
    : public TPool
    , public TCompositeElementMock
{
public:
    explicit TPoolElementMock(std::string id);

    TResourceVector GetIntegralShareLimitForRelaxedPool() const override;

    const TIntegralResourcesState& IntegralResourcesState() const override;
    TIntegralResourcesState& IntegralResourcesState() override;

    EIntegralGuaranteeType GetIntegralGuaranteeType() const override;

    void InitAccumulatedResourceVolume(TResourceVolume resourceVolume);

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
    explicit TOperationElementMock(std::string id);

    void SetResourceDemand(const TJobResources& resourceDemand);
    void SetResourceUsage(const TJobResources& resourceUsage);
    void IncreaseResourceUsageAndDemand(const TJobResources& delta);

    TResourceVector GetBestAllocationShare() const override;

    bool IsGangLike() const override;

    void SetGangFlag(bool value);

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
    TRootElementMock();
};

using TRootElementMockPtr = TIntrusivePtr<TRootElementMock>;

////////////////////////////////////////////////////////////////////////////////

struct TTestFairShareUpdateOptions
{
    EJobResourceType MainResource = EJobResourceType::Cpu;
    TInstant Now = TInstant::Now();
    std::optional<TInstant> PreviousUpdateTime;
    bool EnableStepFunctionForGangOperations = false;
    bool EnableImprovedFairShareByFitFactorComputation = false;
    // TODO(ignat): delete this option if no necessity will be found on production clusters.
    bool EnableImprovedFairShareByFitFactorComputationDistributionGap = false;
    bool EnableFastFifoFairShareByFitFactorComputation = false;
};

////////////////////////////////////////////////////////////////////////////////

void ResetFairShareFunctionsRecursively(TCompositeElementMock* compositeElement);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf
