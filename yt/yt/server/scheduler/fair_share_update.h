#pragma once

#include <yt/yt/server/scheduler/resource_vector.h>
#include <yt/yt/server/scheduler/resource_volume.h>

// Used for TJobResourcesConfig.
#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/ytlib/scheduler/job_resources.h>

namespace NYT::NFairShare {

////////////////////////////////////////////////////////////////////////////////

using NScheduler::TJobResources;
using NScheduler::TResourceVector;
using NScheduler::TResourceVolume;
using NScheduler::EJobResourceType;
using NScheduler::EIntegralGuaranteeType;
using NScheduler::TJobResourcesConfig;
using NScheduler::TJobResourcesConfigPtr;
using NScheduler::ESchedulingMode;
using NScheduler::ResourceCount;
using NScheduler::RatioComparisonPrecision;
using NScheduler::RatioComputationPrecision;

using TVectorPiecewiseSegment = NScheduler::TVectorPiecewiseSegment;
using TScalarPiecewiseSegment = NScheduler::TScalarPiecewiseSegment;
using TVectorPiecewiseLinearFunction = NScheduler::TVectorPiecewiseLinearFunction;
using TScalarPiecewiseLinearFunction = NScheduler::TScalarPiecewiseLinearFunction;

////////////////////////////////////////////////////////////////////////////////

class TFairShareUpdateExecutor;
struct TFairShareUpdateContext;

class TElement;
class TCompositeElement;
class TPool;
class TRootElement;
class TOperationElement;

////////////////////////////////////////////////////////////////////////////////

struct TDetailedFairShare
{
    TResourceVector StrongGuarantee = {};
    TResourceVector IntegralGuarantee = {};
    TResourceVector WeightProportional = {};
    TResourceVector Total = {};
};

TString ToString(const TDetailedFairShare& detailedFairShare);

void FormatValue(TStringBuilderBase* builder, const TDetailedFairShare& detailedFairShare, TStringBuf /* format */);

////////////////////////////////////////////////////////////////////////////////

struct TIntegralResourcesState
{
    TResourceVolume AccumulatedVolume;
    double LastShareRatio = 0.0;
};

////////////////////////////////////////////////////////////////////////////////

struct TSchedulableAttributes
{
    EJobResourceType DominantResource = EJobResourceType::Cpu;

    TDetailedFairShare FairShare;
    TResourceVector UsageShare;
    TResourceVector DemandShare;
    TResourceVector LimitsShare;
    TResourceVector StrongGuaranteeShare;
    TResourceVector ProposedIntegralShare;
    TResourceVector PromisedFairShare;

    TResourceVolume VolumeOverflow;
    TResourceVolume AcceptableVolume;
    TResourceVolume AcceptedFreeVolume;
    TResourceVolume ChildrenVolumeOverflow;

    TJobResources EffectiveStrongGuaranteeResources;

    double BurstRatio = 0.0;
    double TotalBurstRatio = 0.0;
    double ResourceFlowRatio = 0.0;
    double TotalResourceFlowRatio = 0.0;

    int FifoIndex = -1;

    // TODO(ignat): extract post update attributes to separate structure.
    // NB: calculated in postupdate.
    TJobResources UnschedulableOperationsResourceUsage;
    // NB: calculated in postupdate and used for diagnostics purposes.
    bool Alive = false;
    double SatisfactionRatio = 0.0;
    double LocalSatisfactionRatio = 0.0;

    TResourceVector GetGuaranteeShare() const;

    void SetFairShare(const TResourceVector& fairShare);
};

////////////////////////////////////////////////////////////////////////////////

TJobResources ToJobResources(const TJobResourcesConfigPtr& config, TJobResources defaultValue);

////////////////////////////////////////////////////////////////////////////////

class TElement
    : public virtual TRefCounted
{
public:
    virtual const TJobResources& GetResourceDemand() const = 0;
    virtual const TJobResources& GetResourceUsageAtUpdate() const = 0;
    // New method - should incapsulate ResourceLimits_ calculation logic and BestAllocation logic for operations.
    virtual const TJobResources& GetResourceLimits() const = 0;

    virtual TJobResourcesConfigPtr GetStrongGuaranteeResourcesConfig() const = 0;
    virtual double GetWeight() const = 0;

    virtual TSchedulableAttributes& Attributes() = 0;
    virtual const TSchedulableAttributes& Attributes() const = 0;

    virtual TElement* GetParentElement() const = 0;

    virtual bool IsRoot() const;
    virtual bool IsOperation() const;
    TPool* AsPool();

    virtual TString GetId() const = 0;

    virtual const NLogging::TLogger& GetLogger() const = 0;
    virtual bool AreDetailedLogsEnabled() const = 0;

    // It is public for testing purposes.
    void ResetFairShareFunctions();

private:
    bool AreFairShareFunctionsPrepared_ = false;
    std::optional<TVectorPiecewiseLinearFunction> FairShareByFitFactor_;
    std::optional<TVectorPiecewiseLinearFunction> FairShareBySuggestion_;
    std::optional<TScalarPiecewiseLinearFunction> MaxFitFactorBySuggestion_;

    virtual void PrepareFairShareFunctions(TFairShareUpdateContext* context);
    virtual void PrepareFairShareByFitFactor(TFairShareUpdateContext* context) = 0;
    void PrepareMaxFitFactorBySuggestion(TFairShareUpdateContext* context);

    virtual void DetermineEffectiveStrongGuaranteeResources(TFairShareUpdateContext* context);
    virtual void UpdateCumulativeAttributes(TFairShareUpdateContext* context);
    virtual TResourceVector DoUpdateFairShare(double suggestion, TFairShareUpdateContext* context) = 0;

    void CheckFairShareFeasibility() const;

    virtual TResourceVector ComputeLimitsShare(const TFairShareUpdateContext* context) const;
    void UpdateAttributes(const TFairShareUpdateContext* context);

    TResourceVector GetVectorSuggestion(double suggestion) const;

    virtual void AdjustStrongGuarantees(const TFairShareUpdateContext* context);
    virtual void InitIntegralPoolLists(TFairShareUpdateContext* context);
    virtual void DistributeFreeVolume();

    friend class TCompositeElement;
    friend class TPool;
    friend class TRootElement;
    friend class TOperationElement;
    friend class TFairShareUpdateExecutor;
};

DECLARE_REFCOUNTED_CLASS(TElement)
DEFINE_REFCOUNTED_TYPE(TElement)

////////////////////////////////////////////////////////////////////////////////

class TCompositeElement
    : public virtual TElement
{
public:
    virtual TElement* GetChild(int index) = 0;
    virtual const TElement* GetChild(int index) const = 0;
    virtual int GetChildrenCount() const = 0;

    virtual ESchedulingMode GetMode() const = 0;
    virtual bool HasHigherPriorityInFifoMode(const TElement* lhs, const TElement* rhs) const = 0;

    virtual double GetSpecifiedBurstRatio() const = 0;
    virtual double GetSpecifiedResourceFlowRatio() const = 0;

    virtual bool ShouldTruncateUnsatisfiedChildFairShareInFifoPool() const = 0;
    virtual bool CanAcceptFreeVolume() const = 0;
    virtual bool ShouldDistributeFreeVolumeAmongChildren() const = 0;

private:
    using TChildSuggestions = std::vector<double>;

    std::vector<TElement*> SortedChildren_;

    virtual void PrepareFairShareFunctions(TFairShareUpdateContext* context) override;
    virtual void PrepareFairShareByFitFactor(TFairShareUpdateContext* context) override;
    void PrepareFairShareByFitFactorFifo(TFairShareUpdateContext* context);
    void PrepareFairShareByFitFactorNormal(TFairShareUpdateContext* context);

    virtual void AdjustStrongGuarantees(const TFairShareUpdateContext* context) override;
    virtual void InitIntegralPoolLists(TFairShareUpdateContext* context) override;
    virtual void DetermineEffectiveStrongGuaranteeResources(TFairShareUpdateContext* context) override;
    virtual void UpdateCumulativeAttributes(TFairShareUpdateContext* context) override;
    void UpdateOverflowAndAcceptableVolumesRecursively();
    virtual void DistributeFreeVolume() override;
    virtual TResourceVector DoUpdateFairShare(double suggestion, TFairShareUpdateContext* context) override;

    void PrepareFifoPool();

    double GetMinChildWeight() const;

    /// strict_mode = true means that a caller guarantees that the sum predicate is true at least for fit factor = 0.0.
    /// strict_mode = false means that if the sum predicate is false for any fit factor, we fit children to the least possible sum
    /// (i. e. use fit factor = 0.0)
    template <class TValue, class TGetter, class TSetter>
    TValue ComputeByFitting(
        const TGetter& getter,
        const TSetter& setter,
        TValue maxSum,
        bool strictMode = true);

    TChildSuggestions GetChildSuggestionsFifo(double fitFactor);
    TChildSuggestions GetChildSuggestionsNormal(double fitFactor);

    friend class TPool;
    friend class TRootElement;
    friend class TFairShareUpdateExecutor;
};

DECLARE_REFCOUNTED_CLASS(CompositeElement)
DEFINE_REFCOUNTED_TYPE(TCompositeElement)

////////////////////////////////////////////////////////////////////////////////

class TPool
    : public virtual TCompositeElement
{
public:
    // NB: it is combination of options on pool and on tree.
	virtual TResourceVector GetIntegralShareLimitForRelaxedPool() const = 0;

    virtual const TIntegralResourcesState& IntegralResourcesState() const = 0;
    virtual TIntegralResourcesState& IntegralResourcesState() = 0;

    virtual EIntegralGuaranteeType GetIntegralGuaranteeType() const = 0;

private:
    virtual void InitIntegralPoolLists(TFairShareUpdateContext* context) override;

    void UpdateAccumulatedResourceVolume(TFairShareUpdateContext* context);

    friend class TFairShareUpdateExecutor;
};

DECLARE_REFCOUNTED_CLASS(TPool)
DEFINE_REFCOUNTED_TYPE(TPool)

////////////////////////////////////////////////////////////////////////////////

class TRootElement
    : public virtual TCompositeElement
{
public:
    virtual bool IsRoot() const override;

private:
    virtual void DetermineEffectiveStrongGuaranteeResources(TFairShareUpdateContext* context) override;
    virtual void UpdateCumulativeAttributes(TFairShareUpdateContext* context) override;

    void ValidateAndAdjustSpecifiedGuarantees(TFairShareUpdateContext* context);

    friend class TElement;
    friend class TCompositeElement;
    friend class TFairShareUpdateExecutor;
};

DECLARE_REFCOUNTED_CLASS(TRootElement)
DEFINE_REFCOUNTED_TYPE(TRootElement)

////////////////////////////////////////////////////////////////////////////////

class TOperationElement
    : public virtual TElement
{
public:
    virtual bool IsOperation() const override;

    virtual TResourceVector GetBestAllocationShare() const = 0;

private:
    virtual void PrepareFairShareByFitFactor(TFairShareUpdateContext* context) override;

    virtual TResourceVector DoUpdateFairShare(double suggestion, TFairShareUpdateContext* context) override;
    virtual TResourceVector ComputeLimitsShare(const TFairShareUpdateContext* context) const override;

    friend class TFairShareUpdateExecutor;
};

DECLARE_REFCOUNTED_CLASS(TOperationElement)
DEFINE_REFCOUNTED_TYPE(TOperationElement)

////////////////////////////////////////////////////////////////////////////////

struct TFairShareUpdateContext
{
    TFairShareUpdateContext(
        const TJobResources totalResourceLimits,
        const EJobResourceType mainResource,
        const TDuration integralPoolCapacitySaturationPeriod,
        const TDuration integralSmoothPeriod,
        const TInstant now,
        const std::optional<TInstant> previousUpdateTime);

    const TJobResources TotalResourceLimits;

    const EJobResourceType MainResource;
    const TDuration IntegralPoolCapacitySaturationPeriod;
    const TDuration IntegralSmoothPeriod;

    const TInstant Now;
    const std::optional<TInstant> PreviousUpdateTime;

    std::vector<TError> Errors;

    NProfiling::TCpuDuration PrepareFairShareByFitFactorTotalTime = {};
    NProfiling::TCpuDuration PrepareFairShareByFitFactorOperationsTotalTime = {};
    NProfiling::TCpuDuration PrepareFairShareByFitFactorFifoTotalTime = {};
    NProfiling::TCpuDuration PrepareFairShareByFitFactorNormalTotalTime = {};
    NProfiling::TCpuDuration PrepareMaxFitFactorBySuggestionTotalTime = {};
    NProfiling::TCpuDuration PointwiseMinTotalTime = {};
    NProfiling::TCpuDuration ComposeTotalTime = {};
    NProfiling::TCpuDuration CompressFunctionTotalTime = {};

    std::vector<TPool*> RelaxedPools;
    std::vector<TPool*> BurstPools;
};

////////////////////////////////////////////////////////////////////////////////

class TFairShareUpdateExecutor
{
public:
    TFairShareUpdateExecutor(
        const TRootElementPtr& rootElement,
        // TODO(ignat): split context on input and output parts.
        TFairShareUpdateContext* context);

    void Run();

private:
    const TRootElementPtr RootElement_;

    void ConsumeAndRefillIntegralPools();
    void UpdateBurstPoolIntegralShares();
	void UpdateRelaxedPoolIntegralShares();
    void UpdateRootFairShare();

	double GetIntegralShareRatioByVolume(const TPool* pool) const;
	TResourceVector GetHierarchicalAvailableLimitsShare(const TElement* element) const;
	void IncreaseHierarchicalIntegralShare(TElement* element, const TResourceVector& delta);

    TFairShareUpdateContext* Context_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFairShare

