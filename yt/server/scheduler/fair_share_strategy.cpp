#include "fair_share_strategy.h"
#include "public.h"
#include "config.h"
#include "job_resources.h"
#include "master_connector.h"
#include "scheduler_strategy.h"

#include <yt/core/concurrency/async_rw_lock.h>
#include <yt/core/misc/finally.h>
#include <yt/core/profiling/scoped_timer.h>

#include <forward_list>

namespace NYT {
namespace NScheduler {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NObjectClient;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;
static const auto& Profiler = SchedulerProfiler;

static const double RatioComputationPrecision = std::numeric_limits<double>::epsilon();
static const double RatioComparisonPrecision = sqrt(RatioComputationPrecision);

static const int GlobalAttributesIndex = 0;

////////////////////////////////////////////////////////////////////

struct ISchedulerElement;
typedef TIntrusivePtr<ISchedulerElement> ISchedulerElementPtr;

class TOperationElement;
typedef TIntrusivePtr<TOperationElement> TOperationElementPtr;

class TCompositeSchedulerElement;
typedef TIntrusivePtr<TCompositeSchedulerElement> TCompositeSchedulerElementPtr;

class TPool;
typedef TIntrusivePtr<TPool> TPoolPtr;

class TRootElement;
typedef TIntrusivePtr<TRootElement> TRootElementPtr;

struct TFairShareContext;

class TDynamicAttributesMap;

////////////////////////////////////////////////////////////////////

struct TSchedulableAttributes
{
    EResourceType DominantResource = EResourceType::Cpu;
    double DemandRatio = 0.0;
    double UsageRatio = 0.0;
    double FairShareRatio = 0.0;
    double AdjustedMinShareRatio = 0.0;
    double MaxPossibleUsageRatio = 1.0;
    double BestAllocationRatio = 1.0;
    i64 DominantLimit = 0;
};

struct TDynamicAttributes
{
    double SatisfactionRatio = 0.0;
    bool Active = false;
    ISchedulerElementPtr BestLeafDescendant;
    TInstant MinSubtreeStartTime;
    TJobResources ResourceUsageDiscount = ZeroJobResources();
};

DEFINE_ENUM(ESchedulableStatus,
    (Normal)
    (BelowMinShare)
    (BelowFairShare)
);

////////////////////////////////////////////////////////////////////

struct ISchedulerElement
    : public TIntrinsicRefCounted
{
    virtual void Update() = 0;
    virtual void UpdateBottomUp() = 0;
    virtual void UpdateTopDown() = 0;

    virtual void BeginHeartbeat() = 0;
    virtual void UpdateDynamicAttributes(int attributesIndex) = 0;
    virtual void PrescheduleJob(TFairShareContext& context, bool starvingOnly) = 0;
    virtual bool ScheduleJob(TFairShareContext& context) = 0;
    virtual void EndHeartbeat() = 0;

    virtual const TSchedulableAttributes& Attributes() const = 0;
    virtual TSchedulableAttributes& Attributes() = 0;
    virtual void UpdateAttributes() = 0;

    virtual const TDynamicAttributes& DynamicAttributes(int attributesIndex) const = 0;
    virtual TDynamicAttributes& DynamicAttributes(int attributesIndex) = 0;

    virtual bool IsActive(int attributsIndex) const = 0;

    virtual int GetPendingJobCount() const = 0;

    virtual Stroka GetId() const = 0;

    virtual double GetWeight() const = 0;
    virtual double GetMinShareRatio() const = 0;
    virtual double GetMaxShareRatio() const = 0;

    virtual ESchedulableStatus GetStatus() const = 0;

    virtual bool GetStarving() const = 0;
    virtual void SetStarving(bool starving) = 0;
    virtual void CheckForStarvation(TInstant now) = 0;

    virtual const TJobResources& ResourceDemand() const = 0;
    virtual const TJobResources& ResourceUsage() const = 0;
    virtual const TJobResources& ResourceLimits() const = 0;
    virtual const TJobResources& MaxPossibleResourceUsage() const = 0;

    virtual void IncreaseUsage(const TJobResources& delta) = 0;
};

////////////////////////////////////////////////////////////////////

struct TFairShareContext
{
    TFairShareContext(
        const ISchedulingContextPtr& schedulingContext,
        int attributesIndex)
        : SchedulingContext(schedulingContext)
        , AttributesIndex(attributesIndex)
    { }

    const ISchedulingContextPtr SchedulingContext;
    int AttributesIndex;
};

class TDynamicAttributesList
{
public:
    void Initialize(int index)
    {
        while (index >= DynamicAttributesListIterators_.size()) {
            DynamicAttributesList_.emplace_front();
            DynamicAttributesListIterators_.push_back(DynamicAttributesList_.begin());
        }
        *DynamicAttributesListIterators_[index] = TDynamicAttributes();
    }

    TDynamicAttributes& Get(int index)
    {
        YCHECK(index < DynamicAttributesListIterators_.size());
        return *DynamicAttributesListIterators_[index];
    }

    const TDynamicAttributes& Get(int index) const
    {
        YCHECK(index < DynamicAttributesListIterators_.size());
        return *DynamicAttributesListIterators_[index];
    }

    bool IsActive(int index) const
    {
        if (index >= DynamicAttributesListIterators_.size()) {
            return false;
        }
        return DynamicAttributesListIterators_[index]->Active;
    }

private:
    std::forward_list<TDynamicAttributes> DynamicAttributesList_;
    std::vector<std::forward_list<TDynamicAttributes>::iterator> DynamicAttributesListIterators_;
};

////////////////////////////////////////////////////////////////////

class TSchedulerElementBase
    : public ISchedulerElement
{
public:
    virtual void Update() override
    {
        UpdateBottomUp();
        UpdateTopDown();
    }

    // Updates attributes that need to be computed from leafs up to root.
    // For example: parent->ResourceDemand = Sum(child->ResourceDemand).
    virtual void UpdateBottomUp() override
    {
        UpdateAttributes();
        DynamicAttributes(GlobalAttributesIndex).Active = true;
        UpdateDynamicAttributes(GlobalAttributesIndex);
    }

    // Updates attributes that are propagated from root down to leafs.
    // For example: child->FairShareRatio = fraction(parent->FairShareRatio).
    virtual void UpdateTopDown() override
    { }

    virtual void BeginHeartbeat() override
    { }

    virtual void UpdateDynamicAttributes(int attributesIndex) override
    {
        YCHECK(IsActive(attributesIndex));
        DynamicAttributes(attributesIndex).SatisfactionRatio = ComputeLocalSatisfactionRatio();
    }

    virtual void PrescheduleJob(TFairShareContext& context, bool starvingOnly) override
    {
        UpdateDynamicAttributes(context.AttributesIndex);
    }

    virtual void EndHeartbeat() override
    { }

    virtual TNullable<Stroka> GetSchedulingTag() const
    {
        return Null;
    }

    virtual void UpdateAttributes() override
    {
        // Choose dominant resource types, compute max share ratios, compute demand ratios.
        auto demand = ResourceDemand();
        auto usage = ResourceUsage();
        auto totalLimits = Host->GetTotalResourceLimits();

        auto maxPossibleResourceUsage = Min(totalLimits, MaxPossibleResourceUsage());

        if (usage == ZeroJobResources()) {
            Attributes_.DominantResource = GetDominantResource(demand, totalLimits);
        } else {
            Attributes_.DominantResource = GetDominantResource(usage, totalLimits);
        }

        i64 dominantDemand = GetResource(demand, Attributes_.DominantResource);
        i64 dominantUsage = GetResource(usage, Attributes_.DominantResource);
        i64 dominantLimit = GetResource(totalLimits, Attributes_.DominantResource);

        Attributes_.DemandRatio =
            dominantLimit == 0 ? 1.0 : (double) dominantDemand / dominantLimit;

        Attributes_.UsageRatio =
            dominantLimit == 0 ? 1.0 : (double) dominantUsage / dominantLimit;

        Attributes_.DominantLimit = dominantLimit;

        Attributes_.MaxPossibleUsageRatio = GetMaxShareRatio();
        if (Attributes_.UsageRatio > RatioComputationPrecision) {
            // In this case we know pool resource preferences and can take them into account.
            // We find maximum number K such that Usage * K < Limit and use it to estimate
            // maximum dominant resource usage.
            Attributes_.MaxPossibleUsageRatio = std::min(
                GetMinResourceRatio(maxPossibleResourceUsage, usage) * Attributes_.UsageRatio,
                Attributes_.MaxPossibleUsageRatio);
        } else {
            // In this case we have no information about pool resource preferences, so just assume
            // that it uses all resources equally.
            Attributes_.MaxPossibleUsageRatio = std::min(
                Attributes_.DemandRatio,
                Attributes_.MaxPossibleUsageRatio);
        }
    }

    virtual const TDynamicAttributes& DynamicAttributes(int attributesIndex) const override
    {
        return DynamicAttributesList_.Get(attributesIndex);
    }

    virtual TDynamicAttributes& DynamicAttributes(int attributesIndex) override
    {
        return DynamicAttributesList_.Get(attributesIndex);
    }

    virtual bool IsActive(int attributesIndex) const override
    {
        return DynamicAttributesList_.IsActive(attributesIndex);
    }

    ESchedulableStatus GetStatus(double defaultTolerance) const
    {
        double usageRatio = Attributes_.UsageRatio;
        double demandRatio = Attributes_.DemandRatio;

        double tolerance =
            demandRatio < Attributes_.FairShareRatio + RatioComparisonPrecision
            ? 1.0
            : defaultTolerance;

        if (usageRatio > Attributes_.FairShareRatio * tolerance - RatioComparisonPrecision) {
            return ESchedulableStatus::Normal;
        }

        return usageRatio < Attributes_.AdjustedMinShareRatio
               ? ESchedulableStatus::BelowMinShare
               : ESchedulableStatus::BelowFairShare;
    }

    ESchedulableStatus GetStatus() const override
    {
        return ESchedulableStatus::Normal;
    }

    void CheckForStarvation(
        TDuration minSharePreemptionTimeout,
        TDuration fairSharePreemptionTimeout,
        TInstant now)
    {
        auto status = GetStatus();
        switch (status) {
            case ESchedulableStatus::BelowMinShare:
                if (!GetBelowFairShareSince()) {
                    SetBelowFairShareSince(now);
                } else if (GetBelowFairShareSince().Get() < now - minSharePreemptionTimeout) {
                    SetStarving(true);
                }
                break;

            case ESchedulableStatus::BelowFairShare:
                if (!GetBelowFairShareSince()) {
                    SetBelowFairShareSince(now);
                } else if (GetBelowFairShareSince().Get() < now - fairSharePreemptionTimeout) {
                    SetStarving(true);
                }
                break;

            case ESchedulableStatus::Normal:
                SetBelowFairShareSince(Null);
                SetStarving(false);
                break;

            default:
                YUNREACHABLE();
        }
    }

    virtual void CheckForStarvation(TInstant now) override
    {
        YUNREACHABLE();
    }

    void IncreaseUsageRatio(const TJobResources& delta)
    {
        if (Attributes_.DominantLimit != 0) {
            i64 dominantDeltaUsage = GetResource(delta, Attributes_.DominantResource);
            Attributes_.UsageRatio += (double) dominantDeltaUsage / Attributes_.DominantLimit;
        } else {
            Attributes_.UsageRatio = 1.0;
        }
    }

    virtual void IncreaseUsage(const TJobResources& delta) override
    { }

    DEFINE_BYREF_RW_PROPERTY(TSchedulableAttributes, Attributes);
    DEFINE_BYVAL_RW_PROPERTY(bool, Starving);
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, BelowFairShareSince);

protected:
    ISchedulerStrategyHost* const Host;

    TDynamicAttributesList DynamicAttributesList_;

    explicit TSchedulerElementBase(ISchedulerStrategyHost* host)
        : Starving_(false)
        , Host(host)
    {
        DynamicAttributesList_.Initialize(GlobalAttributesIndex);
    }

    double ComputeLocalSatisfactionRatio() const
    {
        double minShareRatio = Attributes_.AdjustedMinShareRatio;
        double fairShareRatio = Attributes_.FairShareRatio;
        double usageRatio = Attributes_.UsageRatio;

        // Check for corner cases.
        if (fairShareRatio < RatioComputationPrecision) {
            return std::numeric_limits<double>::max();
        }

        if (minShareRatio > RatioComputationPrecision && usageRatio < minShareRatio) {
            // Needy element, negative satisfaction.
            return usageRatio / minShareRatio - 1.0;
        } else {
            // Regular element, positive satisfaction.
            return usageRatio / fairShareRatio;
        }
    }
};

////////////////////////////////////////////////////////////////////

class TCompositeSchedulerElement
    : public TSchedulerElementBase
{
public:
    explicit TCompositeSchedulerElement(ISchedulerStrategyHost* host)
        : TSchedulerElementBase(host)
        , Parent_(nullptr)
        , ResourceDemand_(ZeroJobResources())
        , ResourceUsage_(ZeroJobResources())
        , ResourceLimits_(InfiniteJobResources())
        , Mode(ESchedulingMode::Fifo)
    { }

    virtual void UpdateBottomUp() override
    {
        PendingJobCount = 0;
        ResourceDemand_ = ZeroJobResources();
        auto maxPossibleChildrenResourceUsage_ = ZeroJobResources();
        Attributes_.BestAllocationRatio = 0.0;
        for (const auto& child : Children) {
            child->UpdateBottomUp();

            ResourceDemand_ += child->ResourceDemand();
            maxPossibleChildrenResourceUsage_ += child->MaxPossibleResourceUsage();
            Attributes_.BestAllocationRatio = std::max(
                Attributes_.BestAllocationRatio,
                child->Attributes().BestAllocationRatio);

            PendingJobCount += child->GetPendingJobCount();
        }
        MaxPossibleResourceUsage_ = Min(maxPossibleChildrenResourceUsage_, ResourceLimits_);
        TSchedulerElementBase::UpdateBottomUp();
    }

    virtual void UpdateTopDown() override
    {
        switch (Mode) {
            case ESchedulingMode::Fifo:
                // Easy case -- the first child get everything, others get none.
                UpdateFifo();
                break;

            case ESchedulingMode::FairShare:
                // Hard case -- compute fair shares using fit factor.
                UpdateFairShare();
                break;

            default:
                YUNREACHABLE();
        }

        // Propagate updates to children.
        for (const auto& child : Children) {
            child->UpdateTopDown();
        }
    }

    virtual void BeginHeartbeat() override
    {
        TSchedulerElementBase::BeginHeartbeat();
        for (const auto& child : Children) {
            child->BeginHeartbeat();
        }
    }

    virtual void UpdateDynamicAttributes(int attributesIndex) override
    {
        YCHECK(IsActive(attributesIndex));
        auto& attributes = DynamicAttributes(attributesIndex);

        // Compute local satisfaction ratio.
        attributes.SatisfactionRatio = ComputeLocalSatisfactionRatio();
        // Start times bubble up from leaf nodes with operations.
        attributes.MinSubtreeStartTime = TInstant::Max();
        // Adjust satisfaction ratio using children.
        // Declare the element passive if all children are passive.
        attributes.Active = false;
        attributes.BestLeafDescendant.Reset();

        while (auto bestChild = GetBestActiveChild(attributesIndex)) {
            const auto& bestChildAttributes = bestChild->DynamicAttributes(attributesIndex);
            const auto& childBestLeafDescendant = bestChildAttributes.BestLeafDescendant;
            if (!childBestLeafDescendant->IsActive(GlobalAttributesIndex)) {
                bestChild->UpdateDynamicAttributes(attributesIndex);
                if (!bestChildAttributes.Active) {
                    continue;
                }
                YCHECK(childBestLeafDescendant->IsActive(GlobalAttributesIndex));
            }

            // We need to evaluate both MinSubtreeStartTime and SatisfactionRatio
            // because parent can use different scheduling mode.
            attributes.MinSubtreeStartTime = std::min(
                attributes.MinSubtreeStartTime,
                bestChildAttributes.MinSubtreeStartTime);

            attributes.SatisfactionRatio = std::min(
                attributes.SatisfactionRatio,
                bestChildAttributes.SatisfactionRatio);

            attributes.BestLeafDescendant = bestChildAttributes.BestLeafDescendant;
            attributes.Active = true;
            break;
        }
    }

    virtual void PrescheduleJob(TFairShareContext& context, bool starvingOnly) override
    {
        DynamicAttributesList_.Initialize(context.AttributesIndex);
        auto& attributes = DynamicAttributes(context.AttributesIndex);

        attributes.Active = true;

        if (!context.SchedulingContext->CanSchedule(GetSchedulingTag())) {
            attributes.Active = false;
            return;
        }

        for (const auto& child : Children) {
            // If pool is starving, any child will do.
            if (Starving_) {
                child->PrescheduleJob(context, false);
            } else {
                child->PrescheduleJob(context, starvingOnly);
            }
        }

        TSchedulerElementBase::PrescheduleJob(context, starvingOnly);
    }

    virtual bool ScheduleJob(TFairShareContext& context) override
    {
        auto& attributes = DynamicAttributes(context.AttributesIndex);
        if (!attributes.Active) {
            return false;
        }

        auto bestLeafDescendant = attributes.BestLeafDescendant;
        if (!bestLeafDescendant->IsActive(GlobalAttributesIndex)) {
            // NB: This can only happen as a result of deletion of bestLeafDescendant node
            // from scheduling tree in another fiber (e.x. operation abort),
            // while this fiber was waiting for controller.
            UpdateDynamicAttributes(context.AttributesIndex);
            if (!attributes.Active) {
                return false;
            }
            bestLeafDescendant = attributes.BestLeafDescendant;
            YCHECK(bestLeafDescendant->IsActive(GlobalAttributesIndex));
        }

        // NB: Ignore the child's result.
        bestLeafDescendant->ScheduleJob(context);
        return true;
    }

    virtual void EndHeartbeat() override
    {
        TSchedulerElementBase::EndHeartbeat();
        for (const auto& child : Children) {
            child->EndHeartbeat();
        }
    }

    virtual int GetPendingJobCount() const override
    {
        return PendingJobCount;
    }

    virtual bool IsRoot() const
    {
        return false;
    }

    void AddChild(const ISchedulerElementPtr& child, bool enabled = true)
    {
        if (enabled) {
            YCHECK(Children.insert(child).second);
        } else {
            YCHECK(DisabledChildren.insert(child).second);
        }
    }

    void EnableChild(const ISchedulerElementPtr& child)
    {
        auto it = DisabledChildren.find(child);
        YCHECK(it != DisabledChildren.end());
        Children.insert(child);
        DisabledChildren.erase(it);
    }

    void RemoveChild(const ISchedulerElementPtr& child)
    {
        bool foundInChildren = (Children.find(child) != Children.end());
        bool foundInDisabledChildren = (DisabledChildren.find(child) != DisabledChildren.end());
        YCHECK((foundInChildren && !foundInDisabledChildren) || (!foundInChildren && foundInDisabledChildren));
        if (foundInChildren) {
            Children.erase(child);
        } else {
            DisabledChildren.erase(child);
        }
    }

    bool IsEmpty() const
    {
        return Children.empty() && DisabledChildren.empty();
    }

    virtual int GetMaxOperationCount() const = 0;
    virtual int GetMaxRunningOperationCount() const = 0;

    DEFINE_BYVAL_RW_PROPERTY(TCompositeSchedulerElement*, Parent);

    DEFINE_BYREF_RW_PROPERTY(TJobResources, ResourceDemand);
    DEFINE_BYREF_RW_PROPERTY(TJobResources, ResourceUsage);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceLimits);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, MaxPossibleResourceUsage);

protected:
    ESchedulingMode Mode;
    std::vector<EFifoSortParameter> FifoSortParameters;
    int PendingJobCount;

    yhash_set<ISchedulerElementPtr> Children;
    yhash_set<ISchedulerElementPtr> DisabledChildren;

    // Given a non-descending continuous |f|, |f(0) = 0|, and a scalar |a|,
    // computes |x \in [0,1]| s.t. |f(x) = a|.
    // If |f(1) < a| then still returns 1.
    template <class F>
    static double BinarySearch(const F& f, double a)
    {
        if (f(1) < a) {
            return 1.0;
        }

        double lo = 0.0;
        double hi = 1.0;
        while (hi - lo > RatioComputationPrecision) {
            double x = (lo + hi) / 2.0;
            if (f(x) < a) {
                lo = x;
            } else {
                hi = x;
            }
        }
        return (lo + hi) / 2.0;
    }

    template <class TGetter, class TSetter>
    void ComputeByFitting(
        const TGetter& getter,
        const TSetter& setter,
        double sum)
    {
        auto getSum = [&] (double fitFactor) -> double {
            double sum = 0.0;
            for (const auto& child : Children) {
                sum += getter(fitFactor, child);
            }
            return sum;
        };

        // Run binary search to compute fit factor.
        double fitFactor = BinarySearch(getSum, sum);

        // Compute actual min shares from fit factor.
        for (const auto& child : Children) {
            double value = getter(fitFactor, child);
            setter(child, value);
        }
    }


    void UpdateFifo()
    {
        const auto& bestChild = GetBestActiveChildFifo(GlobalAttributesIndex);
        for (const auto& child : Children) {
            auto& childAttributes = child->Attributes();
            if (child == bestChild) {
                childAttributes.AdjustedMinShareRatio = std::min(
                    childAttributes.DemandRatio,
                    Attributes_.AdjustedMinShareRatio);
                childAttributes.FairShareRatio = std::min(
                    childAttributes.DemandRatio,
                    Attributes_.FairShareRatio);
            } else {
                childAttributes.AdjustedMinShareRatio = 0.0;
                childAttributes.FairShareRatio = 0.0;
            }
        }
    }

    void UpdateFairShare()
    {
        // Compute min shares.
        // Compute min weight.
        double minShareSum = 0.0;
        double minWeight = 1.0;
        for (const auto& child : Children) {
            auto& childAttributes = child->Attributes();
            double result = child->GetMinShareRatio();
            // Never give more than can be used.
            result = std::min(result, childAttributes.MaxPossibleUsageRatio);
            // Never give more than we can allocate.
            result = std::min(result, childAttributes.BestAllocationRatio);
            childAttributes.AdjustedMinShareRatio = result;
            minShareSum += result;

            if (child->GetWeight() > RatioComputationPrecision) {
                minWeight = std::min(minWeight, child->GetWeight());
            }
        }

        // Normalize min shares, if needed.
        if (minShareSum > Attributes_.AdjustedMinShareRatio) {
            double fitFactor = Attributes_.AdjustedMinShareRatio / minShareSum;
            for (const auto& child : Children) {
                auto& childAttributes = child->Attributes();
                childAttributes.AdjustedMinShareRatio *= fitFactor;
            }
        }

        // Compute fair shares.
        ComputeByFitting(
            [&] (double fitFactor, const ISchedulerElementPtr& child) -> double {
                const auto& childAttributes = child->Attributes();
                double result = fitFactor * child->GetWeight() / minWeight;
                // Never give less than promised by min share.
                result = std::max(result, childAttributes.AdjustedMinShareRatio);
                // Never give more than can be used.
                result = std::min(result, childAttributes.MaxPossibleUsageRatio);
                // Never give more than we can allocate.
                result = std::min(result, childAttributes.BestAllocationRatio);
                return result;
            },
            [&] (const ISchedulerElementPtr& child, double value) {
                auto& attributes = child->Attributes();
                attributes.FairShareRatio = value;
            },
            Attributes_.FairShareRatio);
    }


    ISchedulerElementPtr GetBestActiveChild(int attributesIndex) const
    {
        switch (Mode) {
            case ESchedulingMode::Fifo:
                return GetBestActiveChildFifo(attributesIndex);
            case ESchedulingMode::FairShare:
                return GetBestActiveChildFairShare(attributesIndex);
            default:
                YUNREACHABLE();
        }
    }

    ISchedulerElementPtr GetBestActiveChildFifo(int attributesIndex) const
    {
        auto isBetter = [this, &attributesIndex] (const ISchedulerElementPtr& lhs, const ISchedulerElementPtr& rhs) -> bool {
            for (auto parameter : FifoSortParameters) {
                switch (parameter) {
                    case EFifoSortParameter::Weight:
                        if (lhs->GetWeight() != rhs->GetWeight()) {
                            return lhs->GetWeight() > rhs->GetWeight();
                        }
                        break;
                    case EFifoSortParameter::StartTime: {
                        const auto& lhsStartTime = lhs->DynamicAttributes(attributesIndex).MinSubtreeStartTime;
                        const auto& rhsStartTime = rhs->DynamicAttributes(attributesIndex).MinSubtreeStartTime;
                        if (lhsStartTime != rhsStartTime) {
                            return lhsStartTime < rhsStartTime;
                        }
                        break;
                    }
                    case EFifoSortParameter::PendingJobCount: {
                        int lhsPendingJobCount = lhs->GetPendingJobCount();
                        int rhsPendingJobCount = rhs->GetPendingJobCount();
                        if (lhsPendingJobCount != rhsPendingJobCount) {
                            return lhsPendingJobCount < rhsPendingJobCount;
                        }
                        break;
                    }
                    default:
                        YUNREACHABLE();
                }
            }
            return false;
        };

        ISchedulerElement* bestChild = nullptr;
        for (const auto& child : Children) {
            if (child->IsActive(attributesIndex)) {
                if (bestChild && isBetter(bestChild, child))
                    continue;

                bestChild = child.Get();
            }
        }
        return bestChild;
    }
    ISchedulerElementPtr GetBestActiveChildFairShare(int attributesIndex) const
    {
        ISchedulerElement* bestChild = nullptr;
        double bestChildSatisfactionRatio = std::numeric_limits<double>::max();
        for (const auto& child : Children) {
            if (child->IsActive(attributesIndex)) {
                double childSatisfactionRatio = child->DynamicAttributes(attributesIndex).SatisfactionRatio;
                if (!bestChild || childSatisfactionRatio < bestChildSatisfactionRatio)
                {
                    bestChild = child.Get();
                    bestChildSatisfactionRatio = childSatisfactionRatio;
                }
            }
        }
        return bestChild;
    }
};

////////////////////////////////////////////////////////////////////

class TPool
    : public TCompositeSchedulerElement
{
public:
    TPool(
        ISchedulerStrategyHost* host,
        const Stroka& id,
        TFairShareStrategyConfigPtr strategyConfig)
        : TCompositeSchedulerElement(host)
        , Id(id)
        , StrategyConfig_(strategyConfig)
    {
        SetDefaultConfig();
    }


    bool IsDefaultConfigured() const
    {
        return DefaultConfigured;
    }

    TPoolConfigPtr GetConfig()
    {
        return Config_;
    }

    void SetConfig(TPoolConfigPtr config)
    {
        DoSetConfig(config);
        DefaultConfigured = false;
    }

    void SetDefaultConfig()
    {
        DoSetConfig(New<TPoolConfig>());
        DefaultConfigured = true;
    }

    virtual Stroka GetId() const override
    {
        return Id;
    }

    virtual double GetWeight() const override
    {
        return Config_->Weight;
    }

    virtual double GetMinShareRatio() const override
    {
        return Config_->MinShareRatio;
    }

    virtual double GetMaxShareRatio() const override
    {
        return Config_->MaxShareRatio;
    }

    virtual ESchedulableStatus GetStatus() const override
    {
        return TSchedulerElementBase::GetStatus(
            Config_->FairShareStarvationTolerance.Get(StrategyConfig_->FairShareStarvationTolerance));
    }

    virtual void SetStarving(bool starving) override
    {
        if (starving && !GetStarving()) {
            TSchedulerElementBase::SetStarving(true);
            LOG_INFO("Pool is now starving (PoolId: %v, Status: %v)",
                GetId(),
                GetStatus());
        } else if (!starving && GetStarving()) {
            TSchedulerElementBase::SetStarving(false);
            LOG_INFO("Pool is no longer starving (PoolId: %v)",
                GetId());
        }
    }

    virtual void CheckForStarvation(TInstant now) override
    {
        TSchedulerElementBase::CheckForStarvation(
            Config_->MinSharePreemptionTimeout.Get(StrategyConfig_->MinSharePreemptionTimeout),
            Config_->FairSharePreemptionTimeout.Get(StrategyConfig_->FairSharePreemptionTimeout),
            now);
    }

    virtual TNullable<Stroka> GetSchedulingTag() const override
    {
        return Config_->SchedulingTag;
    }

    virtual void UpdateBottomUp() override
    {
        ResourceLimits_ = ComputeResourceLimits();
        TCompositeSchedulerElement::UpdateBottomUp();
    }

    virtual void IncreaseUsage(const TJobResources& delta) override
    {
        TCompositeSchedulerElement* currentPool = this;
        while (currentPool) {
            currentPool->ResourceUsage() += delta;
            currentPool->IncreaseUsageRatio(delta);
            currentPool = currentPool->GetParent();
        }
    }

    virtual int GetMaxRunningOperationCount() const override
    {
        return Config_->MaxRunningOperations
            ? *(Config_->MaxRunningOperations)
            : StrategyConfig_->MaxRunningOperationsPerPool;
    }

    virtual int GetMaxOperationCount() const override
    {
        return Config_->MaxOperations
            ? *(Config_->MaxOperations)
            : StrategyConfig_->MaxOperationsPerPool;
    }

private:
    const Stroka Id;
    const TFairShareStrategyConfigPtr StrategyConfig_;

    TPoolConfigPtr Config_;
    bool DefaultConfigured;


    void DoSetConfig(TPoolConfigPtr newConfig)
    {
        Config_ = newConfig;

        bool update = false;
        if (FifoSortParameters != Config_->FifoSortParameters || Mode != Config_->Mode) {
            update = true;
        }

        FifoSortParameters = Config_->FifoSortParameters;
        Mode = Config_->Mode;

        if (update) {
            Update();
        }
    }

    TJobResources ComputeResourceLimits() const
    {
        auto poolLimits = Host->GetResourceLimits(GetSchedulingTag()) * Config_->MaxShareRatio;
        return Min(poolLimits, Config_->ResourceLimits->ToJobResources());
    }

};

////////////////////////////////////////////////////////////////////

class TOperationElement
    : public TSchedulerElementBase
{
public:
    TOperationElement(
        TFairShareStrategyConfigPtr config,
        TStrategyOperationSpecPtr spec,
        TOperationRuntimeParamsPtr runtimeParams,
        ISchedulerStrategyHost* host,
        TOperationPtr operation)
        : TSchedulerElementBase(host)
        , Operation_(operation)
        , Spec_(spec)
        , RuntimeParams_(runtimeParams)
        , Pool_(nullptr)
        , ResourceUsage_(ZeroJobResources())
        , NonpreemptableResourceUsage_(ZeroJobResources())
        , Config(config)
        , OperationId_(Operation_->GetId())
    {
        auto& attributes = DynamicAttributes(GlobalAttributesIndex);
        attributes.Active = true;
        attributes.MinSubtreeStartTime = operation->GetStartTime();
        attributes.BestLeafDescendant = this;
    }

    virtual void UpdateBottomUp() override
    {
        TSchedulerElementBase::UpdateBottomUp();

        auto totalLimits = Host->GetTotalResourceLimits();
        auto allocationLimits = GetAdjustedResourceLimits(
            ResourceDemand_,
            totalLimits,
            Host->GetExecNodeCount());

        i64 dominantLimit = GetResource(totalLimits, Attributes_.DominantResource);
        i64 dominantAllocationLimit = GetResource(allocationLimits, Attributes_.DominantResource);

        Attributes_.BestAllocationRatio =
            dominantLimit == 0 ? 1.0 : (double) dominantAllocationLimit / dominantLimit;
    }

    virtual void PrescheduleJob(TFairShareContext& context, bool starvingOnly) override
    {
        DynamicAttributesList_.Initialize(context.AttributesIndex);
        auto& attributes = DynamicAttributes(context.AttributesIndex);
        attributes = DynamicAttributes(GlobalAttributesIndex);

        attributes.Active = true;

        if (!context.SchedulingContext->CanSchedule(GetSchedulingTag())) {
            attributes.Active = false;
            return;
        }

        if (starvingOnly && !Starving_) {
            attributes.Active = false;
            return;
        }

        if (!Operation_->IsSchedulable()) {
            attributes.Active = false;
            return;
        }

        TSchedulerElementBase::PrescheduleJob(context, starvingOnly);
    }

    virtual bool ScheduleJob(TFairShareContext& context) override
    {
        YCHECK(IsActive(GlobalAttributesIndex));

        auto updateAncestorsAttributes = [&] () {
            TCompositeSchedulerElement* pool = Pool_;
            while (pool) {
                pool->UpdateDynamicAttributes(context.AttributesIndex);
                pool = pool->GetParent();
            }
        };

        if (!Operation_->IsSchedulable() ||
            ConcurrentScheduleJobCalls_ >= Config->MaxConcurrentControllerScheduleJobCalls)
        {
            DynamicAttributes(context.AttributesIndex).Active = false;
            updateAncestorsAttributes();
            return false;
        }

        ++ConcurrentScheduleJobCalls_;
        auto scheduleJobGuard = Finally([&] {
            --ConcurrentScheduleJobCalls_;
        });

        NProfiling::TScopedTimer timer;
        auto jobStartRequest = DoScheduleJob(context);
        auto scheduleJobDuration = timer.GetElapsed();

        // This can happen if operation controller was canceled after invocation
        // of IOperationController::ScheduleJob. In this case cancel won't be applied
        // to the last action in invoker but its result should be ignored.
        if (!IsActive(GlobalAttributesIndex)) {
            return false;
        }

        if (!jobStartRequest) {
            DynamicAttributes(context.AttributesIndex).Active = false;
            updateAncestorsAttributes();
            Operation_->UpdateControllerTimeStatistics("/schedule_job/fail", scheduleJobDuration);
            return false;
        }

        context.SchedulingContext->ResourceUsage() += jobStartRequest->ResourceLimits;
        OnJobStarted(jobStartRequest->Id, jobStartRequest->ResourceLimits);
        context.SchedulingContext->StartJob(Operation_, jobStartRequest);

        UpdateDynamicAttributes(context.AttributesIndex);
        updateAncestorsAttributes();
        Operation_->UpdateControllerTimeStatistics("/schedule_job/success", scheduleJobDuration);

        return true;
    }

    virtual int GetPendingJobCount() const override
    {
        const auto& controller = Operation_->GetController();
        return controller->GetPendingJobCount();
    }

    virtual Stroka GetId() const override
    {
        return ToString(OperationId_);
    }

    virtual double GetWeight() const override
    {
        return RuntimeParams_->Weight;
    }

    virtual double GetMinShareRatio() const override
    {
        return Spec_->MinShareRatio;
    }

    virtual double GetMaxShareRatio() const override
    {
        return Spec_->MaxShareRatio;
    }

    virtual TNullable<Stroka> GetSchedulingTag() const override
    {
        return Spec_->SchedulingTag;
    }

    virtual const TJobResources& ResourceDemand() const override
    {
        if (Operation_->IsSchedulable()) {
            const auto& controller = Operation_->GetController();
            ResourceDemand_ = ResourceUsage_ + controller->GetNeededResources();
        } else {
            ResourceDemand_ = ZeroJobResources();
        }
        return ResourceDemand_;
    }

    virtual const TJobResources& ResourceLimits() const override
    {
        ResourceLimits_ = Host->GetResourceLimits(GetSchedulingTag()) * Spec_->MaxShareRatio;

        auto perTypeLimits = Spec_->ResourceLimits->ToJobResources();
        ResourceLimits_ = Min(ResourceLimits_, perTypeLimits);

        return ResourceLimits_;
    }

    virtual const TJobResources& MaxPossibleResourceUsage() const override
    {
        MaxPossibleResourceUsage_ = Min(ResourceLimits(), ResourceDemand());
        return MaxPossibleResourceUsage_;
    }

    ESchedulableStatus GetStatus() const
    {
        if (!Operation_->IsSchedulable()) {
            return ESchedulableStatus::Normal;
        }

        if (GetPendingJobCount() == 0) {
            return ESchedulableStatus::Normal;
        }

        return TSchedulerElementBase::GetStatus(
            Spec_->FairShareStarvationTolerance.Get(Config->FairShareStarvationTolerance));
    }

    virtual void SetStarving(bool starving) override
    {
        if (starving && !GetStarving()) {
            TSchedulerElementBase::SetStarving(true);
            LOG_INFO("Operation is now starving (OperationId: %v, Status: %v)",
                GetId(),
                GetStatus());
        } else if (!starving && GetStarving()) {
            TSchedulerElementBase::SetStarving(false);
            LOG_INFO("Operation is no longer starving (OperationId: %v)",
                GetId());
        }
    }

    virtual void CheckForStarvation(TInstant now) override
    {
        auto minSharePreemptionTimeout = Spec_->MinSharePreemptionTimeout.Get(Config->MinSharePreemptionTimeout);
        auto fairSharePreemptionTimeout = Spec_->FairSharePreemptionTimeout.Get(Config->FairSharePreemptionTimeout);

        int jobCount = Operation_->GetController()->GetPendingJobCount();
        double jobCountRatio = jobCount / Config->JobCountPreemptionTimeoutCoefficient;

        if (jobCountRatio < 1.0) {
            minSharePreemptionTimeout *= jobCountRatio;
            fairSharePreemptionTimeout *= jobCountRatio;
        }

        TSchedulerElementBase::CheckForStarvation(
            minSharePreemptionTimeout,
            fairSharePreemptionTimeout,
            now);
    }

    virtual void IncreaseUsage(const TJobResources& delta) override
    {
        ResourceUsage() += delta;
        IncreaseUsageRatio(delta);
        GetPool()->IncreaseUsage(delta);
    }

    bool HasStarvingParent() const
    {
        TCompositeSchedulerElement* pool = GetPool();
        while (pool) {
            if (pool->GetStarving()) {
                return true;
            }
            pool = pool->GetParent();
        }
        return false;
    }

    void IncreaseJobResourceUsage(const TJobId& jobId, const TJobResources& resourcesDelta)
    {
        auto& properties = JobPropertiesMap_.at(jobId);
        properties.ResourceUsage += resourcesDelta;
        if (!properties.Preemptable) {
            NonpreemptableResourceUsage_ += resourcesDelta;
        }
        IncreaseUsage(resourcesDelta);
        UpdatePreemptableJobsList();
    }

    void UpdatePreemptableJobsList()
    {
        auto limits = Host->GetTotalResourceLimits();

        auto getNonpreemptableUsageRatio = [&] (const TJobResources& extraResources) -> double {
            i64 usage = GetResource(
                NonpreemptableResourceUsage_ + extraResources,
                Attributes_.DominantResource);
            i64 limit = GetResource(limits, Attributes_.DominantResource);
            return limit == 0 ? 1.0 : (double) usage / limit;
        };

        // Remove nonpreemptable jobs exceeding the fair share.
        while (!NonpreemptableJobs_.empty()) {
            if (getNonpreemptableUsageRatio(ZeroJobResources()) <= Attributes_.FairShareRatio) {
                break;
            }

            auto jobId = NonpreemptableJobs_.back();
            auto& jobProperties = JobPropertiesMap_.at(jobId);
            YCHECK(!jobProperties.Preemptable);

            NonpreemptableJobs_.pop_back();
            NonpreemptableResourceUsage_ -= jobProperties.ResourceUsage;

            PreemptableJobs_.push_front(jobId);

            jobProperties.Preemptable = true;
            jobProperties.JobIdListIterator = PreemptableJobs_.begin();
        }

        // Add more nonpreemptable jobs until filling up the fair share.
        while (!PreemptableJobs_.empty()) {
            auto jobId = PreemptableJobs_.front();
            auto& jobProperties = JobPropertiesMap_.at(jobId);
            YCHECK(jobProperties.Preemptable);

            if (getNonpreemptableUsageRatio(jobProperties.ResourceUsage) > Attributes_.FairShareRatio) {
                break;
            }

            PreemptableJobs_.pop_front();

            NonpreemptableJobs_.push_back(jobId);
            NonpreemptableResourceUsage_ += jobProperties.ResourceUsage;

            jobProperties.Preemptable = false;
            jobProperties.JobIdListIterator = --NonpreemptableJobs_.end();
        }
    }

    bool IsJobExisting(const TJobId& jobId) const
    {
        return JobPropertiesMap_.find(jobId) != JobPropertiesMap_.end();
    }

    bool IsJobPreemptable(const TJobId& jobId) const
    {
        return JobPropertiesMap_.at(jobId).Preemptable;
    }

    void OnJobStarted(const TJobId& jobId, const TJobResources& resourceUsage)
    {
        PreemptableJobs_.push_back(jobId);

        auto it = JobPropertiesMap_.insert(std::make_pair(
            jobId,
            TJobProperties(true, --PreemptableJobs_.end(), ZeroJobResources())));
        YCHECK(it.second);

        IncreaseJobResourceUsage(jobId, resourceUsage);
    }

    void OnJobFinished(const TJobId& jobId)
    {
        auto it = JobPropertiesMap_.find(jobId);
        YCHECK(it != JobPropertiesMap_.end());

        auto& properties = it->second;

        if (properties.Preemptable) {
            PreemptableJobs_.erase(properties.JobIdListIterator);
        } else {
            NonpreemptableJobs_.erase(properties.JobIdListIterator);
        }
        IncreaseJobResourceUsage(jobId, -properties.ResourceUsage);

        JobPropertiesMap_.erase(it);
    }

    DEFINE_BYVAL_RO_PROPERTY(TOperationPtr, Operation);
    DEFINE_BYVAL_RO_PROPERTY(TStrategyOperationSpecPtr, Spec);
    DEFINE_BYVAL_RO_PROPERTY(TOperationRuntimeParamsPtr, RuntimeParams);
    DEFINE_BYVAL_RW_PROPERTY(TPool*, Pool);
    DEFINE_BYREF_RW_PROPERTY(TJobResources, ResourceUsage);

    typedef std::list<TJobId> TJobIdList;

    DEFINE_BYREF_RW_PROPERTY(TJobIdList, NonpreemptableJobs);
    DEFINE_BYREF_RW_PROPERTY(TJobIdList, PreemptableJobs);

    DEFINE_BYREF_RW_PROPERTY(TJobResources, NonpreemptableResourceUsage);

private:
    mutable TJobResources ResourceDemand_;
    mutable TJobResources ResourceLimits_;
    mutable TJobResources MaxPossibleResourceUsage_;

    const TFairShareStrategyConfigPtr Config;
    const TOperationId OperationId_;

    int ConcurrentScheduleJobCalls_ = 0;

    TJobResources GetHierarchicalResourceLimits(const TFairShareContext& context) const
    {
        const auto& schedulingContext = context.SchedulingContext;

        // Bound limits with node free resources.
        auto limits =
            schedulingContext->ResourceLimits()
            - schedulingContext->ResourceUsage()
            + schedulingContext->ResourceUsageDiscount();

        // Bound limits with pool free resources.
        TCompositeSchedulerElement* pool = Pool_;
        while (pool) {
            auto poolLimits =
                pool->ResourceLimits()
                - pool->ResourceUsage()
                + pool->DynamicAttributes(context.AttributesIndex).ResourceUsageDiscount;

            limits = Min(limits, poolLimits);
            pool = pool->GetParent();
        }

        // Bound limits with operation free resources.
        limits = Min(limits, ResourceLimits() - ResourceUsage());

        return limits;
    }

    TJobStartRequestPtr DoScheduleJob(TFairShareContext& context)
    {
        auto jobLimits = GetHierarchicalResourceLimits(context);
        auto controller = Operation_->GetController();

        auto jobStartRequestFuture = BIND(&IOperationController::ScheduleJob, controller)
            .AsyncVia(controller->GetCancelableInvoker())
            .Run(context.SchedulingContext, jobLimits);

        auto jobStartRequestFutureWithTimeout = jobStartRequestFuture
            .WithTimeout(Config->ControllerScheduleJobTimeLimit);

        auto jobStartRequestWithTimeoutOrError = WaitFor(jobStartRequestFutureWithTimeout);

        if (!jobStartRequestWithTimeoutOrError.IsOK()) {
            if (jobStartRequestWithTimeoutOrError.GetCode() == NYT::EErrorCode::Timeout) {
                LOG_WARNING("Controller is scheduling for too long, aborting ScheduleJob");
                // If ScheduleJob was not canceled we need to abort created job.
                jobStartRequestFuture.Subscribe(
                    BIND([=] (const TErrorOr<TJobStartRequestPtr>& jobStartRequestOrError) {
                        if (jobStartRequestOrError.IsOK()) {
                            auto jobStartRequest = jobStartRequestOrError.Value();
                            if (jobStartRequest) {
                                const auto& jobId = jobStartRequest->Id;
                                LOG_WARNING("Aborting late job (JobId: %v, OperationId: %v)",
                                    jobId,
                                    OperationId_);
                                controller->OnJobAborted(
                                    std::make_unique<TAbortedJobSummary>(
                                        jobId,
                                        EAbortReason::SchedulingTimeout));
                            }
                        }
                }));
            } else {
                // This can happen if operation is aborted from control thread during ScheduleJob call.
                // In this case current operation element is removed from scheduling tree and we don't
                // need to update parent attributes.
                YCHECK(!IsActive(GlobalAttributesIndex));
            }
            return nullptr;
        }

        return jobStartRequestWithTimeoutOrError.Value();
    }

    // Fair share strategy stuff.
    struct TJobProperties
    {
        TJobProperties(
            bool preemptable,
            TJobIdList::iterator jobIdListIterator,
            const TJobResources& resourceUsage)
            : Preemptable(preemptable)
            , JobIdListIterator(jobIdListIterator)
            , ResourceUsage(resourceUsage)
        { }

        //! Determines the per-operation list (either preemptable or non-preemptable) this
        //! job belongs to.
        bool Preemptable;

        //! Iterator in the per-operation list pointing to this particular job.
        TJobIdList::iterator JobIdListIterator;

        TJobResources ResourceUsage;
    };

    yhash_map<TJobId, TJobProperties> JobPropertiesMap_;
};

////////////////////////////////////////////////////////////////////

class TRootElement
    : public TCompositeSchedulerElement
{
public:
    TRootElement(
        ISchedulerStrategyHost* host,
        TFairShareStrategyConfigPtr strategyConfig)
        : TCompositeSchedulerElement(host)
        , StrategyConfig_(strategyConfig)
    {
        Attributes_.FairShareRatio = 1.0;
        Attributes_.AdjustedMinShareRatio = 1.0;
        Mode = ESchedulingMode::FairShare;
        Update();
    }

    virtual bool IsRoot() const override
    {
        return true;
    }

    virtual Stroka GetId() const override
    {
        return Stroka(RootPoolName);
    }

    virtual double GetWeight() const override
    {
        return 1.0;
    }

    virtual double GetMinShareRatio() const override
    {
        return 0.0;
    }

    virtual double GetMaxShareRatio() const override
    {
        return 1.0;
    }

    virtual TNullable<Stroka> GetSchedulingTag() const override
    {
        return Null;
    }

    virtual int GetMaxRunningOperationCount() const override
    {
        return StrategyConfig_->MaxRunningOperations;
    }

    virtual int GetMaxOperationCount() const override
    {
        return StrategyConfig_->MaxOperationCount;
    }

private:
    const TFairShareStrategyConfigPtr StrategyConfig_;
};

////////////////////////////////////////////////////////////////////

class TFairShareStrategy
    : public ISchedulerStrategy
{
public:
    TFairShareStrategy(
        TFairShareStrategyConfigPtr config,
        ISchedulerStrategyHost* host)
        : Config(config)
        , Host(host)
    {
        Host->SubscribeOperationRegistered(BIND(&TFairShareStrategy::OnOperationRegistered, this));
        Host->SubscribeOperationUnregistered(BIND(&TFairShareStrategy::OnOperationUnregistered, this));

        Host->SubscribeJobFinished(BIND(&TFairShareStrategy::OnJobFinished, this));
        Host->SubscribeJobUpdated(BIND(&TFairShareStrategy::OnJobUpdated, this));
        Host->SubscribePoolsUpdated(BIND(&TFairShareStrategy::OnPoolsUpdated, this));

        Host->SubscribeOperationRuntimeParamsUpdated(
            BIND(&TFairShareStrategy::OnOperationRuntimeParamsUpdated, this));

        RootElement = New<TRootElement>(Host, config);
    }


    virtual void ScheduleJobs(const ISchedulingContextPtr& schedulingContext) override
    {
        auto guard = WaitFor(TAsyncLockReaderGuard::Acquire(&ScheduleJobsLock))
            .Value();

        auto now = schedulingContext->GetNow();
        int attributesIndex = AllocateAttributesIndex();
        TFairShareContext context(schedulingContext, attributesIndex);

        RootElement->BeginHeartbeat();

        // Run periodic update.
        if (Config->FairShareUpdatePeriod && (!LastUpdateTime || now > LastUpdateTime.Get() + *Config->FairShareUpdatePeriod)) {
            PROFILE_TIMING ("/fair_share_update_time") {
                // The root element gets the whole cluster.
                RootElement->Update();
            }
            LastUpdateTime = now;
        }

        // Update starvation flags for all operations.
        for (const auto& pair : OperationToElement) {
            pair.second->CheckForStarvation(now);
        }

        // Update starvation flags for all pools.
        if (Config->EnablePoolStarvation) {
            for (const auto& pair : Pools) {
                pair.second->CheckForStarvation(now);
            }
        }

        // Run periodic logging.
        if (Config->FairShareLogPeriod && (!LastLogTime || now > LastLogTime.Get() + *Config->FairShareLogPeriod)) {
            // Log pools information.
            Host->LogEventFluently(ELogEventType::FairShareInfo, now)
                .Do(BIND(&TFairShareStrategy::BuildPoolsInformation, this))
                .Item("operations").DoMapFor(OperationToElement, [=] (TFluentMap fluent, const TOperationMap::value_type& pair) {
                    const auto& operationId = pair.first;
                    BuildYsonMapFluently(fluent)
                        .Item(ToString(operationId))
                        .BeginMap()
                            .Do(BIND(&TFairShareStrategy::BuildOperationProgress, this, operationId))
                        .EndMap();
                });
            for (auto& pair : OperationToElement) {
                const auto& operationId = pair.first;
                LOG_DEBUG("FairShareInfo: %v (OperationId: %v)",
                    GetOperationLoggingProgress(operationId),
                    operationId);
            }
            LastLogTime = now;
        }

        // First-chance scheduling.
        LOG_DEBUG("Scheduling new jobs");
        RootElement->PrescheduleJob(context, false);
        while (schedulingContext->CanStartMoreJobs()) {
            if (!RootElement->ScheduleJob(context)) {
                break;
            }
        }

        // Compute discount to node usage.
        LOG_DEBUG("Looking for preemptable jobs");
        yhash_set<TCompositeSchedulerElementPtr> discountedPools;
        std::vector<TJobPtr> preemptableJobs;
        for (const auto& job : schedulingContext->RunningJobs()) {
            const auto& operationElement = FindOperationElement(job->GetOperationId());
            if (!operationElement || !operationElement->IsJobExisting(job->GetId())) {
                LOG_DEBUG("Dangling running job found (JobId: %v, OperationId: %v)",
                    job->GetId(),
                    job->GetOperationId());
                continue;
            }

            if (IsJobPreemptable(job) && !operationElement->HasStarvingParent()) {
                TCompositeSchedulerElement* pool = operationElement->GetPool();
                while (pool) {
                    discountedPools.insert(pool);
                    pool->DynamicAttributes(attributesIndex).ResourceUsageDiscount += job->ResourceUsage();
                    pool = pool->GetParent();
                }
                schedulingContext->ResourceUsageDiscount() += job->ResourceUsage();
                preemptableJobs.push_back(job);
                LOG_DEBUG("Job is preemptable (JobId: %v)",
                    job->GetId());
            }
        }

        auto resourceDiscount = schedulingContext->ResourceUsageDiscount();
        int startedBeforePreemption = schedulingContext->StartedJobs().size();

        // Second-chance scheduling.
        // NB: Schedule at most one job.
        LOG_DEBUG("Scheduling new jobs with preemption");
        RootElement->PrescheduleJob(context, true);
        while (schedulingContext->CanStartMoreJobs()) {
            if (!RootElement->ScheduleJob(context)) {
                break;
            }
            if (schedulingContext->StartedJobs().size() != startedBeforePreemption) {
                break;
            }
        }

        int startedAfterPreemption = schedulingContext->StartedJobs().size();
        int scheduledDuringPreemption = startedAfterPreemption - startedBeforePreemption;

        // Reset discounts.
        schedulingContext->ResourceUsageDiscount() = ZeroJobResources();
        for (const auto& pool : discountedPools) {
            pool->DynamicAttributes(attributesIndex).ResourceUsageDiscount = ZeroJobResources();
        }

        // Preempt jobs if needed.
        std::sort(
            preemptableJobs.begin(),
            preemptableJobs.end(),
            [] (const TJobPtr& lhs, const TJobPtr& rhs) {
                return lhs->GetStartTime() > rhs->GetStartTime();
            });

        auto poolLimitsViolated = [&] (const TJobPtr& job) -> bool {
            const auto& operationElement = FindOperationElement(job->GetOperationId());
            if (!operationElement) {
                return false;
            }

            TCompositeSchedulerElement* pool = operationElement->GetPool();
            while (pool) {
                if (!Dominates(pool->ResourceLimits(), pool->ResourceUsage())) {
                    return true;
                }
                pool = pool->GetParent();
            }
            return false;
        };

        auto anyPoolLimitsViolated = [&] () -> bool {
            for (const auto& job : schedulingContext->StartedJobs()) {
                if (poolLimitsViolated(job)) {
                    return true;
                }
            }
            return false;
        };

        bool nodeLimitsViolated = true;
        bool poolsLimitsViolated = true;

        for (const auto& job : preemptableJobs) {
            const auto& operationElement = FindOperationElement(job->GetOperationId());
            if (!operationElement || !operationElement->IsJobExisting(job->GetId())) {
                LOG_INFO("Dangling preemptable job found (JobId: %v, OperationId: %v)",
                    job->GetId(),
                    job->GetOperationId());
                continue;
            }

            // Update flags only if violation is not resolved yet to avoid costly computations.
            if (nodeLimitsViolated) {
                nodeLimitsViolated = !Dominates(schedulingContext->ResourceLimits(), schedulingContext->ResourceUsage());
            }
            if (!nodeLimitsViolated && poolsLimitsViolated) {
                poolsLimitsViolated = anyPoolLimitsViolated();
            }

            if (!nodeLimitsViolated && !poolsLimitsViolated) {
                break;
            }

            if (nodeLimitsViolated || (poolsLimitsViolated && poolLimitsViolated(job))) {
                PreemptJob(job, context);
            }
        }

        RootElement->EndHeartbeat();
        FreeAttributesIndex(attributesIndex);

        LOG_DEBUG("Heartbeat info (StartedJobs: %v, PreemptedJobs: %v, "
            "JobsScheduledDuringPreemption: %v, PreemptableJobs: %v, PreemptableResources: %v)",
            schedulingContext->StartedJobs().size(),
            schedulingContext->PreemptedJobs().size(),
            scheduledDuringPreemption,
            preemptableJobs.size(),
            FormatResources(resourceDiscount));
    }

    virtual void ResetState() override
    {
        LastPoolsNodeUpdate.Reset();
        LastUpdateTime.Reset();
        LastLogTime.Reset();
    }

    virtual TError CanAddOperation(TOperationPtr operation) override
    {
        auto spec = ParseSpec(operation, operation->GetSpec());
        auto poolName = spec->Pool ? *spec->Pool : operation->GetAuthenticatedUser();
        auto pool = FindPool(poolName);
        TCompositeSchedulerElement* poolElement;
        if (!pool) {
            auto defaultPool = FindPool(Config->DefaultParentPool);
            if (!defaultPool) {
                poolElement = RootElement.Get();
            } else {
                poolElement = defaultPool.Get();
            }
        } else {
            poolElement = pool.Get();
        }

        const auto& poolWithViolatedLimit = FindPoolWithViolatedOperationCountLimit(poolElement);
        if (poolWithViolatedLimit) {
            return TError(
                EErrorCode::TooManyOperations,
                "Limit for the number of concurrent operations %v for pool %v has been reached",
                poolWithViolatedLimit->GetMaxOperationCount(),
                poolWithViolatedLimit->GetId());
        }
        return TError();
    }

    virtual void BuildOperationAttributes(const TOperationId& operationId, IYsonConsumer* consumer) override
    {
        const auto& element = GetOperationElement(operationId);
        auto serializedParams = ConvertToAttributes(element->GetRuntimeParams());
        BuildYsonMapFluently(consumer)
            .Items(*serializedParams);
    }

    virtual void BuildOperationProgress(const TOperationId& operationId, IYsonConsumer* consumer) override
    {
        const auto& element = FindOperationElement(operationId);
        if (!element) {
            return;
        }

        auto* pool = element->GetPool();
        BuildYsonMapFluently(consumer)
            .Item("pool").Value(pool->GetId())
            .Item("start_time").Value(element->DynamicAttributes(GlobalAttributesIndex).MinSubtreeStartTime)
            .Item("preemptable_job_count").Value(element->PreemptableJobs().size())
            .Do(BIND(&TFairShareStrategy::BuildElementYson, MakeStrong(pool), element));
    }

    virtual void BuildBriefOperationProgress(const TOperationId& operationId, IYsonConsumer* consumer) override
    {
        const auto& element = FindOperationElement(operationId);
        if (!element) {
            return;
        }

        auto* pool = element->GetPool();
        const auto& attributes = element->Attributes();
        BuildYsonMapFluently(consumer)
            .Item("pool").Value(pool->GetId())
            .Item("fair_share_ratio").Value(attributes.FairShareRatio);
    }

    virtual Stroka GetOperationLoggingProgress(const TOperationId& operationId) override
    {
        const auto& element = GetOperationElement(operationId);
        const auto& attributes = element->Attributes();
        const auto& dynamicAttributes = element->DynamicAttributes(GlobalAttributesIndex);

        return Format(
            "Scheduling = {Status: %v, DominantResource: %v, Demand: %.4lf, "
            "Usage: %.4lf, FairShare: %.4lf, Satisfaction: %.4lg, AdjustedMinShare: %.4lf, "
            "MaxPossibleUsage: %.4lf,  BestAllocation: %.4lf, "
            "Starving: %v, Weight: %v, "
            "PreemptableRunningJobs: %v}",
            element->GetStatus(),
            attributes.DominantResource,
            attributes.DemandRatio,
            attributes.UsageRatio,
            attributes.FairShareRatio,
            dynamicAttributes.SatisfactionRatio,
            attributes.AdjustedMinShareRatio,
            attributes.MaxPossibleUsageRatio,
            attributes.BestAllocationRatio,
            element->GetStarving(),
            element->GetWeight(),
            element->PreemptableJobs().size());
    }

    void BuildPoolsInformation(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("pools").DoMapFor(Pools, [&] (TFluentMap fluent, const TPoolMap::value_type& pair) {
                const auto& id = pair.first;
                const auto& pool = pair.second;
                const auto& config = pool->GetConfig();
                fluent
                    .Item(id).BeginMap()
                        .Item("mode").Value(config->Mode)
                        .Item("running_operation_count").Value(RunningOperationCount[pool->GetId()])
                        .Item("max_running_operation_count").Value(pool->GetMaxRunningOperationCount())
                        .DoIf(config->Mode == ESchedulingMode::Fifo, [&] (TFluentMap fluent) {
                            fluent
                                .Item("fifo_sort_parameters").Value(config->FifoSortParameters);
                        })
                        .DoIf(pool->GetParent(), [&] (TFluentMap fluent) {
                            fluent
                                .Item("parent").Value(pool->GetParent()->GetId());
                        })
                        .Do(BIND(&TFairShareStrategy::BuildElementYson, RootElement, pool))
                    .EndMap();
            });
    }

    virtual void BuildOrchid(IYsonConsumer* consumer) override
    {
        BuildPoolsInformation(consumer);
    }

    virtual void BuildBriefSpec(const TOperationId& operationId, IYsonConsumer* consumer) override
    {
        const auto& element = GetOperationElement(operationId);
        BuildYsonMapFluently(consumer)
            .Item("pool").Value(element->GetPool()->GetId());
    }

private:
    const TFairShareStrategyConfigPtr Config;
    ISchedulerStrategyHost* const Host;

    INodePtr LastPoolsNodeUpdate;
    typedef yhash_map<Stroka, TPoolPtr> TPoolMap;
    TPoolMap Pools;

    typedef yhash_map<TOperationId, TOperationElementPtr> TOperationMap;
    TOperationMap OperationToElement;

    std::list<TOperationPtr> OperationQueue;
    yhash_map<Stroka, int> RunningOperationCount;
    yhash_map<Stroka, int> OperationCount;

    TRootElementPtr RootElement;
    TNullable<TInstant> LastUpdateTime;
    TNullable<TInstant> LastLogTime;

    TAsyncReaderWriterLock ScheduleJobsLock;

    std::vector<int> FreeAttributesIndices;
    int MaxUsedAttributesIndex = GlobalAttributesIndex;

    bool IsJobPreemptable(const TJobPtr& job)
    {
        const auto& element = GetOperationElement(job->GetOperationId());

        double usageRatio = element->Attributes().UsageRatio;
        if (usageRatio < Config->MinPreemptableRatio) {
            return false;
        }

        const auto& attributes = element->Attributes();
        if (usageRatio < attributes.FairShareRatio) {
            return false;
        }

        if (!element->IsJobPreemptable(job->GetId())) {
            return false;
        }

        return true;
    }

    void PreemptJob(const TJobPtr& job, TFairShareContext& context)
    {
        const auto& operationElement = GetOperationElement(job->GetOperationId());

        context.SchedulingContext->ResourceUsage() -= job->ResourceUsage();
        operationElement->IncreaseJobResourceUsage(job->GetId(), -job->ResourceUsage());
        job->ResourceUsage() = ZeroJobResources();

        context.SchedulingContext->PreemptJob(job);
    }


    TStrategyOperationSpecPtr ParseSpec(const TOperationPtr& operation, INodePtr specNode)
    {
        try {
            return ConvertTo<TStrategyOperationSpecPtr>(specNode);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error parsing spec of pooled operation %v, defaults will be used",
                operation->GetId());
            return New<TStrategyOperationSpec>();
        }
    }

    TOperationRuntimeParamsPtr BuildInitialRuntimeParams(TStrategyOperationSpecPtr spec)
    {
        auto params = New<TOperationRuntimeParams>();
        params->Weight = spec->Weight;
        return params;
    }

    bool CanAddOperationToPool(const TPoolPtr& pool)
    {
        TCompositeSchedulerElement* element = pool.Get();
        while (element) {
            if (RunningOperationCount[element->GetId()] >= element->GetMaxRunningOperationCount()) {
                return false;
            }
            element = element->GetParent();
        }
        return true;
    }

    void OnOperationRegistered(TOperationPtr operation)
    {
        auto spec = ParseSpec(operation, operation->GetSpec());
        auto params = BuildInitialRuntimeParams(spec);
        auto operationElement = New<TOperationElement>(
            Config,
            spec,
            params,
            Host,
            operation);
        YCHECK(OperationToElement.insert(std::make_pair(operation->GetId(), operationElement)).second);

        auto poolName = spec->Pool ? *spec->Pool : operation->GetAuthenticatedUser();
        auto pool = FindPool(poolName);
        if (!pool) {
            pool = New<TPool>(Host, poolName, Config);
            RegisterPool(pool);
        }
        if (!pool->GetParent()) {
            SetPoolDefaultParent(pool);
        }

        IncreaseOperationCount(pool.Get(), 1);

        pool->AddChild(operationElement, false);
        pool->IncreaseUsage(operationElement->ResourceUsage());
        operationElement->SetPool(pool.Get());

        if (CanAddOperationToPool(pool.Get())) {
            ActivateOperation(operation->GetId());
        } else {
            OperationQueue.push_back(operation);
        }
    }

    TCompositeSchedulerElementPtr FindPoolWithViolatedOperationCountLimit(TCompositeSchedulerElement* element)
    {
        while (element) {
            if (OperationCount[element->GetId()] >= element->GetMaxOperationCount()) {
                return element;
            }
            element = element->GetParent();
        }
        return nullptr;
    }

    void IncreaseOperationCount(TCompositeSchedulerElement* element, int delta)
    {
        while (element) {
            OperationCount[element->GetId()] += delta;
            element = element->GetParent();
        }
    }

    void IncreaseRunningOperationCount(TCompositeSchedulerElement* element, int delta)
    {
        while (element) {
            RunningOperationCount[element->GetId()] += delta;
            element = element->GetParent();
        }
    }

    void ActivateOperation(const TOperationId& operationId)
    {
        const auto& operationElement = GetOperationElement(operationId);
        auto* pool = operationElement->GetPool();
        pool->EnableChild(operationElement);
        IncreaseRunningOperationCount(pool, 1);

        Host->ActivateOperation(operationId);

        LOG_INFO("Operation added to pool (OperationId: %v, Pool: %v)",
            operationId,
            pool->GetId());
    }

    void OnOperationUnregistered(TOperationPtr operation)
    {
        auto operationElement = GetOperationElement(operation->GetId());
        auto* pool = operationElement->GetPool();

        YCHECK(OperationToElement.erase(operation->GetId()) == 1);
        operationElement->DynamicAttributes(GlobalAttributesIndex).Active = false;
        pool->RemoveChild(operationElement);
        pool->IncreaseUsage(-operationElement->ResourceUsage());
        IncreaseOperationCount(pool, -1);

        LOG_INFO("Operation removed from pool (OperationId: %v, Pool: %v)",
            operation->GetId(),
            pool->GetId());

        bool IsPending = false;
        {
            auto it = OperationQueue.begin();
            while (it != OperationQueue.end()) {
                if (*it == operationElement->GetOperation()) {
                    IsPending = true;
                    OperationQueue.erase(it);
                    break;
                }
                ++it;
            }
        }

        if (!IsPending) {
            IncreaseRunningOperationCount(pool, -1);

            // Try to run operations from queue.
            auto it = OperationQueue.begin();
            while (it != OperationQueue.end() && RunningOperationCount[RootPoolName] < Config->MaxRunningOperations) {
                const auto& operation = *it;
                auto* operationPool = GetOperationElement(operation->GetId())->GetPool();
                if (CanAddOperationToPool(operationPool)) {
                    ActivateOperation(operation->GetId());
                    auto toRemove = it++;
                    OperationQueue.erase(toRemove);
                } else {
                    ++it;
                }
            }
        }

        if (pool->IsEmpty() && pool->IsDefaultConfigured()) {
            UnregisterPool(pool);
        }
    }

    void OnOperationRuntimeParamsUpdated(
        TOperationPtr operation,
        INodePtr update)
    {
        const auto& element = FindOperationElement(operation->GetId());
        if (!element)
            return;

        NLogging::TLogger Logger(SchedulerLogger);
        Logger.AddTag("OperationId: %v", operation->GetId());

        try {
            if (ReconfigureYsonSerializable(element->GetRuntimeParams(), update)) {
                LOG_INFO("Operation runtime parameters updated");
            }
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error parsing operation runtime parameters");
        }
    }


    void OnJobFinished(const TJobPtr& job)
    {
        const auto& element = GetOperationElement(job->GetOperationId());
        element->OnJobFinished(job->GetId());
    }

    void OnJobUpdated(const TJobPtr& job, const TJobResources& resourcesDelta)
    {
        const auto& element = GetOperationElement(job->GetOperationId());
        element->IncreaseJobResourceUsage(job->GetId(), resourcesDelta);
    }

    void RegisterPool(const TPoolPtr& pool)
    {
        YCHECK(Pools.insert(std::make_pair(pool->GetId(), pool)).second);
        LOG_INFO("Pool registered (Pool: %v)", pool->GetId());
    }

    void RegisterPool(const TPoolPtr& pool, const TCompositeSchedulerElementPtr& parent)
    {
        YCHECK(Pools.insert(std::make_pair(pool->GetId(), pool)).second);
        pool->SetParent(parent.Get());
        parent->AddChild(pool);

        LOG_INFO("Pool registered (Pool: %v, Parent: %v)",
            pool->GetId(),
            parent->GetId());
    }

    void UnregisterPool(const TPoolPtr& pool)
    {
        YCHECK(Pools.erase(pool->GetId()) == 1);
        pool->DynamicAttributes(GlobalAttributesIndex).Active = false;
        auto parent = pool->GetParent();
        SetPoolParent(pool, nullptr);

        LOG_INFO("Pool unregistered (Pool: %v, Parent: %v)",
            pool->GetId(),
            parent->GetId());
    }

    void SetPoolParent(const TPoolPtr& pool, const TCompositeSchedulerElementPtr& parent)
    {
        if (pool->GetParent() == parent)
            return;

        auto* oldParent = pool->GetParent();
        if (oldParent) {
            oldParent->IncreaseUsage(-pool->ResourceUsage());
            IncreaseRunningOperationCount(oldParent, -RunningOperationCount[pool->GetId()]);
            oldParent->RemoveChild(pool);
        }

        pool->SetParent(parent.Get());
        if (parent) {
            parent->AddChild(pool);
            parent->IncreaseUsage(pool->ResourceUsage());
            IncreaseRunningOperationCount(parent.Get(), RunningOperationCount[pool->GetId()]);

            LOG_INFO("Parent pool set (Pool: %v, Parent: %v)",
                pool->GetId(),
                parent->GetId());
        }
    }

    void SetPoolDefaultParent(const TPoolPtr& pool)
    {
        auto defaultParentPool = FindPool(Config->DefaultParentPool);
        if (!defaultParentPool || defaultParentPool == pool) {
            // NB: root element is not a pool, so we should supress warning in this special case.
            if (Config->DefaultParentPool != RootPoolName) {
                LOG_WARNING("Default parent pool %Qv is not registered", Config->DefaultParentPool);
            }
            SetPoolParent(pool, RootElement);
        } else {
            SetPoolParent(pool, defaultParentPool);
        }
    }

    TPoolPtr FindPool(const Stroka& id)
    {
        auto it = Pools.find(id);
        return it == Pools.end() ? nullptr : it->second;
    }

    TPoolPtr GetPool(const Stroka& id)
    {
        auto pool = FindPool(id);
        YCHECK(pool);
        return pool;
    }


    TOperationElementPtr FindOperationElement(const TOperationId& operationId)
    {
        auto it = OperationToElement.find(operationId);
        return it == OperationToElement.end() ? nullptr : it->second;
    }

    TOperationElementPtr GetOperationElement(const TOperationId& operationId)
    {
        auto element = FindOperationElement(operationId);
        YCHECK(element);
        return element;
    }

    void OnPoolsUpdated(INodePtr poolsNode)
    {
        if (LastPoolsNodeUpdate && NYTree::AreNodesEqual(LastPoolsNodeUpdate, poolsNode)) {
            return;
        }
        LastPoolsNodeUpdate = poolsNode;

        auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(&ScheduleJobsLock)).Value();

        try {
            // Build the set of potential orphans.
            yhash_set<Stroka> orphanPoolIds;
            for (const auto& pair : Pools) {
                YCHECK(orphanPoolIds.insert(pair.first).second);
            }

            // Track ids appearing in various branches of the tree.
            yhash_map<Stroka, TYPath> poolIdToPath;

            // NB: std::function is needed by parseConfig to capture itself.
            std::function<void(INodePtr, TCompositeSchedulerElementPtr)> parseConfig =
                [&] (INodePtr configNode, TCompositeSchedulerElementPtr parent) {
                    auto configMap = configNode->AsMap();
                    for (const auto& pair : configMap->GetChildren()) {
                        const auto& childId = pair.first;
                        const auto& childNode = pair.second;
                        auto childPath = childNode->GetPath();
                        if (!poolIdToPath.insert(std::make_pair(childId, childPath)).second) {
                            LOG_ERROR("Pool %Qv is defined both at %v and %v; skipping second occurrence",
                                childId,
                                poolIdToPath[childId],
                                childPath);
                            continue;
                        }

                        // Parse config.
                        auto configNode = ConvertToNode(childNode->Attributes());
                        TPoolConfigPtr config;
                        try {
                            config = ConvertTo<TPoolConfigPtr>(configNode);
                        } catch (const std::exception& ex) {
                            LOG_ERROR(ex, "Error parsing configuration of pool %Qv; using defaults",
                                childPath);
                            config = New<TPoolConfig>();
                        }

                        auto pool = FindPool(childId);
                        if (pool) {
                            // Reconfigure existing pool.
                            pool->SetConfig(config);
                            YCHECK(orphanPoolIds.erase(childId) == 1);
                        } else {
                            // Create new pool.
                            pool = New<TPool>(Host, childId, Config);
                            pool->SetConfig(config);
                            RegisterPool(pool, parent);
                        }
                        SetPoolParent(pool, parent);

                        // Parse children.
                        parseConfig(childNode, pool.Get());
                    }
                };

            // Run recursive descent parsing.
            parseConfig(poolsNode, RootElement);

            // Unregister orphan pools.
            for (const auto& id : orphanPoolIds) {
                auto pool = GetPool(id);
                if (pool->IsEmpty()) {
                    UnregisterPool(pool);
                } else {
                    pool->SetDefaultConfig();
                    SetPoolDefaultParent(pool);
                }
            }

            RootElement->Update();

            LOG_INFO("Pools updated");
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error updating pools");
        }
    }

    static void BuildElementYson(
        TCompositeSchedulerElementPtr composite,
        const ISchedulerElementPtr& element,
        IYsonConsumer* consumer)
    {
        const auto& attributes = element->Attributes();
        const auto& dynamicAttributes = element->DynamicAttributes(GlobalAttributesIndex);

        BuildYsonMapFluently(consumer)
            .Item("scheduling_status").Value(element->GetStatus())
            .Item("starving").Value(element->GetStarving())
            .Item("resource_demand").Value(element->ResourceDemand())
            .Item("resource_usage").Value(element->ResourceUsage())
            .Item("resource_limits").Value(element->ResourceLimits())
            .Item("dominant_resource").Value(attributes.DominantResource)
            .Item("weight").Value(element->GetWeight())
            .Item("min_share_ratio").Value(element->GetMinShareRatio())
            .Item("max_share_ratio").Value(element->GetMaxShareRatio())
            .Item("adjusted_min_share_ratio").Value(attributes.AdjustedMinShareRatio)
            .Item("max_possible_usage_ratio").Value(attributes.MaxPossibleUsageRatio)
            .Item("usage_ratio").Value(attributes.UsageRatio)
            .Item("demand_ratio").Value(attributes.DemandRatio)
            .Item("fair_share_ratio").Value(attributes.FairShareRatio)
            .Item("satisfaction_ratio").Value(dynamicAttributes.SatisfactionRatio)
            .Item("best_allocation_ratio").Value(attributes.BestAllocationRatio);
    }

    int AllocateAttributesIndex()
    {
        if (FreeAttributesIndices.empty()) {
            FreeAttributesIndices.push_back(++MaxUsedAttributesIndex);
        }
        int index = FreeAttributesIndices.back();
        FreeAttributesIndices.pop_back();
        return index;
    }

    void FreeAttributesIndex(int index)
    {
        FreeAttributesIndices.push_back(index);
    }

};

std::unique_ptr<ISchedulerStrategy> CreateFairShareStrategy(
    TFairShareStrategyConfigPtr config,
    ISchedulerStrategyHost* host)
{
    return std::unique_ptr<ISchedulerStrategy>(new TFairShareStrategy(config, host));
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

