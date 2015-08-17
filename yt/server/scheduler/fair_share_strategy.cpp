#include "stdafx.h"
#include "config.h"
#include "fair_share_strategy.h"
#include "public.h"
#include "scheduler_strategy.h"
#include "master_connector.h"
#include "job_resources.h"

#include <core/concurrency/async_rw_lock.h>
#include <core/profiling/scoped_timer.h>

namespace NYT {
namespace NScheduler {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;

////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;
static const auto& Profiler = SchedulerProfiler;

static const double RatioComputationPrecision = std::numeric_limits<double>::epsilon();
static const double RatioComparisonPrecision = sqrt(RatioComputationPrecision);

////////////////////////////////////////////////////////////////////

struct ISchedulerElement;
typedef TIntrusivePtr<ISchedulerElement> ISchedulerElementPtr;
typedef TWeakPtr<ISchedulerElement> ISchedulerElementWeakPtr;

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
    ISchedulerElementWeakPtr BestLeafDescendant;
    TInstant MinSubtreeStartTime;
    TNodeResources ResourceUsageDiscount = ZeroNodeResources();
};

DEFINE_ENUM(ESchedulableStatus,
    (Normal)
    (BelowMinShare)
    (BelowFairShare)
);

////////////////////////////////////////////////////////////////////

struct ISchedulerElement
    : public TRefCounted
{
    virtual void Update() = 0;
    virtual void UpdateBottomUp() = 0;
    virtual void UpdateTopDown() = 0;

    virtual void BeginHeartbeat() = 0;
    virtual void UpdateDynamicAttributes(TDynamicAttributesMap& dynamicAttributesMap) = 0;
    virtual void PrescheduleJob(TFairShareContext& context, bool starvingOnly) = 0;
    virtual bool ScheduleJob(TFairShareContext& context) = 0;
    virtual void EndHeartbeat() = 0;

    virtual const TSchedulableAttributes& Attributes() const = 0;
    virtual TSchedulableAttributes& Attributes() = 0;
    virtual void UpdateAttributes() = 0;

    virtual const TDynamicAttributes& DynamicAttributes() const = 0;
    virtual TDynamicAttributes& DynamicAttributes() = 0;

    virtual int GetPendingJobCount() const = 0;

    virtual Stroka GetId() const = 0;

    virtual double GetWeight() const = 0;
    virtual double GetMinShareRatio() const = 0;
    virtual double GetMaxShareRatio() const = 0;

    virtual ESchedulableStatus GetStatus() const = 0;

    virtual bool GetStarving() const = 0;
    virtual void SetStarving(bool starving) = 0;
    virtual void CheckForStarvation(TInstant now) = 0;

    virtual const TNodeResources& ResourceDemand() const = 0;
    virtual const TNodeResources& ResourceUsage() const = 0;
    virtual const TNodeResources& ResourceLimits() const = 0;
    virtual const TNodeResources& MaxPossibleResourceUsage() const = 0;

    virtual void IncreaseUsage(const TNodeResources& delta) = 0;
};

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

template <>
struct THash<NYT::NScheduler::ISchedulerElementPtr> {
    inline size_t operator()(const NYT::NScheduler::ISchedulerElementPtr& a) const {
        return THash<Stroka>()(a->GetId());
    }
};

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

class TDynamicAttributesMap
{
public:
    void Initialize(const ISchedulerElement* element, TDynamicAttributes value = TDynamicAttributes())
    {
        Impl_[element->GetId()] = value;
    }

    void Initialize(const ISchedulerElementPtr& element, TDynamicAttributes value = TDynamicAttributes())
    {
        Initialize(element.Get(), value);
    }

    void Erase(const ISchedulerElement* element)
    {
        YCHECK(Impl_.erase(element->GetId()));
    }

    void Erase(const ISchedulerElementPtr& element)
    {
        Erase(element.Get());
    }

    TDynamicAttributes& At(const ISchedulerElement* element)
    {
        return Impl_.at(element->GetId());
    }

    TDynamicAttributes& At(const ISchedulerElementPtr& element)
    {
        return At(element.Get());
    }

    const TDynamicAttributes& At(const ISchedulerElement* element) const
    {
        return Impl_.at(element->GetId());
    }

    const TDynamicAttributes& At(const ISchedulerElementPtr& element) const
    {
        return At(element.Get());
    }

    bool GetActive(const ISchedulerElement* element) const
    {
        auto it = Impl_.find(element->GetId());
        if (it == Impl_.end()) {
            return false;
        }
        return it->second.Active;
    }

    bool GetActive(const ISchedulerElementPtr& element) const
    {
        return GetActive(element.Get());
    }

private:
    yhash_map<Stroka, TDynamicAttributes> Impl_;

};

struct TFairShareContext
{
    explicit TFairShareContext(ISchedulingContext* schedulingContext)
        : SchedulingContext(schedulingContext)
    { }

    ISchedulingContext* SchedulingContext;
    TDynamicAttributesMap DynamicAttributesMap;
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
        DynamicAttributesMap.At(this).Active = true;
        UpdateDynamicAttributes(DynamicAttributesMap);
    }

    // Updates attributes that are propagated from root down to leafs.
    // For example: child->FairShareRatio = fraction(parent->FairShareRatio).
    virtual void UpdateTopDown() override
    { }

    virtual void BeginHeartbeat() override
    { }

    virtual void UpdateDynamicAttributes(TDynamicAttributesMap& dynamicAttributesMap) override
    {
        YCHECK(dynamicAttributesMap.GetActive(this));
        dynamicAttributesMap.At(this).SatisfactionRatio = ComputeLocalSatisfactionRatio();
    }

    virtual void PrescheduleJob(TFairShareContext& context, bool starvingOnly) override
    {
        UpdateDynamicAttributes(context.DynamicAttributesMap);
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
        auto allocationLimits = GetAdjustedResourceLimits(
            demand,
            totalLimits,
            Host->GetExecNodeCount());
        auto maxPossibleResourceUsage = Min(totalLimits, MaxPossibleResourceUsage());

        if (usage == ZeroNodeResources()) {
            Attributes_.DominantResource = GetDominantResource(demand, totalLimits);
        } else {
            Attributes_.DominantResource = GetDominantResource(usage, totalLimits);
        }

        i64 dominantDemand = GetResource(demand, Attributes_.DominantResource);
        i64 dominantUsage = GetResource(usage, Attributes_.DominantResource);
        i64 dominantAllocationLimit = GetResource(allocationLimits, Attributes_.DominantResource);
        i64 dominantLimit = GetResource(totalLimits, Attributes_.DominantResource);

        Attributes_.DemandRatio =
            dominantLimit == 0 ? 1.0 : (double) dominantDemand / dominantLimit;

        Attributes_.UsageRatio =
            dominantLimit == 0 ? 1.0 : (double) dominantUsage / dominantLimit;

        Attributes_.BestAllocationRatio =
            dominantLimit == 0 ? 1.0 : (double) dominantAllocationLimit / dominantLimit;

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

    virtual const TDynamicAttributes& DynamicAttributes() const override
    {
        return DynamicAttributesMap.At(this);
    }

    virtual TDynamicAttributes& DynamicAttributes() override
    {
        return DynamicAttributesMap.At(this);
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

    void IncreaseUsageRatio(const TNodeResources& delta)
    {
        if (Attributes_.DominantLimit != 0) {
            i64 dominantDeltaUsage = GetResource(delta, Attributes_.DominantResource);
            Attributes_.UsageRatio += (double) dominantDeltaUsage / Attributes_.DominantLimit;
        } else {
            Attributes_.UsageRatio = 1.0;
        }
    }

    virtual void IncreaseUsage(const TNodeResources& delta) override
    { }

    DEFINE_BYREF_RW_PROPERTY(TSchedulableAttributes, Attributes);
    DEFINE_BYVAL_RW_PROPERTY(bool, Starving);
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, BelowFairShareSince);

protected:
    ISchedulerStrategyHost* Host;

    TDynamicAttributesMap& DynamicAttributesMap;

    TSchedulerElementBase(ISchedulerStrategyHost* host, TDynamicAttributesMap& dynamicAttributesMap)
        : Starving_(false)
        , Host(host)
        , DynamicAttributesMap(dynamicAttributesMap)
    { }

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
    TCompositeSchedulerElement(ISchedulerStrategyHost* host, TDynamicAttributesMap& dynamicAttributesMap)
        : TSchedulerElementBase(host, dynamicAttributesMap)
        , Parent_(nullptr)
        , ResourceDemand_(ZeroNodeResources())
        , ResourceUsage_(ZeroNodeResources())
        , ResourceLimits_(InfiniteNodeResources())
        , Mode(ESchedulingMode::Fifo)
    { }

    virtual void UpdateBottomUp() override
    {
        PendingJobCount = 0;
        ResourceDemand_ = ZeroNodeResources();
        auto maxPossibleChildrenResourceUsage_ = ZeroNodeResources();
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

    virtual void UpdateDynamicAttributes(TDynamicAttributesMap& dynamicAttributesMap) override
    {
        YCHECK(dynamicAttributesMap.GetActive(this));
        auto& attributes = dynamicAttributesMap.At(this);

        // Compute local satisfaction ratio.
        attributes.SatisfactionRatio = ComputeLocalSatisfactionRatio();
        // Start times bubble up from leaf nodes with operations.
        attributes.MinSubtreeStartTime = TInstant::Max();
        // Adjust satisfaction ratio using children.
        // Declare the element passive if all children are passive.
        attributes.Active = false;
        attributes.BestLeafDescendant.Reset();

        while (auto bestChild = GetBestActiveChild(dynamicAttributesMap)) {
            auto childBestLeafDescendant = dynamicAttributesMap.At(bestChild).BestLeafDescendant.Lock();
            if (!childBestLeafDescendant) {
                bestChild->UpdateDynamicAttributes(dynamicAttributesMap);
                if (!dynamicAttributesMap.GetActive(bestChild)) {
                    continue;
                }
                childBestLeafDescendant = dynamicAttributesMap.At(bestChild).BestLeafDescendant.Lock();
                YCHECK(childBestLeafDescendant);
            }

            // We need to evaluate both MinSubtreeStartTime and SatisfactionRatio
            // because parent can use different scheduling mode.
            attributes.MinSubtreeStartTime = std::min(
                attributes.MinSubtreeStartTime,
                dynamicAttributesMap.At(bestChild).MinSubtreeStartTime);

            attributes.SatisfactionRatio = std::min(
                attributes.SatisfactionRatio,
                dynamicAttributesMap.At(bestChild).SatisfactionRatio);

            attributes.BestLeafDescendant = dynamicAttributesMap.At(bestChild).BestLeafDescendant;
            attributes.Active = true;
            break;
        }
    }

    virtual void PrescheduleJob(TFairShareContext& context, bool starvingOnly) override
    {
        const auto& node = context.SchedulingContext->GetNode();
        context.DynamicAttributesMap.Initialize(this);
        auto& attributes = context.DynamicAttributesMap.At(this);

        attributes.Active = true;

        if (!node->CanSchedule(GetSchedulingTag())) {
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
        auto& attributes = context.DynamicAttributesMap.At(this);
        if (!attributes.Active) {
            return false;
        }

        auto bestLeafDescendant = attributes.BestLeafDescendant.Lock();
        if (!bestLeafDescendant || !DynamicAttributesMap.GetActive(bestLeafDescendant)) {
            // NB: This can only happen as a result of deletion of bestLeafDescendant node
            // from scheduling tree in another fiber (e.x. operation abort),
            // while this fiber was waiting for controller.
            UpdateDynamicAttributes(context.DynamicAttributesMap);
            if (!attributes.Active) {
                return false;
            }
            bestLeafDescendant = attributes.BestLeafDescendant.Lock();
            YCHECK(bestLeafDescendant);
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

    void AddChild(ISchedulerElementPtr child, bool enabled = true)
    {
        if (enabled) {
            YCHECK(Children.insert(child).second);
        } else {
            YCHECK(DisabledChildren.insert(child).second);
        }
    }

    void EnableChild(ISchedulerElementPtr child)
    {
        auto it = DisabledChildren.find(child);
        YCHECK(it != DisabledChildren.end());
        Children.insert(child);
        DisabledChildren.erase(it);
    }

    void RemoveChild(ISchedulerElementPtr child)
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

    std::vector<ISchedulerElementPtr> GetChildren() const
    {
        return std::vector<ISchedulerElementPtr>(Children.begin(), Children.end());
    }

    bool IsEmpty() const
    {
        return Children.empty() && DisabledChildren.empty();
    }

    DEFINE_BYVAL_RW_PROPERTY(TCompositeSchedulerElement*, Parent);

    DEFINE_BYREF_RW_PROPERTY(TNodeResources, ResourceDemand);
    DEFINE_BYREF_RW_PROPERTY(TNodeResources, ResourceUsage);
    DEFINE_BYREF_RO_PROPERTY(TNodeResources, ResourceLimits);
    DEFINE_BYREF_RO_PROPERTY(TNodeResources, MaxPossibleResourceUsage);

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
        auto bestChild = GetBestActiveChildFifo(DynamicAttributesMap);
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


    std::vector<ISchedulerElementPtr> GetActiveChildren(const TDynamicAttributesMap& dynamicAttributesMap) const
    {
        std::vector<ISchedulerElementPtr> result;
        result.reserve(Children.size());
        for (const auto& child : Children) {
            if (dynamicAttributesMap.GetActive(child)) {
                result.push_back(child);
            }
        }
        return result;
    }

    ISchedulerElementPtr GetBestActiveChild(const TDynamicAttributesMap& dynamicAttributesMap) const
    {
        switch (Mode) {
            case ESchedulingMode::Fifo:
                return GetBestActiveChildFifo(dynamicAttributesMap);
            case ESchedulingMode::FairShare:
                return GetBestActiveChildFairShare(dynamicAttributesMap);
            default:
                YUNREACHABLE();
        }
    }

    ISchedulerElementPtr GetBestActiveChildFifo(const TDynamicAttributesMap& dynamicAttributesMap) const
    {
        auto isBetter = [this, &dynamicAttributesMap] (const ISchedulerElementPtr& lhs, const ISchedulerElementPtr& rhs) -> bool {
            for (auto parameter : FifoSortParameters) {
                switch (parameter) {
                    case EFifoSortParameter::Weight:
                        if (lhs->GetWeight() != rhs->GetWeight()) {
                            return lhs->GetWeight() > rhs->GetWeight();
                        }
                        break;
                    case EFifoSortParameter::StartTime: {
                        const auto& lhsStartTime = dynamicAttributesMap.At(lhs).MinSubtreeStartTime;
                        const auto& rhsStartTime = dynamicAttributesMap.At(rhs).MinSubtreeStartTime;
                        if (lhsStartTime != rhsStartTime) {
                            return lhsStartTime < rhsStartTime;
                        }
                        break;
                    }
                    case EFifoSortParameter::PendingJobCount:
                        if (lhs->GetPendingJobCount() != rhs->GetPendingJobCount()) {
                            return lhs->GetPendingJobCount() < rhs->GetPendingJobCount();
                        }
                        break;
                    default:
                        YUNREACHABLE();
                }
            }
            return false;
        };

        ISchedulerElementPtr bestChild;
        for (const auto& child : GetActiveChildren(dynamicAttributesMap)) {
            if (bestChild && isBetter(bestChild, child))
                continue;

            bestChild = child;
        }
        return bestChild;
    }
    ISchedulerElementPtr GetBestActiveChildFairShare(const TDynamicAttributesMap& dynamicAttributesMap) const
    {
        ISchedulerElementPtr bestChild;
        double bestChildSatisfactionRatio;
        for (const auto& child : GetActiveChildren(dynamicAttributesMap)) {
            double childSatisfactionRatio = dynamicAttributesMap.At(child).SatisfactionRatio;
            if (!bestChild || childSatisfactionRatio < bestChildSatisfactionRatio)
            {
                bestChild = child;
                bestChildSatisfactionRatio = childSatisfactionRatio;
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
        TFairShareStrategyConfigPtr strategyConfig,
        TDynamicAttributesMap& dynamicAttributesMap)
        : TCompositeSchedulerElement(host, dynamicAttributesMap)
        , Id(id)
        , StrategyConfig_(strategyConfig)
    {
        DynamicAttributesMap.Initialize(this);
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

    virtual void IncreaseUsage(const TNodeResources& delta) override
    {
        TCompositeSchedulerElement* currentPool = this;
        while (currentPool) {
            currentPool->ResourceUsage() += delta;
            currentPool->IncreaseUsageRatio(delta);
            currentPool = currentPool->GetParent();
        }
    }

private:
    Stroka Id;

    TPoolConfigPtr Config_;
    bool DefaultConfigured;

    TFairShareStrategyConfigPtr StrategyConfig_;

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

    TNodeResources ComputeResourceLimits() const
    {
        auto poolLimits = Host->GetResourceLimits(GetSchedulingTag()) * Config_->MaxShareRatio;
        return Min(poolLimits, Config_->ResourceLimits->ToNodeResources());
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
        TOperationPtr operation,
        TDynamicAttributesMap& dynamicAttributesMap)
        : TSchedulerElementBase(host, dynamicAttributesMap)
        , Operation_(operation)
        , Spec_(spec)
        , RuntimeParams_(runtimeParams)
        , Pool_(nullptr)
        , ResourceUsage_(ZeroNodeResources())
        , NonpreemptableResourceUsage_(ZeroNodeResources())
        , Config(config)
        , OperationId_(Operation_->GetId())
    {
        DynamicAttributesMap.Initialize(this);
        DynamicAttributesMap.At(this).Active = true;
        DynamicAttributesMap.At(this).MinSubtreeStartTime = operation->GetStartTime();
        DynamicAttributesMap.At(this).BestLeafDescendant = MakeWeak(this);
    }


    virtual void PrescheduleJob(TFairShareContext& context, bool starvingOnly) override
    {
        context.DynamicAttributesMap.Initialize(this, DynamicAttributesMap.At(this));
        auto& attributes = context.DynamicAttributesMap.At(this);
        const auto& node = context.SchedulingContext->GetNode();

        attributes.Active = true;

        if (!node->CanSchedule(GetSchedulingTag())) {
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
        YCHECK(DynamicAttributesMap.GetActive(this));

        auto updateAncestorsAttributes = [&] () {
            TCompositeSchedulerElement* pool = Pool_;
            while (pool) {
                pool->UpdateDynamicAttributes(context.DynamicAttributesMap);
                pool = pool->GetParent();
            }
        };

        if (!Operation_->IsSchedulable()) {
            context.DynamicAttributesMap.At(this).Active = false;
            updateAncestorsAttributes();
            return false;
        }

        auto controller = Operation_->GetController();
        auto jobLimits = GetHierarchicalResourceLimits(context);

        auto asyncResult = BIND(&IOperationController::ScheduleJob, controller)
            .AsyncVia(controller->GetCancelableInvoker())
            .Run(context.SchedulingContext, jobLimits);

        NProfiling::TScopedTimer timer;
        auto jobIdOrError = WaitFor(asyncResult);
        auto scheduleJobDuration = timer.GetElapsed();

        // This can happen if operation is aborted from control thread during ScheduleJob call.
        // In this case current operation element is removed from scheduling tree and we don't
        // need to update parent attributes.
        if (!jobIdOrError.IsOK()) {
            YCHECK(!DynamicAttributesMap.GetActive(this));
            return false;
        }

        // This can happen if operation controller was canceled after invocation
        // of IOperationController::ScheduleJob. In this case cancel won't be applied
        // to the last action in invoker but its result should be ignored.
        if (!DynamicAttributesMap.GetActive(this)) {
            return false;
        }

        auto jobId = jobIdOrError.Value();

        if (!jobId) {
            context.DynamicAttributesMap.At(this).Active = false;
            updateAncestorsAttributes();
            Operation_->UpdateControllerTimeStatistics("/schedule_job/fail", scheduleJobDuration);
            return false;
        }

        const auto& job = context.SchedulingContext->FindStartedJob(jobId);
        context.SchedulingContext->GetNode()->ResourceUsage() += job->ResourceUsage();
        OnJobStarted(jobId, job->ResourceUsage());
        UpdateDynamicAttributes(context.DynamicAttributesMap);
        updateAncestorsAttributes();
        Operation_->UpdateControllerTimeStatistics("/schedule_job/success", scheduleJobDuration);

        return true;
    }

    virtual int GetPendingJobCount() const override
    {
        auto controller = Operation_->GetController();
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

    virtual const TNodeResources& ResourceDemand() const override
    {
        ResourceDemand_ = ZeroNodeResources();
        if (Operation_->IsSchedulable()) {
            auto controller = Operation_->GetController();
            ResourceDemand_ = ResourceUsage_ + controller->GetNeededResources();
        }
        return ResourceDemand_;
    }

    virtual const TNodeResources& ResourceLimits() const override
    {
        ResourceLimits_ = Host->GetResourceLimits(GetSchedulingTag()) * Spec_->MaxShareRatio;

        auto perTypeLimits = Spec_->ResourceLimits->ToNodeResources();
        ResourceLimits_ = Min(ResourceLimits_, perTypeLimits);

        return ResourceLimits_;
    }

    virtual const TNodeResources& MaxPossibleResourceUsage() const override
    {
        MaxPossibleResourceUsage_ = Min(ResourceLimits(), ResourceDemand());
        return MaxPossibleResourceUsage_;
    }

    ESchedulableStatus GetStatus() const
    {
        if (!Operation_->IsSchedulable()) {
            return ESchedulableStatus::Normal;
        }

        auto controller = Operation_->GetController();
        if (controller->GetPendingJobCount() == 0) {
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
            minSharePreemptionTimeout, fairSharePreemptionTimeout, now);
    }

    virtual void IncreaseUsage(const TNodeResources& delta) override
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

    void IncreaseJobResourceUsage(const TJobId& jobId, const TNodeResources& resourcesDelta)
    {
        auto& properties = JobPropertiesMap_.at(jobId);
        properties.ResourceUsage += resourcesDelta;
        if (!properties.IsPreemptable) {
            NonpreemptableResourceUsage_ += resourcesDelta;
        }
        IncreaseUsage(resourcesDelta);
        UpdatePreemptableJobsList();
    }

    void UpdatePreemptableJobsList()
    {
        auto limits = Host->GetTotalResourceLimits();

        auto getNonpreemptableUsageRatio = [&] (const TNodeResources& extraResources) -> double {
            i64 usage = GetResource(
                NonpreemptableResourceUsage_ + extraResources,
                Attributes_.DominantResource);
            i64 limit = GetResource(limits, Attributes_.DominantResource);
            return limit == 0 ? 1.0 : (double) usage / limit;
        };

        // Remove nonpreemptable jobs exceeding the fair share.
        while (!NonpreemptableJobs_.empty()) {
            if (getNonpreemptableUsageRatio(ZeroNodeResources()) <= Attributes_.FairShareRatio) {
                break;
            }

            auto jobId = NonpreemptableJobs_.back();
            auto& jobProperties = JobPropertiesMap_.at(jobId);
            YCHECK(!jobProperties.IsPreemptable);

            NonpreemptableJobs_.pop_back();
            NonpreemptableResourceUsage_ -= jobProperties.ResourceUsage;

            PreemptableJobs_.push_front(jobId);

            jobProperties.IsPreemptable = true;
            jobProperties.JobIdListIterator = PreemptableJobs_.begin();
        }

        // Add more nonpreemptable jobs until filling up the fair share.
        while (!PreemptableJobs_.empty()) {
            auto jobId = PreemptableJobs_.front();
            auto& jobProperties = JobPropertiesMap_.at(jobId);
            YCHECK(jobProperties.IsPreemptable);

            if (getNonpreemptableUsageRatio(jobProperties.ResourceUsage) > Attributes_.FairShareRatio) {
                break;
            }

            PreemptableJobs_.pop_front();

            NonpreemptableJobs_.push_back(jobId);
            NonpreemptableResourceUsage_ += jobProperties.ResourceUsage;

            jobProperties.IsPreemptable = false;
            jobProperties.JobIdListIterator = --NonpreemptableJobs_.end();
        }
    }

    bool IsJobExisting(const TJobId& jobId) const
    {
        return JobPropertiesMap_.find(jobId) != JobPropertiesMap_.end();
    }

    bool IsJobPreemptable(const TJobId& jobId) const
    {
        return JobPropertiesMap_.at(jobId).IsPreemptable;
    }

    void OnJobStarted(const TJobId& jobId, const TNodeResources& resourceUsage)
    {
        PreemptableJobs_.push_back(jobId);

        auto it = JobPropertiesMap_.insert(std::make_pair(
            jobId,
            TJobProperties(true, --PreemptableJobs_.end(), ZeroNodeResources())));
        YCHECK(it.second);

        IncreaseJobResourceUsage(jobId, resourceUsage);
    }

    void OnJobFinished(const TJobId& jobId)
    {
        auto it = JobPropertiesMap_.find(jobId);
        YCHECK(it != JobPropertiesMap_.end());

        auto& properties = it->second;

        if (properties.IsPreemptable) {
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
    DEFINE_BYREF_RW_PROPERTY(TNodeResources, ResourceUsage);

    typedef std::list<TJobId> TJobIdList;

    DEFINE_BYREF_RW_PROPERTY(TJobIdList, NonpreemptableJobs);
    DEFINE_BYREF_RW_PROPERTY(TJobIdList, PreemptableJobs);

    DEFINE_BYREF_RW_PROPERTY(TNodeResources, NonpreemptableResourceUsage);

private:
    mutable TNodeResources ResourceDemand_;
    mutable TNodeResources ResourceLimits_;
    mutable TNodeResources MaxPossibleResourceUsage_;

    TFairShareStrategyConfigPtr Config;

    TOperationId OperationId_;

    TNodeResources GetHierarchicalResourceLimits(const TFairShareContext& context) const
    {
        const auto& node = context.SchedulingContext->GetNode();

        // Bound limits with node free resources.
        auto limits =
            node->ResourceLimits()
            - node->ResourceUsage()
            + context.SchedulingContext->ResourceUsageDiscount();

        // Bound limits with pool free resources.
        TCompositeSchedulerElement* pool = Pool_;
        while (pool) {
            auto poolLimits =
                pool->ResourceLimits()
                - pool->ResourceUsage()
                + context.DynamicAttributesMap.At(pool).ResourceUsageDiscount;

            limits = Min(limits, poolLimits);
            pool = pool->GetParent();
        }

        // Bound limits with operation free resources.
        limits = Min(limits, ResourceLimits() - ResourceUsage());

        return limits;
    }

    // Fair share strategy stuff.
    struct TJobProperties
    {
        TJobProperties(bool isPreemptable, TJobIdList::iterator jobIdListIterator, const TNodeResources& resourceUsage)
            : IsPreemptable(isPreemptable)
            , JobIdListIterator(jobIdListIterator)
            , ResourceUsage(resourceUsage)
        { }

        //! Determines the per-operation list (either preemptable or non-preemptable) this
        //! job belongs to.
        bool IsPreemptable;

        //! Iterator in the per-operation list pointing to this particular job.
        TJobIdList::iterator JobIdListIterator;

        TNodeResources ResourceUsage;
    };

    typedef yhash_map<TJobId, TJobProperties> TJobPropertiesMap;
    TJobPropertiesMap JobPropertiesMap_;
};

////////////////////////////////////////////////////////////////////

class TRootElement
    : public TCompositeSchedulerElement
{
public:
    TRootElement(ISchedulerStrategyHost* host, TDynamicAttributesMap& dynamicAttributesMap)
        : TCompositeSchedulerElement(host, dynamicAttributesMap)
    {
        DynamicAttributesMap.Initialize(this);
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

        RootElement = New<TRootElement>(Host, DynamicAttributesMap);
    }


    virtual void ScheduleJobs(ISchedulingContext* schedulingContext) override
    {
        auto guard = WaitFor(TAsyncLockReaderGuard::Acquire(&ScheduleJobsLock)).Value();

        auto now = schedulingContext->GetNow();
        auto node = schedulingContext->GetNode();
        TFairShareContext context(schedulingContext);

        RootElement->BeginHeartbeat();

        // Run periodic update.
        if (Config->FairShareUpdatePeriod && (!LastUpdateTime || now > LastUpdateTime.Get() + *Config->FairShareUpdatePeriod)) {
            PROFILE_TIMING ("/fair_share_update_time") {
                // The root element get the whole cluster.
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
                    auto operationId = pair.first;
                    BuildYsonMapFluently(fluent)
                        .Item(ToString(operationId))
                        .BeginMap()
                            .Do(BIND(&TFairShareStrategy::BuildOperationProgress, this, operationId))
                        .EndMap();
                });
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
            auto operationElement = FindOperationElement(job->GetOperationId());
            if (!operationElement || !operationElement->IsJobExisting(job->GetId())) {
                LOG_INFO("Dangling running job found (JobId: %v, OperationId: %v)",
                    job->GetId(),
                    job->GetOperationId());
                continue;
            }

            if (IsJobPreemptable(job) && !operationElement->HasStarvingParent()) {
                TCompositeSchedulerElement* pool = operationElement->GetPool();
                while (pool) {
                    discountedPools.insert(pool);
                    context.DynamicAttributesMap.At(pool).ResourceUsageDiscount += job->ResourceUsage();
                    pool = pool->GetParent();
                }
                context.SchedulingContext->ResourceUsageDiscount() += job->ResourceUsage();
                preemptableJobs.push_back(job);
                LOG_DEBUG("Job is preemptable (JobId: %v)",
                    job->GetId());
            }
        }

        auto resourceDiscount = context.SchedulingContext->ResourceUsageDiscount();
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
        context.SchedulingContext->ResourceUsageDiscount() = ZeroNodeResources();
        for (const auto& pool : discountedPools) {
            context.DynamicAttributesMap.At(pool).ResourceUsageDiscount = ZeroNodeResources();
        }

        // Preempt jobs if needed.
        std::sort(
            preemptableJobs.begin(),
            preemptableJobs.end(),
            [] (const TJobPtr& lhs, const TJobPtr& rhs) {
                return lhs->GetStartTime() > rhs->GetStartTime();
            });

        auto poolLimitsViolated = [&] (TJobPtr job) -> bool {
            auto operationElement = FindOperationElement(job->GetOperationId());
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
            auto operationElement = FindOperationElement(job->GetOperationId());
            if (!operationElement || !operationElement->IsJobExisting(job->GetId())) {
                LOG_INFO("Dangling preemptable job found (JobId: %v, OperationId: %v)",
                    job->GetId(),
                    job->GetOperationId());
                continue;
            }

            // Update flags only if violation is not resolved yet to avoid costly computations.
            if (nodeLimitsViolated) {
                nodeLimitsViolated = !Dominates(node->ResourceLimits(), node->ResourceUsage());
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

        LOG_DEBUG("Heartbeat info (StartedJobs: %v, PreemptedJobs: %v, "
            "JobsScheduledDuringPreemption: %v, PreemptableJobs: %v, PreemptableResources: {%v})",
            schedulingContext->StartedJobs().size(),
            schedulingContext->PreemptedJobs().size(),
            scheduledDuringPreemption,
            preemptableJobs.size(),
            FormatResources(resourceDiscount));
    }

    virtual void BuildOperationAttributes(const TOperationId& operationId, IYsonConsumer* consumer) override
    {
        auto element = GetOperationElement(operationId);
        auto serializedParams = ConvertToAttributes(element->GetRuntimeParams());
        BuildYsonMapFluently(consumer)
            .Items(*serializedParams);
    }

    virtual void BuildOperationProgress(const TOperationId& operationId, IYsonConsumer* consumer) override
    {
        auto element = GetOperationElement(operationId);
        auto pool = element->GetPool();
        BuildYsonMapFluently(consumer)
            .Item("pool").Value(pool->GetId())
            .Item("start_time").Value(element->DynamicAttributes().MinSubtreeStartTime)
            .Item("preemptable_job_count").Value(element->PreemptableJobs().size())
            .Do(BIND(&TFairShareStrategy::BuildElementYson, pool, element));
    }

    virtual void BuildBriefOperationProgress(const TOperationId& operationId, IYsonConsumer* consumer) override
    {
        auto element = GetOperationElement(operationId);
        auto pool = element->GetPool();
        const auto& attributes = element->Attributes();
        BuildYsonMapFluently(consumer)
            .Item("pool").Value(pool->GetId())
            .Item("fair_share_ratio").Value(attributes.FairShareRatio);
    }

    virtual Stroka GetOperationLoggingProgress(const TOperationId& operationId) override
    {
        auto element = GetOperationElement(operationId);
        const auto& attributes = element->Attributes();
        const auto& dynamicAttributes = element->DynamicAttributes();

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
                auto pool = pair.second;
                auto config = pool->GetConfig();
                fluent
                    .Item(id).BeginMap()
                        .Item("mode").Value(config->Mode)
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
        auto element = GetOperationElement(operationId);
        BuildYsonMapFluently(consumer)
            .Item("pool").Value(element->GetPool()->GetId());
    }

private:
    TFairShareStrategyConfigPtr Config;
    ISchedulerStrategyHost* Host;

    typedef yhash_map<Stroka, TPoolPtr> TPoolMap;
    TPoolMap Pools;

    typedef yhash_map<TOperationId, TOperationElementPtr> TOperationMap;
    TOperationMap OperationToElement;

    std::list<TOperationPtr> OperationQueue;
    yhash_map<Stroka, int> RunningOperationCount;

    TRootElementPtr RootElement;
    TNullable<TInstant> LastUpdateTime;
    TNullable<TInstant> LastLogTime;

    TDynamicAttributesMap DynamicAttributesMap;

    TAsyncReaderWriterLock ScheduleJobsLock;

    bool IsJobPreemptable(TJobPtr job)
    {
        auto element = GetOperationElement(job->GetOperationId());
        auto spec = element->GetSpec();

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

    void PreemptJob(TJobPtr job, TFairShareContext& context)
    {
        auto operationElement = GetOperationElement(job->GetOperationId());

        context.SchedulingContext->GetNode()->ResourceUsage() -= job->ResourceUsage();
        operationElement->IncreaseJobResourceUsage(job->GetId(), -job->ResourceUsage());
        job->ResourceUsage() = ZeroNodeResources();

        context.SchedulingContext->PreemptJob(job);
    }


    TStrategyOperationSpecPtr ParseSpec(TOperationPtr operation, INodePtr specNode)
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

    bool CanAddOperationToPool(TPoolPtr pool)
    {
        TCompositeSchedulerElement* element = pool.Get();
        while (element) {
            if (element->IsRoot()) {
                break;
            }
            auto poolName = element->GetId();
            int MaxRunningOperations = pool->GetConfig()->MaxRunningOperations
                ? *(pool->GetConfig()->MaxRunningOperations)
                : Config->MaxRunningOperationsPerPool;
            if (RunningOperationCount[poolName] >= MaxRunningOperations) {
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
            operation,
            DynamicAttributesMap);
        YCHECK(OperationToElement.insert(std::make_pair(operation->GetId(), operationElement)).second);

        auto poolName = spec->Pool ? *spec->Pool : operation->GetAuthenticatedUser();
        auto pool = FindPool(poolName);
        if (!pool) {
            pool = New<TPool>(Host, poolName, Config, DynamicAttributesMap);
            RegisterPool(pool);
        }
        if (!pool->GetParent()) {
            auto defaultParentPool = FindPool(Config->DefaultParentPool);
            if (!defaultParentPool) {
                LOG_WARNING("Default parent pool %Qv is not registered", Config->DefaultParentPool);
                SetPoolParent(pool, RootElement);
            } else {
                SetPoolParent(pool, defaultParentPool);
            }
        }
        pool->AddChild(operationElement, false);
        pool->IncreaseUsage(operationElement->ResourceUsage());
        operationElement->SetPool(pool.Get());

        if (CanAddOperationToPool(pool.Get()) && RunningOperationCount[RootPoolName] < Config->MaxRunningOperations) {
            ActivateOperation(operation);
        } else {
            OperationQueue.push_back(operation);
            operation->SetQueued(true);
        }
    }

    // TODO(acid): This interface can also use operationId.
    void ActivateOperation(TOperationPtr operation)
    {
        auto operationElement = GetOperationElement(operation->GetId());
        auto pool = operationElement->GetPool();
        pool->EnableChild(operationElement);

        TCompositeSchedulerElement* element = pool;
        while (element) {
            RunningOperationCount[element->GetId()] += 1;
            element = element->GetParent();
        }

        LOG_INFO("Operation added to pool (OperationId: %v, Pool: %v)",
            operation->GetId(),
            pool->GetId());
    }

    void OnOperationUnregistered(TOperationPtr operation)
    {
        auto operationElement = GetOperationElement(operation->GetId());
        auto* pool = operationElement->GetPool();

        YCHECK(OperationToElement.erase(operation->GetId()) == 1);
        DynamicAttributesMap.Erase(operationElement);
        pool->RemoveChild(operationElement);
        pool->IncreaseUsage(-operationElement->ResourceUsage());

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
            TCompositeSchedulerElement* element = pool;
            while (element) {
                RunningOperationCount[element->GetId()] -= 1;
                element = element->GetParent();
            }

            // Try to run operations from queue.
            auto it = OperationQueue.begin();
            while (it != OperationQueue.end() && RunningOperationCount[RootPoolName] < Config->MaxRunningOperations) {
                auto operation = *it;
                if (CanAddOperationToPool(GetOperationElement(operation->GetId())->GetPool())) {
                    ActivateOperation(operation);
                    if (operation->GetState() == EOperationState::Pending) {
                        operation->SetState(EOperationState::Running);
                    }
                    operation->SetQueued(false);

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
        auto element = FindOperationElement(operation->GetId());
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


    void OnJobFinished(TJobPtr job)
    {
        auto element = GetOperationElement(job->GetOperationId());
        element->OnJobFinished(job->GetId());
    }

    void OnJobUpdated(TJobPtr job, const TNodeResources& resourcesDelta)
    {
        auto element = GetOperationElement(job->GetOperationId());
        element->IncreaseJobResourceUsage(job->GetId(), resourcesDelta);
    }

    void RegisterPool(TPoolPtr pool)
    {
        YCHECK(Pools.insert(std::make_pair(pool->GetId(), pool)).second);
        LOG_INFO("Pool registered (Pool: %v)", pool->GetId());
    }

    void RegisterPool(TPoolPtr pool, TCompositeSchedulerElementPtr parent)
    {
        YCHECK(Pools.insert(std::make_pair(pool->GetId(), pool)).second);
        pool->SetParent(parent.Get());
        parent->AddChild(pool);

        LOG_INFO("Pool registered (Pool: %v, Parent: %v)",
            pool->GetId(),
            parent->GetId());
    }

    void UnregisterPool(TPoolPtr pool)
    {
        YCHECK(Pools.erase(pool->GetId()) == 1);
        DynamicAttributesMap.Erase(pool);
        auto parent = pool->GetParent();
        SetPoolParent(pool, nullptr);

        LOG_INFO("Pool unregistered (Pool: %v, Parent: %v)",
            pool->GetId(),
            parent->GetId());
    }

    void SetPoolParent(TPoolPtr pool, TCompositeSchedulerElementPtr parent)
    {
        if (pool->GetParent() == parent)
            return;

        auto* oldParent = pool->GetParent();
        if (oldParent) {
            oldParent->IncreaseUsage(-pool->ResourceUsage());
            oldParent->RemoveChild(pool);
        }

        pool->SetParent(parent.Get());
        if (parent) {
            parent->AddChild(pool);
            parent->IncreaseUsage(pool->ResourceUsage());

            LOG_INFO("Set parent pool (Pool: %v, Parent: %v)",
                pool->GetId(),
                parent->GetId());
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
                            pool = New<TPool>(Host, childId, Config, DynamicAttributesMap);
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
                    SetPoolParent(pool, RootElement);
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
        ISchedulerElementPtr element,
        IYsonConsumer* consumer)
    {
        const auto& attributes = element->Attributes();
        const auto& dynamicAttributes = element->DynamicAttributes();

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

