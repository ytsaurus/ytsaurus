#include "stdafx.h"
#include "config.h"
#include "fair_share_strategy.h"
#include "scheduler_strategy.h"
#include "master_connector.h"
#include "job_resources.h"

#include <iostream>

namespace NYT {
namespace NScheduler {

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

class TOperationElement;
typedef TIntrusivePtr<TOperationElement> TOperationElementPtr;

class TCompositeSchedulerElement;
typedef TIntrusivePtr<TCompositeSchedulerElement> TCompositeSchedulerElementPtr;

class TPool;
typedef TIntrusivePtr<TPool> TPoolPtr;

class TRootElement;
typedef TIntrusivePtr<TRootElement> TRootElementPtr;

////////////////////////////////////////////////////////////////////

struct TSchedulableAttributes
{
    TSchedulableAttributes()
    { }

    EResourceType DominantResource = EResourceType::Cpu;
    double DemandRatio = 0.0;
    double UsageRatio = 0.0;
    double FairShareRatio = 0.0;
    double AdjustedMinShareRatio = 0.0;
    double MaxShareRatio = 1.0;
    double SatisfactionRatio = 0.0;
    double BestAllocationRatio = 1.0;
    i64 DominantLimit = 0;
    bool Active = true;
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
    virtual void UpdateDynamicAttributes() = 0;
    virtual void PrescheduleJob(TExecNodePtr node, bool starvingOnly) = 0;
    virtual bool ScheduleJob(ISchedulingContext* context) = 0;
    virtual void EndHeartbeat() = 0;

    virtual const TSchedulableAttributes& Attributes() const = 0;
    virtual TSchedulableAttributes& Attributes() = 0;
    virtual void UpdateAttributes() = 0;

    virtual TInstant GetStartTime() const = 0;
    virtual int GetPendingJobCount() const = 0;

    virtual Stroka GetId() const = 0;

    virtual double GetWeight() const = 0;
    virtual double GetMinShareRatio() const = 0;
    virtual double GetMaxShareRatio() const = 0;

    virtual ISchedulerElement* GetBestLeafDescendant() = 0;
    virtual ESchedulableStatus GetStatus() const = 0;

    virtual bool GetStarving() const = 0;
    virtual void SetStarving(bool starving) = 0;
    virtual void CheckForStarvation(TInstant now) = 0;

    virtual const TNodeResources& ResourceDemand() const = 0;
    virtual const TNodeResources& ResourceUsage() const = 0;
    virtual const TNodeResources& ResourceUsageDiscount() const = 0;
    virtual const TNodeResources& ResourceLimits() const = 0;

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
    }

    // Updates attributes that are propagated from root down to leafs.
    // For example: child->FairShareRatio = fraction(parent->FairShareRatio).
    virtual void UpdateTopDown() override
    { }

    virtual void BeginHeartbeat() override
    {
        Attributes_.Active = true;
    }

    virtual void UpdateDynamicAttributes() override
    {
        Attributes_.SatisfactionRatio = ComputeLocalSatisfactionRatio();
    }

    virtual void PrescheduleJob(TExecNodePtr /*node*/, bool /*starvingOnly*/) override
    {
        UpdateDynamicAttributes();
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
        auto limits = Min(totalLimits, ResourceLimits());

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

        Attributes_.MaxShareRatio = GetMaxShareRatio();
        if (Attributes_.UsageRatio > RatioComputationPrecision)
        {
            Attributes_.MaxShareRatio = std::min(
                GetMinResourceRatio(limits, usage) * Attributes_.UsageRatio,
                Attributes_.MaxShareRatio);
        }
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

    explicit TSchedulerElementBase(ISchedulerStrategyHost* host)
        : Starving_(false)
        , Host(host)
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
    explicit TCompositeSchedulerElement(ISchedulerStrategyHost* host)
        : TSchedulerElementBase(host)
        , Parent_(nullptr)
        , ResourceDemand_(ZeroNodeResources())
        , ResourceUsage_(ZeroNodeResources())
        , ResourceUsageDiscount_(ZeroNodeResources())
        , ResourceLimits_(InfiniteNodeResources())
        , Mode(ESchedulingMode::Fifo)
    { }

    virtual void UpdateBottomUp() override
    {
        PendingJobCount = 0;
        ResourceDemand_ = ZeroNodeResources();
        Attributes_.BestAllocationRatio = 0.0;
        for (const auto& child : Children) {
            child->UpdateBottomUp();

            ResourceDemand_ += child->ResourceDemand();
            Attributes_.BestAllocationRatio = std::max(
                Attributes_.BestAllocationRatio,
                child->Attributes().BestAllocationRatio);

            PendingJobCount += child->GetPendingJobCount();
        }
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

    virtual void UpdateDynamicAttributes() override
    {
        // Compute local satisfaction ratio.
        Attributes_.SatisfactionRatio = ComputeLocalSatisfactionRatio();
        // Start times bubble up from leaf nodes with operations.
        MinSubtreeStartTime = TInstant::Max();
        // Adjust satisfaction ratio using children.
        // Declare the element passive if all children are passive.
        Attributes_.Active = false;
        BestLeafDescendant_ = nullptr;

        auto bestChild = GetBestChild();
        if (bestChild) {
            // We need to evaluate both MinSubtreeStartTime and SatisfactionRatio
            // because parent can use different scheduling mode.
            MinSubtreeStartTime = std::min(MinSubtreeStartTime, bestChild->GetStartTime());

            Attributes_.SatisfactionRatio = std::min(
                Attributes_.SatisfactionRatio,
                bestChild->Attributes().SatisfactionRatio);

            BestLeafDescendant_ = bestChild->GetBestLeafDescendant();
            Attributes_.Active = true;
        }
    }

    virtual void PrescheduleJob(TExecNodePtr node, bool starvingOnly) override
    {
        if (!Attributes_.Active)
            return;

        if (!node->CanSchedule(GetSchedulingTag())) {
            Attributes_.Active = false;
            return;
        }

        for (const auto& child : GetActiveChildren()) {
            // If pool is starving, any child will do.
            if (Starving_) {
                child->PrescheduleJob(node, false);
            } else {
                child->PrescheduleJob(node, starvingOnly);
            }
        }
        UpdateDynamicAttributes();
    }

    virtual bool ScheduleJob(ISchedulingContext* context) override
    {
        if (!BestLeafDescendant_) {
            return false;
        }

        // NB: Ignore the child's result.
        BestLeafDescendant_->ScheduleJob(context);

        return true;
    }

    virtual void EndHeartbeat() override
    {
        TSchedulerElementBase::EndHeartbeat();
        for (const auto& child : Children) {
            child->EndHeartbeat();
        }
    }

    virtual ISchedulerElement* GetBestLeafDescendant() override
    {
        return BestLeafDescendant_;
    }

    virtual TInstant GetStartTime() const override
    {
        // For pools StartTime is equal to minimal start time among active children.
        return MinSubtreeStartTime;
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

    virtual int GetMaxRunningOperationCount() const = 0;

    DEFINE_BYVAL_RW_PROPERTY(TCompositeSchedulerElement*, Parent);

    DEFINE_BYREF_RW_PROPERTY(TNodeResources, ResourceDemand);
    DEFINE_BYREF_RW_PROPERTY(TNodeResources, ResourceUsage);
    DEFINE_BYREF_RW_PROPERTY(TNodeResources, ResourceUsageDiscount);
    DEFINE_BYREF_RO_PROPERTY(TNodeResources, ResourceLimits);

protected:
    ESchedulingMode Mode;
    std::vector<EFifoSortParameter> FifoSortParameters;

    yhash_set<ISchedulerElementPtr> Children;
    yhash_set<ISchedulerElementPtr> DisabledChildren;

    ISchedulerElement* BestLeafDescendant_ = nullptr;

    TInstant MinSubtreeStartTime;
    int PendingJobCount;

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
        auto bestChild = GetBestChildFifo(false);
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
            // Never give more than demanded.
            result = std::min(result, childAttributes.DemandRatio);
            // Never give more than max share allows.
            result = std::min(result, childAttributes.MaxShareRatio);
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
                // Never give more than demanded.
                result = std::min(result, childAttributes.DemandRatio);
                // Never give more than max share allows.
                result = std::min(result, childAttributes.MaxShareRatio);
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


    std::vector<ISchedulerElementPtr> GetActiveChildren() const
    {
        std::vector<ISchedulerElementPtr> result;
        result.reserve(Children.size());
        for (const auto& child : Children) {
            if (child->Attributes().Active) {
                result.push_back(child);
            }
        }
        return result;
    }

    ISchedulerElementPtr GetBestChild() const
    {
        switch (Mode) {
            case ESchedulingMode::Fifo:
                return GetBestChildFifo(true);
            case ESchedulingMode::FairShare:
                return GetBestChildFairShare();
            default:
                YUNREACHABLE();
        }
    }

    ISchedulerElementPtr GetBestChildFifo(bool needsActive) const
    {
        auto isBetter = [this] (const ISchedulerElementPtr& lhs, const ISchedulerElementPtr& rhs) -> bool {
            for (auto parameter : FifoSortParameters) {
                switch (parameter) {
                    case EFifoSortParameter::Weight:
                        if (lhs->GetWeight() != rhs->GetWeight()) {
                            return lhs->GetWeight() > rhs->GetWeight();
                        }
                        break;
                    case EFifoSortParameter::StartTime:
                        if (lhs->GetStartTime() != rhs->GetStartTime()) {
                            return lhs->GetStartTime() < rhs->GetStartTime();
                        }
                        break;
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
        for (const auto& child : Children) {
            if (needsActive && !child->Attributes().Active)
                continue;

            if (bestChild && isBetter(bestChild, child))
                continue;

            bestChild = child;
        }
        return bestChild;
    }

    ISchedulerElementPtr GetBestChildFairShare() const
    {
        ISchedulerElementPtr bestChild;
        for (const auto& child : GetActiveChildren()) {
            if (!bestChild ||
                child->Attributes().SatisfactionRatio < bestChild->Attributes().SatisfactionRatio)
            {
                bestChild = child;
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

    virtual void IncreaseUsage(const TNodeResources& delta) override
    {
        TCompositeSchedulerElement* currentPool = this;
        while (currentPool) {
            currentPool->ResourceUsage() += delta;
            currentPool->IncreaseUsageRatio(delta);
            currentPool->UpdateDynamicAttributes();
            currentPool = currentPool->GetParent();
        }
    }

    virtual int GetMaxRunningOperationCount() const override
    {
        return Config_->MaxRunningOperations
            ? *(Config_->MaxRunningOperations)
            : StrategyConfig_->MaxRunningOperationsPerPool;
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
            FifoSortParameters = Config_->FifoSortParameters;
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
        poolLimits = Min(poolLimits, Config_->ResourceLimits->ToNodeResources());

        auto totalChildrenLimits = ZeroNodeResources();
        for (const auto& child : Children) {
            totalChildrenLimits += child->ResourceLimits();
        }

        return Min(poolLimits, totalChildrenLimits);
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
        , ResourceUsage_(ZeroNodeResources())
        , ResourceUsageDiscount_(ZeroNodeResources())
        , NonpreemptableResourceUsage_(ZeroNodeResources())
        , Config(config)
    { }

    virtual void PrescheduleJob(TExecNodePtr node, bool starvingOnly) override
    {
        TSchedulerElementBase::PrescheduleJob(node, starvingOnly);

        if (!node->CanSchedule(GetSchedulingTag())) {
            Attributes_.Active = false;
        }

        if (starvingOnly && !Starving_) {
            Attributes_.Active = false;
        }

        if (Operation_->GetState() != EOperationState::Running) {
            Attributes_.Active = false;
        }
    }

    virtual bool ScheduleJob(ISchedulingContext* context) override
    {
        auto node = context->GetNode();
        auto controller = Operation_->GetController();

        // Compute job limits from node limits and pool limits.
        auto jobLimits = node->ResourceLimits() - node->ResourceUsage() + node->ResourceUsageDiscount();
        TCompositeSchedulerElement* pool = Pool_;
        while (pool) {
            auto poolLimits = pool->ResourceLimits() - pool->ResourceUsage() + pool->ResourceUsageDiscount();
            jobLimits = Min(jobLimits, poolLimits);
            pool = pool->GetParent();
        }
        auto operationLimits = ResourceLimits() - ResourceUsage();
        jobLimits = Min(jobLimits, operationLimits);

        auto job = controller->ScheduleJob(context, jobLimits);
        if (job) {
            return true;
        } else {
            Attributes_.Active = false;
            TCompositeSchedulerElement* pool = Pool_;
            while (pool) {
                pool->UpdateDynamicAttributes();
                pool = pool->GetParent();
            }
            return false;
        }
    }

    virtual TInstant GetStartTime() const override
    {
        return Operation_->GetStartTime();
    }

    virtual int GetPendingJobCount() const override
    {
        auto controller = Operation_->GetController();
        return controller->GetPendingJobCount();
    }

    virtual Stroka GetId() const override
    {
        return ToString(Operation_->GetId());
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

    virtual ISchedulerElement* GetBestLeafDescendant() override
    {
        return this;
    }

    virtual const TNodeResources& ResourceDemand() const override
    {
        ResourceDemand_ = ZeroNodeResources();
        if (!Operation_->GetSuspended()) {
            auto controller = Operation_->GetController();
            ResourceDemand_ = ResourceUsage_ + controller->GetNeededResources();
        }
        return ResourceDemand_;
    }

    virtual const TNodeResources& ResourceLimits() const override
    {
        ResourceLimits_ = Host->GetResourceLimits(GetSchedulingTag());

        auto perTypeLimits = Spec_->ResourceLimits->ToNodeResources();
        ResourceLimits_ = Min(ResourceLimits_, perTypeLimits);

        return ResourceLimits_;
    }

    ESchedulableStatus GetStatus() const
    {
        if (Operation_->GetState() != EOperationState::Running) {
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

        int jobCount = Operation_->GetController()->GetTotalJobCount();
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
        UpdateDynamicAttributes();
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

    DEFINE_BYVAL_RO_PROPERTY(TOperationPtr, Operation);
    DEFINE_BYVAL_RO_PROPERTY(TStrategyOperationSpecPtr, Spec);
    DEFINE_BYVAL_RO_PROPERTY(TOperationRuntimeParamsPtr, RuntimeParams);
    DEFINE_BYVAL_RW_PROPERTY(TPool*, Pool);
    DEFINE_BYREF_RW_PROPERTY(TNodeResources, ResourceUsage);
    DEFINE_BYREF_RW_PROPERTY(TNodeResources, ResourceUsageDiscount);

    DEFINE_BYREF_RW_PROPERTY(TNodeResources, NonpreemptableResourceUsage);
    DEFINE_BYREF_RW_PROPERTY(TJobList, NonpreemptableJobs);
    DEFINE_BYREF_RW_PROPERTY(TJobList, PreemptableJobs);

private:
    mutable TNodeResources ResourceDemand_;
    mutable TNodeResources ResourceLimits_;

    TFairShareStrategyConfigPtr Config;
};

////////////////////////////////////////////////////////////////////

class TRootElement
    : public TCompositeSchedulerElement
{
public:
    TRootElement(ISchedulerStrategyHost* host, TFairShareStrategyConfigPtr strategyConfig)
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

private:
    TFairShareStrategyConfigPtr StrategyConfig_;
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

        Host->SubscribeJobStarted(BIND(&TFairShareStrategy::OnJobStarted, this));
        Host->SubscribeJobFinished(BIND(&TFairShareStrategy::OnJobFinished, this));
        Host->SubscribeJobUpdated(BIND(&TFairShareStrategy::OnJobUpdated, this));
        Host->SubscribePoolsUpdated(BIND(&TFairShareStrategy::OnPoolsUpdated, this));

        Host->SubscribeOperationRuntimeParamsUpdated(
            BIND(&TFairShareStrategy::OnOperationRuntimeParamsUpdated, this));

        RootElement = New<TRootElement>(Host, config);
    }


    virtual void ScheduleJobs(ISchedulingContext* context) override
    {
        auto now = TInstant::Now();
        auto node = context->GetNode();

        // Run periodic update.
        if (!LastUpdateTime || now > LastUpdateTime.Get() + Config->FairShareUpdatePeriod) {
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

        RootElement->BeginHeartbeat();

        // Run periodic logging.
        if (!LastLogTime || now > LastLogTime.Get() + Config->FairShareLogPeriod) {
            // Update satisfaction attributes.
            RootElement->PrescheduleJob(context->GetNode(), false);
            // Log pools information.
            Host->LogEventFluently(ELogEventType::FairShareInfo)
                .Do(BIND(&TFairShareStrategy::BuildPoolsInformation, this))
                .Item("operations").DoMapFor(OperationToElement, [=] (TFluentMap fluent, const TOperationMap::value_type& pair) {
                    auto operation = pair.first;
                    BuildYsonMapFluently(fluent)
                        .Item(ToString(operation->GetId()))
                        .BeginMap()
                            .Do(BIND(&TFairShareStrategy::BuildOperationProgress, this, operation))
                        .EndMap();
                });
            for (auto& pair : OperationToElement) {
                auto operation = pair.first;
                LOG_DEBUG("FairShareInfo: %v (OperationId: %v)",
                    GetOperationLoggingProgress(operation),
                    operation->GetId());
            }
            LastLogTime = now;
        }

        // First-chance scheduling.
        LOG_DEBUG("Scheduling new jobs");
        RootElement->PrescheduleJob(context->GetNode(), false);
        while (context->CanStartMoreJobs()) {
            if (!RootElement->ScheduleJob(context)) {
                break;
            }
        }

        // Compute discount to node usage.
        LOG_DEBUG("Looking for preemptable jobs");
        yhash_set<TCompositeSchedulerElementPtr> discountedPools;
        std::vector<TJobPtr> preemptableJobs;
        for (const auto& job : context->RunningJobs()) {
            auto operation = job->GetOperation();
            auto operationElement = GetOperationElement(operation);
            if (IsJobPreemptable(job) && !operationElement->HasStarvingParent()) {
                TCompositeSchedulerElement* pool = operationElement->GetPool();
                while (pool) {
                    discountedPools.insert(pool);
                    pool->ResourceUsageDiscount() += job->ResourceUsage();
                    pool = pool->GetParent();
                }
                node->ResourceUsageDiscount() += job->ResourceUsage();
                preemptableJobs.push_back(job);
                LOG_DEBUG("Job is preemptable (JobId: %v)",
                    job->GetId());
            }
        }

        RootElement->BeginHeartbeat();

        auto resourceDiscount = node->ResourceUsageDiscount();
        int startedBeforePreemption = context->StartedJobs().size();

        // Second-chance scheduling.
        // NB: Schedule at most one job.
        LOG_DEBUG("Scheduling new jobs with preemption");
        RootElement->PrescheduleJob(context->GetNode(), true);
        while (context->CanStartMoreJobs()) {
            if (!RootElement->ScheduleJob(context)) {
                break;
            }
            if (context->StartedJobs().size() != startedBeforePreemption) {
                break;
            }
        }

        int startedAfterPreemption = context->StartedJobs().size();
        int scheduledDuringPreemption = startedAfterPreemption - startedBeforePreemption;

        // Reset discounts.
        node->ResourceUsageDiscount() = ZeroNodeResources();
        for (const auto& pool : discountedPools) {
            pool->ResourceUsageDiscount() = ZeroNodeResources();
        }

        // Preempt jobs if needed.
        std::sort(
            preemptableJobs.begin(),
            preemptableJobs.end(),
            [] (const TJobPtr& lhs, const TJobPtr& rhs) {
                return lhs->GetStartTime() > rhs->GetStartTime();
            });

        auto poolLimitsViolated = [&] (TJobPtr job) -> bool {
            auto operation = job->GetOperation();
            auto operationElement = GetOperationElement(operation);
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
            for (const auto& job : context->StartedJobs()) {
                if (poolLimitsViolated(job)) {
                    return true;
                }
            }
            return false;
        };

        bool nodeLimitsViolated = true;
        bool poolsLimitsViolated = true;

        for (const auto& job : preemptableJobs) {
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
                context->PreemptJob(job);
            }
        }

        RootElement->EndHeartbeat();

        LOG_DEBUG("Heartbeat info (StartedJobs: %v, PreemptedJobs: %v, "
            "JobsScheduledDuringPreemption: %v, PreemptableJobs: %v, PreemptableResources: {%v})",
            context->StartedJobs().size(),
            context->PreemptedJobs().size(),
            scheduledDuringPreemption,
            preemptableJobs.size(),
            FormatResources(resourceDiscount));
    }

    virtual void BuildOperationAttributes(TOperationPtr operation, IYsonConsumer* consumer) override
    {
        auto element = GetOperationElement(operation);
        auto serializedParams = ConvertToAttributes(element->GetRuntimeParams());
        BuildYsonMapFluently(consumer)
            .Items(*serializedParams);
    }

    virtual void BuildOperationProgress(TOperationPtr operation, IYsonConsumer* consumer) override
    {
        auto element = GetOperationElement(operation);
        auto pool = element->GetPool();
        BuildYsonMapFluently(consumer)
            .Item("pool").Value(pool->GetId())
            .Item("start_time").Value(element->GetStartTime())
            .Item("preemptable_job_count").Value(element->PreemptableJobs().size())
            .Do(BIND(&TFairShareStrategy::BuildElementYson, pool, element));
    }

    virtual void BuildBriefOperationProgress(TOperationPtr operation, IYsonConsumer* consumer) override
    {
        auto element = GetOperationElement(operation);
        auto pool = element->GetPool();
        const auto& attributes = element->Attributes();
        BuildYsonMapFluently(consumer)
            .Item("pool").Value(pool->GetId())
            .Item("fair_share_ratio").Value(attributes.FairShareRatio);
    }

    virtual Stroka GetOperationLoggingProgress(TOperationPtr operation) override
    {
        auto element = GetOperationElement(operation);
        const auto& attributes = element->Attributes();
        return Format(
            "Scheduling = {Status: %v, DominantResource: %v, Demand: %.4lf, "
            "Usage: %.4lf, FairShare: %.4lf, Satisfaction: %.4lg, AdjustedMinShare: %.4lf, "
            "MaxShare: %.4lf,  BestAllocation: %.4lf, "
            "Starving: %v, Weight: %v, "
            "PreemptableRunningJobs: %v}",
            element->GetStatus(),
            attributes.DominantResource,
            attributes.DemandRatio,
            attributes.UsageRatio,
            attributes.FairShareRatio,
            attributes.SatisfactionRatio,
            attributes.AdjustedMinShareRatio,
            attributes.MaxShareRatio,
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

    virtual void BuildBriefSpec(TOperationPtr operation, IYsonConsumer* consumer) override
    {
        auto element = GetOperationElement(operation);
        BuildYsonMapFluently(consumer)
            .Item("pool").Value(element->GetPool()->GetId());
    }

private:
    TFairShareStrategyConfigPtr Config;
    ISchedulerStrategyHost* Host;

    typedef yhash_map<Stroka, TPoolPtr> TPoolMap;
    TPoolMap Pools;

    typedef yhash_map<TOperationPtr, TOperationElementPtr> TOperationMap;
    TOperationMap OperationToElement;

    std::list<TOperationPtr> OperationQueue;
    yhash_map<Stroka, int> RunningOperationCount;

    typedef std::list<TJobPtr> TJobList;
    TJobList JobList;
    yhash_map<TJobPtr, TJobList::iterator> JobToIterator;

    TRootElementPtr RootElement;
    TNullable<TInstant> LastUpdateTime;
    TNullable<TInstant> LastLogTime;

    bool IsJobPreemptable(TJobPtr job)
    {
        auto operation = job->GetOperation();
        if (operation->GetState() != EOperationState::Running) {
            return false;
        }

        auto element = GetOperationElement(operation);
        auto spec = element->GetSpec();

        double usageRatio = element->Attributes().UsageRatio;
        if (usageRatio < Config->MinPreemptableRatio) {
            return false;
        }

        const auto& attributes = element->Attributes();
        if (usageRatio < attributes.FairShareRatio) {
            return false;
        }

        if (!job->GetPreemptable()) {
            return false;
        }

        return true;
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
        YCHECK(OperationToElement.insert(std::make_pair(operation, operationElement)).second);

        auto poolName = spec->Pool ? *spec->Pool : operation->GetAuthenticatedUser();
        auto pool = FindPool(poolName);
        if (!pool) {
            pool = New<TPool>(Host, poolName, Config);
            RegisterPool(pool);
        }
        if (!pool->GetParent()) {
            SetPoolDefaultParent(pool);
        }
        pool->AddChild(operationElement, false);
        pool->IncreaseUsage(operationElement->ResourceUsage());
        operationElement->SetPool(pool.Get());

        if (CanAddOperationToPool(pool.Get())) {
            ActivateOperation(operation);
        } else {
            OperationQueue.push_back(operation);
            operation->SetQueued(true);
        }
    }

    void IncreaseRunningOperationCount(TCompositeSchedulerElement* element, int delta)
    {
        while (element) {
            RunningOperationCount[element->GetId()] += delta;
            element = element->GetParent();
        }
    }

    void ActivateOperation(TOperationPtr operation)
    {
        auto operationElement = GetOperationElement(operation);
        auto pool = operationElement->GetPool();
        pool->EnableChild(operationElement);
        IncreaseRunningOperationCount(pool, 1);

        LOG_INFO("Operation added to pool (OperationId: %v, Pool: %v)",
            operation->GetId(),
            pool->GetId());
    }

    void OnOperationUnregistered(TOperationPtr operation)
    {
        auto operationElement = GetOperationElement(operation);
        auto* pool = operationElement->GetPool();

        YCHECK(OperationToElement.erase(operation) == 1);
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
            IncreaseRunningOperationCount(pool, -1);

            // Try to run operations from queue.
            auto it = OperationQueue.begin();
            while (it != OperationQueue.end() && RunningOperationCount[RootPoolName] < Config->MaxRunningOperations) {
                auto operation = *it;
                if (CanAddOperationToPool(GetOperationElement(operation)->GetPool())) {
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
        auto element = FindOperationElement(operation);
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


    void OnJobStarted(TJobPtr job)
    {
        auto element = GetOperationElement(job->GetOperation());

        auto it = JobList.insert(JobList.begin(), job);
        YCHECK(JobToIterator.insert(std::make_pair(job, it)).second);

        job->SetPreemptable(true);
        element->PreemptableJobs().push_back(job);
        job->SetJobListIterator(--element->PreemptableJobs().end());

        OnJobResourceUsageUpdated(job, element, job->ResourceUsage());
    }

    void OnJobFinished(TJobPtr job)
    {
        auto element = GetOperationElement(job->GetOperation());

        auto it = JobToIterator.find(job);
        YASSERT(it != JobToIterator.end());

        JobList.erase(it->second);
        JobToIterator.erase(it);

        if (job->GetPreemptable()) {
            element->PreemptableJobs().erase(job->GetJobListIterator());
        } else {
            element->NonpreemptableJobs().erase(job->GetJobListIterator());
        }

        OnJobResourceUsageUpdated(job, element, -job->ResourceUsage());
    }

    void OnJobUpdated(TJobPtr job, const TNodeResources& resourcesDelta)
    {
        auto element = GetOperationElement(job->GetOperation());
        OnJobResourceUsageUpdated(job, element, resourcesDelta);
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
            IncreaseRunningOperationCount(oldParent, -RunningOperationCount[pool->GetId()]);
            oldParent->RemoveChild(pool);
        }

        pool->SetParent(parent.Get());
        if (parent) {
            parent->AddChild(pool);
            parent->IncreaseUsage(pool->ResourceUsage());
            IncreaseRunningOperationCount(parent.Get(), RunningOperationCount[pool->GetId()]);

            LOG_INFO("Set parent pool (Pool: %v, Parent: %v)",
                pool->GetId(),
                parent->GetId());
        }
    }

    void SetPoolDefaultParent(TPoolPtr pool)
    {
        auto defaultParentPool = FindPool(Config->DefaultParentPool);
        if (!defaultParentPool) {
            LOG_WARNING("Default parent pool %Qv is not registered", Config->DefaultParentPool);
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


    TOperationElementPtr FindOperationElement(TOperationPtr operation)
    {
        auto it = OperationToElement.find(operation);
        return it == OperationToElement.end() ? nullptr : it->second;
    }

    TOperationElementPtr GetOperationElement(TOperationPtr operation)
    {
        auto element = FindOperationElement(operation);
        YCHECK(element);
        return element;
    }

    void OnPoolsUpdated(INodePtr poolsNode)
    {
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

    void OnJobResourceUsageUpdated(
        TJobPtr job,
        TOperationElementPtr element,
        const TNodeResources& resourcesDelta)
    {
        element->IncreaseUsage(resourcesDelta);

        const auto& attributes = element->Attributes();
        auto limits = Host->GetTotalResourceLimits();

        auto& preemptableJobs = element->PreemptableJobs();
        auto& nonpreemptableJobs = element->NonpreemptableJobs();
        auto& nonpreemptableResourceUsage = element->NonpreemptableResourceUsage();

        if (!job->GetPreemptable()) {
            nonpreemptableResourceUsage += resourcesDelta;
        }

        auto getNonpreemptableUsageRatio = [&] (const TNodeResources& extraResources) -> double {
            i64 usage = GetResource(
                nonpreemptableResourceUsage + extraResources,
                attributes.DominantResource);
            i64 limit = GetResource(limits, attributes.DominantResource);
            return limit == 0 ? 1.0 : (double) usage / limit;
        };

        // Remove nonpreemptable jobs exceeding the fair share.
        while (!nonpreemptableJobs.empty()) {
            if (getNonpreemptableUsageRatio(ZeroNodeResources()) <= attributes.FairShareRatio)
                break;

            auto job = nonpreemptableJobs.back();
            YCHECK(!job->GetPreemptable());

            nonpreemptableJobs.pop_back();
            nonpreemptableResourceUsage -= job->ResourceUsage();

            preemptableJobs.push_front(job);

            job->SetPreemptable(true);
            job->SetJobListIterator(preemptableJobs.begin());
        }

        // Add more nonpreemptable jobs until filling up the fair share.
        while (!preemptableJobs.empty()) {
            auto job = preemptableJobs.front();
            YCHECK(job->GetPreemptable());

            if (getNonpreemptableUsageRatio(job->ResourceUsage()) > attributes.FairShareRatio)
                break;

            preemptableJobs.pop_front();

            nonpreemptableJobs.push_back(job);
            nonpreemptableResourceUsage += job->ResourceUsage();

            job->SetPreemptable(false);
            job->SetJobListIterator(--nonpreemptableJobs.end());
        }
    }


    static void BuildElementYson(
        TCompositeSchedulerElementPtr composite,
        ISchedulerElementPtr element,
        IYsonConsumer* consumer)
    {
        const auto& attributes = element->Attributes();
        BuildYsonMapFluently(consumer)
            .Item("scheduling_status").Value(element->GetStatus())
            .Item("starving").Value(element->GetStarving())
            .Item("resource_demand").Value(element->ResourceDemand())
            .Item("resource_usage").Value(element->ResourceUsage())
            .Item("resource_limits").Value(element->ResourceLimits())
            .Item("dominant_resource").Value(attributes.DominantResource)
            .Item("weight").Value(element->GetWeight())
            .Item("min_share_ratio").Value(element->GetMinShareRatio())
            .Item("adjusted_min_share_ratio").Value(attributes.AdjustedMinShareRatio)
            .Item("max_share_ratio").Value(attributes.MaxShareRatio)
            .Item("usage_ratio").Value(attributes.UsageRatio)
            .Item("demand_ratio").Value(attributes.DemandRatio)
            .Item("fair_share_ratio").Value(attributes.FairShareRatio)
            .Item("satisfaction_ratio").Value(attributes.SatisfactionRatio)
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

