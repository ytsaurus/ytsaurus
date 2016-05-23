#include "fair_share_strategy.h"
#include "public.h"
#include "config.h"
#include "job_resources.h"
#include "master_connector.h"
#include "scheduler_strategy.h"

#include <yt/core/concurrency/async_rw_lock.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/misc/finally.h>

#include <yt/core/profiling/profile_manager.h>
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

////////////////////////////////////////////////////////////////////

NProfiling::TTagIdList GetFailReasonProfilingTags(EScheduleJobFailReason reason)
{
    static std::unordered_map<Stroka, NProfiling::TTagId> tagId;

    auto reasonAsString = ToString(reason);
    auto it = tagId.find(reasonAsString);
    if (it == tagId.end()) {
        it = tagId.emplace(
            reasonAsString,
            NProfiling::TProfileManager::Get()->RegisterTag("reason", reasonAsString)
        ).first;
    }
    return {it->second};
};

////////////////////////////////////////////////////////////////////

struct TSchedulableAttributes
{
    EResourceType DominantResource = EResourceType::Cpu;
    double DemandRatio = 0.0;
    double FairShareRatio = 0.0;
    double AdjustedMinShareRatio = 0.0;
    double MaxPossibleUsageRatio = 1.0;
    double BestAllocationRatio = 1.0;
    i64 DominantLimit = 0;

    double AdjustedFairShareStarvationTolerance = 1.0;
    TDuration AdjustedMinSharePreemptionTimeout;
    TDuration AdjustedFairSharePreemptionTimeout;
};

struct TDynamicAttributes
{
    double SatisfactionRatio = 0.0;
    bool Active = false;
    ISchedulerElement* BestLeafDescendant = nullptr;
    TInstant MinSubtreeStartTime;
    TJobResources ResourceUsageDiscount = ZeroJobResources();
};

typedef std::vector<TDynamicAttributes> TDynamicAttributesList;

DEFINE_ENUM(ESchedulableStatus,
    (Normal)
    (BelowMinShare)
    (BelowFairShare)
);

////////////////////////////////////////////////////////////////////

struct ISchedulerElement
    : public TIntrinsicRefCounted
{
    virtual void Annotate(int& treeSize) = 0;
    virtual int GetTreeIndex() const = 0;

    virtual void Update(TDynamicAttributesList& dynamicAttributesList) = 0;
    virtual void UpdateBottomUp(TDynamicAttributesList& dynamicAttributesList) = 0;
    virtual void UpdateTopDown(TDynamicAttributesList& dynamicAttributesList) = 0;

    virtual void UpdateDynamicAttributes(TDynamicAttributesList& dynamicAttributesList) = 0;
    virtual void PrescheduleJob(TFairShareContext& context, bool starvingOnly) = 0;
    virtual bool ScheduleJob(TFairShareContext& context) = 0;

    virtual const TSchedulableAttributes& Attributes() const = 0;
    virtual TSchedulableAttributes& Attributes() = 0;
    virtual void UpdateAttributes() = 0;

    virtual TNullable<Stroka> GetNodeTag() const = 0;

    virtual bool IsActive(const TDynamicAttributesList& dynamicAttributesList) const = 0;

    virtual bool IsAlive() const = 0;
    virtual void SetAlive(bool alive) = 0;

    virtual int GetPendingJobCount() const = 0;

    virtual Stroka GetId() const = 0;

    virtual double GetWeight() const = 0;
    virtual double GetMinShareRatio() const = 0;
    virtual double GetMaxShareRatio() const = 0;

    virtual double GetFairShareStarvationTolerance() const = 0;
    virtual TDuration GetMinSharePreemptionTimeout() const = 0;
    virtual TDuration GetFairSharePreemptionTimeout() const = 0;

    virtual ESchedulableStatus GetStatus() const = 0;

    virtual bool GetStarving() const = 0;
    virtual void SetStarving(bool starving) = 0;
    virtual void CheckForStarvation(TInstant now) = 0;

    virtual const TJobResources& ResourceDemand() const = 0;
    virtual const TJobResources& ResourceLimits() const = 0;
    virtual const TJobResources& MaxPossibleResourceUsage() const = 0;

    virtual TJobResources GetResourceUsage() const = 0;
    virtual double GetResourceUsageRatio() const = 0;
    virtual void IncreaseLocalResourceUsage(const TJobResources& delta) = 0;
    virtual void IncreaseResourceUsage(const TJobResources& delta) = 0;

    virtual TCompositeSchedulerElement* GetParent() const = 0;
    virtual void SetParent(TCompositeSchedulerElement* parent) = 0;

    virtual ISchedulerElementPtr Clone() = 0;
};

////////////////////////////////////////////////////////////////////

struct TFairShareContext
{
    TFairShareContext(
        const ISchedulingContextPtr& schedulingContext,
        int treeSize)
        : SchedulingContext(schedulingContext)
        , DynamicAttributesList(treeSize)
    { }

    TDynamicAttributes& DynamicAttributes(ISchedulerElement* element)
    {
        int index = element->GetTreeIndex();
        YCHECK(index < DynamicAttributesList.size());
        return DynamicAttributesList[index];
    }

    const TDynamicAttributes& DynamicAttributes(ISchedulerElement* element) const
    {
        int index = element->GetTreeIndex();
        YCHECK(index < DynamicAttributesList.size());
        return DynamicAttributesList[index];
    }

    const ISchedulingContextPtr SchedulingContext;
    TDynamicAttributesList DynamicAttributesList;
    TDuration TotalScheduleJobDuration;
    TDuration ExecScheduleJobDuration;
    TEnumIndexedVector<int, EScheduleJobFailReason> FailedScheduleJob;
    yhash_map<TJobPtr, TOperationElementPtr> JobToOperationElement;
};

////////////////////////////////////////////////////////////////////

const int UNASSIGNED_TREE_INDEX = -1;

class TSchedulerElementBaseFixedState
{
protected:
    explicit TSchedulerElementBaseFixedState(const TJobResources& totalResourceLimits)
        : ResourceDemand_(ZeroJobResources())
        , ResourceLimits_(InfiniteJobResources())
        , MaxPossibleResourceUsage_(ZeroJobResources())
        , TotalResourceLimits_(totalResourceLimits)
    { }

    TSchedulableAttributes Attributes_;

    TCompositeSchedulerElement* Parent_ = nullptr;

    TNullable<TInstant> BelowFairShareSince_;
    bool Starving_ = false;

    TJobResources ResourceDemand_;
    TJobResources ResourceLimits_;
    TJobResources MaxPossibleResourceUsage_;
    const TJobResources TotalResourceLimits_;

    int PendingJobCount_ = 0;

    int TreeIndex_ = UNASSIGNED_TREE_INDEX;

};

class TSchedulerElementBaseSharedState
    : public TIntrinsicRefCounted
{
public:
    TSchedulerElementBaseSharedState()
        : ResourceUsage_(ZeroJobResources())
    { }

    void IncreaseResourceUsage(const TJobResources& delta)
    {
        TWriterGuard guard(ResourceUsageLock_);

        ResourceUsage_ += delta;
    }

    TJobResources GetResourceUsage()
    {
        TReaderGuard guard(ResourceUsageLock_);

        return ResourceUsage_;
    }

    double GetResourceUsageRatio(
        EResourceType dominantResource,
        i64 dominantResourceLimit)
    {
        TReaderGuard guard(ResourceUsageLock_);

        if (dominantResourceLimit == 0) {
            return 1.0;
        }
        return GetResource(ResourceUsage_, dominantResource) / dominantResourceLimit;
    }

    bool GetAlive() const
    {
        return Alive_;
    }

    void SetAlive(bool alive)
    {
        Alive_ = alive;
    }

private:
    TJobResources ResourceUsage_;
    TReaderWriterSpinLock ResourceUsageLock_;

    std::atomic<bool> Alive_ = {true};

};

typedef TIntrusivePtr<TSchedulerElementBaseSharedState> TSchedulerElementBaseSharedStatePtr;

class TSchedulerElementBase
    : public ISchedulerElement
    , public TSchedulerElementBaseFixedState
{
public:
    virtual void Annotate(int& treeSize) override
    {
        TreeIndex_ = treeSize++;
    }

    virtual int GetTreeIndex() const override
    {
        return TreeIndex_;
    }

    virtual void Update(TDynamicAttributesList& dynamicAttributesList) override
    {
        UpdateBottomUp(dynamicAttributesList);
        UpdateTopDown(dynamicAttributesList);
    }

    // Updates attributes that need to be computed from leafs up to root.
    // For example: parent->ResourceDemand = Sum(child->ResourceDemand).
    virtual void UpdateBottomUp(TDynamicAttributesList& dynamicAttributesList) override
    {
        UpdateAttributes();
        dynamicAttributesList[this->GetTreeIndex()].Active = true;
        UpdateDynamicAttributes(dynamicAttributesList);
    }

    // Updates attributes that are propagated from root down to leafs.
    // For example: child->FairShareRatio = fraction(parent->FairShareRatio).
    virtual void UpdateTopDown(TDynamicAttributesList& dynamicAttributesList) override
    { }

    virtual void UpdateDynamicAttributes(TDynamicAttributesList& dynamicAttributesList) override
    {
        YCHECK(IsActive(dynamicAttributesList));
        dynamicAttributesList[this->GetTreeIndex()].SatisfactionRatio = ComputeLocalSatisfactionRatio();
        dynamicAttributesList[this->GetTreeIndex()].Active = IsAlive();
    }

    virtual void PrescheduleJob(TFairShareContext& context, bool starvingOnly) override
    {
        UpdateDynamicAttributes(context.DynamicAttributesList);
    }

    virtual const TSchedulableAttributes& Attributes() const override
    {
        return Attributes_;
    }

    virtual TSchedulableAttributes& Attributes() override
    {
        return Attributes_;
    }

    virtual void UpdateAttributes() override
    {
        // Choose dominant resource types, compute max share ratios, compute demand ratios.
        const auto& demand = ResourceDemand();
        auto usage = GetResourceUsage();
        auto totalLimits = Host_->GetTotalResourceLimits();

        auto maxPossibleResourceUsage = Min(totalLimits, MaxPossibleResourceUsage_);

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

        double usageRatio =
            dominantLimit == 0 ? 1.0 : (double) dominantUsage / dominantLimit;

        Attributes_.DominantLimit = dominantLimit;

        Attributes_.MaxPossibleUsageRatio = GetMaxShareRatio();
        if (usageRatio > RatioComputationPrecision) {
            // In this case we know pool resource preferences and can take them into account.
            // We find maximum number K such that Usage * K < Limit and use it to estimate
            // maximum dominant resource usage.
            Attributes_.MaxPossibleUsageRatio = std::min(
                GetMinResourceRatio(maxPossibleResourceUsage, usage) * usageRatio,
                Attributes_.MaxPossibleUsageRatio);
        } else {
            // In this case we have no information about pool resource preferences, so just assume
            // that it uses all resources equally.
            Attributes_.MaxPossibleUsageRatio = std::min(
                Attributes_.DemandRatio,
                Attributes_.MaxPossibleUsageRatio);
        }
    }

    virtual TNullable<Stroka> GetNodeTag() const override
    {
        return Null;
    }

    virtual bool IsActive(const TDynamicAttributesList& dynamicAttributesList) const override
    {
        return dynamicAttributesList[GetTreeIndex()].Active;
    }

    virtual bool IsAlive() const override
    {
        return SharedState_->GetAlive();
    }

    virtual void SetAlive(bool alive) override
    {
        SharedState_->SetAlive(alive);
    }

    virtual TCompositeSchedulerElement* GetParent() const override
    {
        return Parent_;
    }

    virtual void SetParent(TCompositeSchedulerElement* parent) override
    {
        Parent_ = parent;
    }

    virtual int GetPendingJobCount() const override
    {
        return PendingJobCount_;
    }

    virtual ESchedulableStatus GetStatus() const override
    {
        return ESchedulableStatus::Normal;
    }

    virtual bool GetStarving() const override
    {
        return Starving_;
    }

    virtual void SetStarving(bool starving) override
    {
        Starving_ = starving;
    }

    virtual const TJobResources& ResourceDemand() const override
    {
        return ResourceDemand_;
    }

    virtual const TJobResources& ResourceLimits() const override
    {
        return ResourceLimits_;
    }

    virtual const TJobResources& MaxPossibleResourceUsage() const override
    {
        return MaxPossibleResourceUsage_;
    }

    TJobResources GetResourceUsage() const override
    {
        return SharedState_->GetResourceUsage();
    }

    virtual double GetResourceUsageRatio() const override
    {
        return SharedState_->GetResourceUsageRatio(
            Attributes_.DominantResource,
            Attributes_.DominantLimit);
    }

    virtual void IncreaseLocalResourceUsage(const TJobResources& delta) override
    {
        SharedState_->IncreaseResourceUsage(delta);
    }

protected:
    ISchedulerStrategyHost* const Host_;
    const TFairShareStrategyConfigPtr StrategyConfig_;

    TSchedulerElementBaseSharedStatePtr SharedState_;

    TSchedulerElementBase(
        ISchedulerStrategyHost* host,
        TFairShareStrategyConfigPtr strategyConfig)
        : TSchedulerElementBaseFixedState(host->GetTotalResourceLimits())
        , Host_(host)
        , StrategyConfig_(strategyConfig)
        , SharedState_(New<TSchedulerElementBaseSharedState>())
    { }

    TSchedulerElementBase(const TSchedulerElementBase& other)
        : TSchedulerElementBaseFixedState(other)
        , Host_(nullptr)
        , StrategyConfig_(CloneYsonSerializable(other.StrategyConfig_))
        , SharedState_(other.SharedState_)
    { }

    double ComputeLocalSatisfactionRatio() const
    {
        double minShareRatio = Attributes_.AdjustedMinShareRatio;
        double fairShareRatio = Attributes_.FairShareRatio;
        double usageRatio = GetResourceUsageRatio();

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

    ESchedulableStatus GetStatus(double defaultTolerance) const
    {
        double usageRatio = GetResourceUsageRatio();
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

    void CheckForStarvation(
        TDuration minSharePreemptionTimeout,
        TDuration fairSharePreemptionTimeout,
        TInstant now)
    {
        auto status = GetStatus();
        switch (status) {
            case ESchedulableStatus::BelowMinShare:
                if (!BelowFairShareSince_) {
                    BelowFairShareSince_ = now;
                } else if (BelowFairShareSince_.Get() < now - minSharePreemptionTimeout) {
                    SetStarving(true);
                }
                break;

            case ESchedulableStatus::BelowFairShare:
                if (!BelowFairShareSince_) {
                    BelowFairShareSince_ = now;
                } else if (BelowFairShareSince_.Get() < now - fairSharePreemptionTimeout) {
                    SetStarving(true);
                }
                break;

            case ESchedulableStatus::Normal:
                BelowFairShareSince_ = Null;
                SetStarving(false);
                break;

            default:
                YUNREACHABLE();
        }
    }

};

////////////////////////////////////////////////////////////////////

class TCompositeSchedulerElementFixedState
{
protected:
    TCompositeSchedulerElementFixedState()
        : RunningOperationCount_(0)
        , OperationCount_(0)
    { }

    ESchedulingMode Mode_ = ESchedulingMode::Fifo;
    std::vector<EFifoSortParameter> FifoSortParameters_;

    DEFINE_BYREF_RW_PROPERTY(int, RunningOperationCount);
    DEFINE_BYREF_RW_PROPERTY(int, OperationCount);

    DEFINE_BYREF_RO_PROPERTY(double, AdjustedFairShareStarvationToleranceLimit);
    DEFINE_BYREF_RO_PROPERTY(TDuration, AdjustedMinSharePreemptionTimeoutLimit);
    DEFINE_BYREF_RO_PROPERTY(TDuration, AdjustedFairSharePreemptionTimeoutLimit);

};

class TCompositeSchedulerElement
    : public TSchedulerElementBase
    , public TCompositeSchedulerElementFixedState
{
public:
    TCompositeSchedulerElement(
        ISchedulerStrategyHost* host,
        TFairShareStrategyConfigPtr strategyConfig)
        : TSchedulerElementBase(host, strategyConfig)
    { }

    TCompositeSchedulerElement(const TCompositeSchedulerElement& other)
        : TSchedulerElementBase(other)
        , TCompositeSchedulerElementFixedState(other)
    {
        for (const auto& child : other.Children) {
            auto childClone = child->Clone();
            childClone->SetParent(this);
            Children.insert(childClone);
        }
        for (const auto& child : other.DisabledChildren) {
            auto childClone = child->Clone();
            childClone->SetParent(this);
            DisabledChildren.insert(childClone);
        }
    }

    virtual void Annotate(int& treeSize) override
    {
        TSchedulerElementBase::Annotate(treeSize);
        for (const auto& child : Children) {
            child->Annotate(treeSize);
        }
    }

    virtual void UpdateBottomUp(TDynamicAttributesList& dynamicAttributesList) override
    {
        Attributes_.BestAllocationRatio = 0.0;
        PendingJobCount_ = 0;
        ResourceDemand_ = ZeroJobResources();
        auto maxPossibleChildrenResourceUsage_ = ZeroJobResources();
        for (const auto& child : Children) {
            child->UpdateBottomUp(dynamicAttributesList);

            Attributes_.BestAllocationRatio = std::max(
                Attributes_.BestAllocationRatio,
                child->Attributes().BestAllocationRatio);

            PendingJobCount_ += child->GetPendingJobCount();
            ResourceDemand_ += child->ResourceDemand();
            maxPossibleChildrenResourceUsage_ += child->MaxPossibleResourceUsage();
        }
        MaxPossibleResourceUsage_ = Min(maxPossibleChildrenResourceUsage_, ResourceLimits_);
        TSchedulerElementBase::UpdateBottomUp(dynamicAttributesList);
    }

    virtual double GetFairShareStarvationToleranceLimit() const
    {
        return 1.0;
    }

    virtual TDuration GetMinSharePreemptionTimeoutLimit() const
    {
        return TDuration::Zero();
    }

    virtual TDuration GetFairSharePreemptionTimeoutLimit() const
    {
        return TDuration::Zero();
    }

    void UpdatePreemptionSettingsLimits()
    {
        if (Parent_) {
            AdjustedFairShareStarvationToleranceLimit_ = std::min(
                GetFairShareStarvationToleranceLimit(),
                Parent_->AdjustedFairShareStarvationToleranceLimit());

            AdjustedMinSharePreemptionTimeoutLimit_ = std::max(
                GetMinSharePreemptionTimeoutLimit(),
                Parent_->AdjustedMinSharePreemptionTimeoutLimit());

            AdjustedFairSharePreemptionTimeoutLimit_ = std::max(
                GetFairSharePreemptionTimeoutLimit(),
                Parent_->AdjustedFairSharePreemptionTimeoutLimit());
        }
    }

    void UpdateChildPreemptionSettings(const ISchedulerElementPtr& child)
    {
        auto& childAttributes = child->Attributes();

        childAttributes.AdjustedFairShareStarvationTolerance = std::min(
            child->GetFairShareStarvationTolerance(),
            AdjustedFairShareStarvationToleranceLimit_);

        childAttributes.AdjustedMinSharePreemptionTimeout = std::max(
            child->GetMinSharePreemptionTimeout(),
            AdjustedMinSharePreemptionTimeoutLimit_);

        childAttributes.AdjustedFairSharePreemptionTimeout = std::max(
            child->GetFairSharePreemptionTimeout(),
            AdjustedFairSharePreemptionTimeoutLimit_);
    }

    virtual void UpdateTopDown(TDynamicAttributesList& dynamicAttributesList) override
    {
        switch (Mode_) {
            case ESchedulingMode::Fifo:
                // Easy case -- the first child get everything, others get none.
                UpdateFifo(dynamicAttributesList);
                break;

            case ESchedulingMode::FairShare:
                // Hard case -- compute fair shares using fit factor.
                UpdateFairShare(dynamicAttributesList);
                break;

            default:
                YUNREACHABLE();
        }

        UpdatePreemptionSettingsLimits();

        // Propagate updates to children.
        for (const auto& child : Children) {
            UpdateChildPreemptionSettings(child);
            child->UpdateTopDown(dynamicAttributesList);
        }
    }

    virtual void UpdateDynamicAttributes(TDynamicAttributesList& dynamicAttributesList) override
    {
        YCHECK(IsActive(dynamicAttributesList));
        auto& attributes = dynamicAttributesList[this->GetTreeIndex()];

        if (!IsAlive()) {
            attributes.Active = false;
            return;
        }

        // Compute local satisfaction ratio.
        attributes.SatisfactionRatio = ComputeLocalSatisfactionRatio();
        // Start times bubble up from leaf nodes with operations.
        attributes.MinSubtreeStartTime = TInstant::Max();
        // Adjust satisfaction ratio using children.
        // Declare the element passive if all children are passive.
        attributes.Active = false;
        attributes.BestLeafDescendant = nullptr;

        while (auto bestChild = GetBestActiveChild(dynamicAttributesList)) {
            const auto& bestChildAttributes = dynamicAttributesList[bestChild->GetTreeIndex()];
            auto childBestLeafDescendant = bestChildAttributes.BestLeafDescendant;
            if (!childBestLeafDescendant->IsAlive()) {
                bestChild->UpdateDynamicAttributes(dynamicAttributesList);
                if (!bestChildAttributes.Active) {
                    continue;
                }
                childBestLeafDescendant = bestChildAttributes.BestLeafDescendant;
            }

            // We need to evaluate both MinSubtreeStartTime and SatisfactionRatio
            // because parent can use different scheduling mode.
            attributes.MinSubtreeStartTime = std::min(
                attributes.MinSubtreeStartTime,
                bestChildAttributes.MinSubtreeStartTime);

            attributes.SatisfactionRatio = std::min(
                attributes.SatisfactionRatio,
                bestChildAttributes.SatisfactionRatio);

            attributes.BestLeafDescendant = childBestLeafDescendant;
            attributes.Active = true;
            break;
        }
    }

    virtual void PrescheduleJob(TFairShareContext& context, bool starvingOnly) override
    {
        auto& attributes = context.DynamicAttributes(this);

        attributes.Active = true;

        if (!IsAlive()) {
            attributes.Active = false;
            return;
        }

        if (!context.SchedulingContext->CanSchedule(GetNodeTag())) {
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
        auto& attributes = context.DynamicAttributes(this);
        if (!attributes.Active) {
            return false;
        }

        auto bestLeafDescendant = attributes.BestLeafDescendant;
        if (!bestLeafDescendant->IsAlive()) {
            UpdateDynamicAttributes(context.DynamicAttributesList);
            if (!attributes.Active) {
                return false;
            }
            bestLeafDescendant = attributes.BestLeafDescendant;
        }

        // NB: Ignore the child's result.
        bestLeafDescendant->ScheduleJob(context);
        return true;
    }

    virtual void IncreaseResourceUsage(const TJobResources& delta) override
    {
        auto* currentElement = this;
        while (currentElement) {
            currentElement->IncreaseLocalResourceUsage(delta);
            currentElement = currentElement->GetParent();
        }
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

protected:
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


    void UpdateFifo(TDynamicAttributesList& dynamicAttributesList)
    {
        // TODO(acid): This code shouldn't use active children.
        const auto& bestChild = GetBestActiveChildFifo(dynamicAttributesList);
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

    void UpdateFairShare(TDynamicAttributesList& dynamicAttributesList)
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


    ISchedulerElementPtr GetBestActiveChild(const TDynamicAttributesList& dynamicAttributesList) const
    {
        switch (Mode_) {
            case ESchedulingMode::Fifo:
                return GetBestActiveChildFifo(dynamicAttributesList);
            case ESchedulingMode::FairShare:
                return GetBestActiveChildFairShare(dynamicAttributesList);
            default:
                YUNREACHABLE();
        }
    }

    ISchedulerElementPtr GetBestActiveChildFifo(const TDynamicAttributesList& dynamicAttributesList) const
    {
        auto isBetter = [this, &dynamicAttributesList] (const ISchedulerElementPtr& lhs, const ISchedulerElementPtr& rhs) -> bool {
            for (auto parameter : FifoSortParameters_) {
                switch (parameter) {
                    case EFifoSortParameter::Weight:
                        if (lhs->GetWeight() != rhs->GetWeight()) {
                            return lhs->GetWeight() > rhs->GetWeight();
                        }
                        break;
                    case EFifoSortParameter::StartTime: {
                        const auto& lhsStartTime = dynamicAttributesList[lhs->GetTreeIndex()].MinSubtreeStartTime;
                        const auto& rhsStartTime = dynamicAttributesList[rhs->GetTreeIndex()].MinSubtreeStartTime;
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
            if (child->IsActive(dynamicAttributesList)) {
                if (bestChild && isBetter(bestChild, child))
                    continue;

                bestChild = child.Get();
            }
        }
        return bestChild;
    }

    ISchedulerElementPtr GetBestActiveChildFairShare(const TDynamicAttributesList& dynamicAttributesList) const
    {
        ISchedulerElement* bestChild = nullptr;
        double bestChildSatisfactionRatio = std::numeric_limits<double>::max();
        for (const auto& child : Children) {
            if (child->IsActive(dynamicAttributesList)) {
                double childSatisfactionRatio = dynamicAttributesList[child->GetTreeIndex()].SatisfactionRatio;
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

class TPoolFixedState
{
protected:
    explicit TPoolFixedState(const Stroka& id)
        : Id_(id)
    { }

    const Stroka Id_;
    bool DefaultConfigured_ = true;

};

class TPool
    : public TCompositeSchedulerElement
    , public TPoolFixedState
{
public:
    TPool(
        ISchedulerStrategyHost* host,
        const Stroka& id,
        TFairShareStrategyConfigPtr strategyConfig)
        : TCompositeSchedulerElement(host, strategyConfig)
        , TPoolFixedState(id)
    {
        SetDefaultConfig();
    }

    TPool(const TPool& other)
        : TCompositeSchedulerElement(other)
        , TPoolFixedState(other)
        , Config_(CloneYsonSerializable(other.Config_))
    { }

    bool IsDefaultConfigured() const
    {
        return DefaultConfigured_;
    }

    TPoolConfigPtr GetConfig()
    {
        return Config_;
    }

    void SetConfig(TPoolConfigPtr config)
    {
        DoSetConfig(config);
        DefaultConfigured_ = false;
    }

    void SetDefaultConfig()
    {
        DoSetConfig(New<TPoolConfig>());
        DefaultConfigured_ = true;
    }

    virtual Stroka GetId() const override
    {
        return Id_;
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
        return TSchedulerElementBase::GetStatus(Attributes_.AdjustedFairShareStarvationTolerance);
    }

    virtual double GetFairShareStarvationTolerance() const override
    {
        return Config_->FairShareStarvationTolerance.Get(Parent_->Attributes().AdjustedFairShareStarvationTolerance);
    }

    virtual TDuration GetMinSharePreemptionTimeout() const override
    {
        return Config_->MinSharePreemptionTimeout.Get(Parent_->Attributes().AdjustedMinSharePreemptionTimeout);
    }

    virtual TDuration GetFairSharePreemptionTimeout() const override
    {
        return Config_->FairSharePreemptionTimeout.Get(Parent_->Attributes().AdjustedFairSharePreemptionTimeout);
    }

    virtual double GetFairShareStarvationToleranceLimit() const override
    {
        return Config_->FairShareStarvationToleranceLimit.Get(StrategyConfig_->FairShareStarvationToleranceLimit);
    }

    virtual TDuration GetMinSharePreemptionTimeoutLimit() const override
    {
        return Config_->MinSharePreemptionTimeoutLimit.Get(StrategyConfig_->MinSharePreemptionTimeoutLimit);
    }

    virtual TDuration GetFairSharePreemptionTimeoutLimit() const override
    {
        return Config_->FairSharePreemptionTimeoutLimit.Get(StrategyConfig_->FairSharePreemptionTimeoutLimit);
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
            Attributes_.AdjustedMinSharePreemptionTimeout,
            Attributes_.AdjustedFairSharePreemptionTimeout,
            now);
    }

    virtual TNullable<Stroka> GetNodeTag() const override
    {
        return Config_->SchedulingTag;
    }

    virtual void UpdateBottomUp(TDynamicAttributesList& dynamicAttributesList) override
    {
        ResourceLimits_ = ComputeResourceLimits();
        TCompositeSchedulerElement::UpdateBottomUp(dynamicAttributesList);
    }

    virtual int GetMaxRunningOperationCount() const override
    {
        return Config_->MaxRunningOperationCount.Get(StrategyConfig_->MaxRunningOperationCountPerPool);
    }

    virtual int GetMaxOperationCount() const override
    {
        return Config_->MaxOperationCount.Get(StrategyConfig_->MaxOperationCountPerPool);
    }

    virtual ISchedulerElementPtr Clone() override
    {
        return New<TPool>(*this);
    }

private:
    TPoolConfigPtr Config_;

    void DoSetConfig(TPoolConfigPtr newConfig)
    {
        Config_ = newConfig;
        FifoSortParameters_ = Config_->FifoSortParameters;
        Mode_ = Config_->Mode;
    }

    TJobResources ComputeResourceLimits() const
    {
        auto maxShareLimits = Host_->GetResourceLimits(GetNodeTag()) * Config_->MaxShareRatio;
        auto perTypeLimits = Config_->ResourceLimits->ToJobResources();
        return Min(maxShareLimits, perTypeLimits);
    }

};

////////////////////////////////////////////////////////////////////

class TOperationElementFixedState
{
protected:
    explicit TOperationElementFixedState(TOperationPtr operation)
        : OperationId_(operation->GetId())
        , Operation_(operation)
    { }

    const TOperationId OperationId_;

    DEFINE_BYVAL_RO_PROPERTY(TOperationPtr, Operation);
};

class TOperationElementSharedState
    : public TIntrinsicRefCounted
{
public:
    TOperationElementSharedState()
        : NonpreemptableResourceUsage_(ZeroJobResources())
    { }

    void IncreaseJobResourceUsage(const TJobId& jobId, const TJobResources& resourcesDelta)
    {
        TWriterGuard guard(JobPropertiesMapLock_);

        IncreaseJobResourceUsage(JobProperties(jobId), resourcesDelta);
    }

    void UpdatePreemptableJobsList(double fairShareRatio, const TJobResources& totalResourceLimits)
    {
        TWriterGuard guard(JobPropertiesMapLock_);

        auto dominantResource = GetDominantResource(NonpreemptableResourceUsage_, totalResourceLimits);
        i64 dominantLimit = GetResource(totalResourceLimits, dominantResource);

        auto getNonpreemptableUsageRatio = [&] (const TJobResources& extraResources) -> double {
            i64 usage = GetResource(NonpreemptableResourceUsage_ + extraResources, dominantResource);
            return dominantLimit == 0 ? 1.0 : (double) usage / dominantLimit;
        };

        // Remove nonpreemptable jobs exceeding the fair share.
        while (!NonpreemptableJobs_.empty()) {
            if (getNonpreemptableUsageRatio(ZeroJobResources()) <= fairShareRatio) {
                break;
            }

            auto jobId = NonpreemptableJobs_.back();
            auto& jobProperties = JobProperties(jobId);
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
            auto& jobProperties = JobProperties(jobId);
            YCHECK(jobProperties.Preemptable);

            if (getNonpreemptableUsageRatio(jobProperties.ResourceUsage) > fairShareRatio) {
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
        TReaderGuard guard(JobPropertiesMapLock_);

        return JobPropertiesMap_.find(jobId) != JobPropertiesMap_.end();
    }

    bool IsJobPreemptable(const TJobId& jobId) const
    {
        TReaderGuard guard(JobPropertiesMapLock_);

        return JobProperties(jobId).Preemptable;
    }

    int GetPreemptableJobCount() const
    {
        TReaderGuard guard(JobPropertiesMapLock_);

        return PreemptableJobs_.size();
    }

    void AddJob(const TJobId& jobId, const TJobResources resourceUsage)
    {
        TWriterGuard guard(JobPropertiesMapLock_);

        PreemptableJobs_.push_back(jobId);

        auto it = JobPropertiesMap_.insert(std::make_pair(
            jobId,
            TJobProperties(true, --PreemptableJobs_.end(), ZeroJobResources())));
        YCHECK(it.second);

        IncreaseJobResourceUsage(it.first->second, resourceUsage);
    }

    TJobResources RemoveJob(const TJobId& jobId)
    {
        TWriterGuard guard(JobPropertiesMapLock_);

        auto it = JobPropertiesMap_.find(jobId);
        YCHECK(it != JobPropertiesMap_.end());

        auto& properties = it->second;
        if (properties.Preemptable) {
            PreemptableJobs_.erase(properties.JobIdListIterator);
        } else {
            NonpreemptableJobs_.erase(properties.JobIdListIterator);
        }

        auto resourceUsage = properties.ResourceUsage;
        IncreaseJobResourceUsage(properties, -resourceUsage);

        JobPropertiesMap_.erase(it);

        return resourceUsage;
    }

    bool IsBlocked(
        TInstant now,
        int MaxConcurrentScheduleJobCalls,
        const TDuration& ScheduleJobFailBackoffTime) const
    {
        TReaderGuard guard(ConcurrentScheduleJobCallsLock_);

        return IsBlockedImpl(now, MaxConcurrentScheduleJobCalls, ScheduleJobFailBackoffTime);
    }

    bool TryStartScheduleJobCall(
        TInstant now,
        int MaxConcurrentScheduleJobCalls,
        const TDuration& ScheduleJobFailBackoffTime)
    {
        TWriterGuard guard(ConcurrentScheduleJobCallsLock_);

        if (IsBlockedImpl(now, MaxConcurrentScheduleJobCalls, ScheduleJobFailBackoffTime)) {
            return false;
        }

        BackingOff_ = false;
        ++ConcurrentScheduleJobCalls_;
        return true;
    }

    void FinishScheduleJobCall()
    {
        TWriterGuard guard(ConcurrentScheduleJobCallsLock_);

        --ConcurrentScheduleJobCalls_;
    }

    void FailScheduleJobCall(TInstant now)
    {
        TWriterGuard guard(ConcurrentScheduleJobCallsLock_);

        BackingOff_ = true;
        LastScheduleJobFailTime_ = now;
    }

private:
    typedef std::list<TJobId> TJobIdList;

    TJobIdList NonpreemptableJobs_;
    TJobIdList PreemptableJobs_;

    TJobResources NonpreemptableResourceUsage_;

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
    TReaderWriterSpinLock JobPropertiesMapLock_;

    int ConcurrentScheduleJobCalls_ = 0;
    TInstant LastScheduleJobFailTime_;
    bool BackingOff_ = false;
    TReaderWriterSpinLock ConcurrentScheduleJobCallsLock_;

    TJobProperties& JobProperties(const TJobId& jobId)
    {
        auto it = JobPropertiesMap_.find(jobId);
        YCHECK(it != JobPropertiesMap_.end());
        return it->second;
    }

    const TJobProperties& JobProperties(const TJobId& jobId) const
    {
        auto it = JobPropertiesMap_.find(jobId);
        YCHECK(it != JobPropertiesMap_.end());
        return it->second;
    }

    bool IsBlockedImpl(
        TInstant now,
        int MaxConcurrentScheduleJobCalls,
        const TDuration& ScheduleJobFailBackoffTime) const
    {
        return ConcurrentScheduleJobCalls_ >= MaxConcurrentScheduleJobCalls ||
            (BackingOff_ && LastScheduleJobFailTime_ + ScheduleJobFailBackoffTime > now);
    }

    void IncreaseJobResourceUsage(TJobProperties& properties, const TJobResources& resourcesDelta)
    {
        properties.ResourceUsage += resourcesDelta;
        if (!properties.Preemptable) {
            NonpreemptableResourceUsage_ += resourcesDelta;
        }
    }
};

typedef TIntrusivePtr<TOperationElementSharedState> TOperationElementSharedStatePtr;

class TOperationElement
    : public TSchedulerElementBase
    , public TOperationElementFixedState
{
public:
    TOperationElement(
        TFairShareStrategyConfigPtr strategyConfig,
        TStrategyOperationSpecPtr spec,
        TOperationRuntimeParamsPtr runtimeParams,
        ISchedulerStrategyHost* host,
        TOperationPtr operation)
        : TSchedulerElementBase(host, strategyConfig)
        , TOperationElementFixedState(operation)
        , Spec_(spec)
        , RuntimeParams_(runtimeParams)
        , SharedState_(New<TOperationElementSharedState>())
    { }

    TOperationElement(const TOperationElement& other)
        : TSchedulerElementBase(other)
        , TOperationElementFixedState(other)
        , Spec_(CloneYsonSerializable(other.Spec_))
        , RuntimeParams_(CloneYsonSerializable(other.RuntimeParams_))
        , SharedState_(other.SharedState_)
    { }

    virtual double GetFairShareStarvationTolerance() const override
    {
        return Spec_->FairShareStarvationTolerance.Get(Parent_->Attributes().AdjustedFairShareStarvationTolerance);
    }

    virtual TDuration GetMinSharePreemptionTimeout() const override
    {
        return Spec_->MinSharePreemptionTimeout.Get(Parent_->Attributes().AdjustedMinSharePreemptionTimeout);
    }

    virtual TDuration GetFairSharePreemptionTimeout() const override
    {
        return Spec_->FairSharePreemptionTimeout.Get(Parent_->Attributes().AdjustedFairSharePreemptionTimeout);
    }

    virtual void UpdateBottomUp(TDynamicAttributesList& dynamicAttributesList) override
    {
        TSchedulerElementBase::UpdateBottomUp(dynamicAttributesList);

        ResourceDemand_ = ComputeResourceDemand();
        ResourceLimits_ = ComputeResourceLimits();
        MaxPossibleResourceUsage_ = ComputeMaxPossibleResourceUsage();
        PendingJobCount_ = ComputePendingJobCount();

        auto totalLimits = Host_->GetTotalResourceLimits();
        auto allocationLimits = GetAdjustedResourceLimits(
            ResourceDemand_,
            totalLimits,
            Host_->GetExecNodeCount());

        i64 dominantLimit = GetResource(totalLimits, Attributes_.DominantResource);
        i64 dominantAllocationLimit = GetResource(allocationLimits, Attributes_.DominantResource);

        Attributes_.BestAllocationRatio =
            dominantLimit == 0 ? 1.0 : (double) dominantAllocationLimit / dominantLimit;
    }

    virtual void UpdateDynamicAttributes(TDynamicAttributesList& dynamicAttributesList) override
    {
        auto& attributes = dynamicAttributesList[this->GetTreeIndex()];
        attributes.Active = true;
        attributes.BestLeafDescendant = this;
        attributes.MinSubtreeStartTime = Operation_->GetStartTime();

        TSchedulerElementBase::UpdateDynamicAttributes(dynamicAttributesList);
    }

    virtual void PrescheduleJob(TFairShareContext& context, bool starvingOnly) override
    {
        auto& attributes = context.DynamicAttributes(this);

        attributes.Active = true;

        if (!IsAlive()) {
            attributes.Active = false;
            return;
        }

        if (!context.SchedulingContext->CanSchedule(GetNodeTag())) {
            attributes.Active = false;
            return;
        }

        if (starvingOnly && !Starving_) {
            attributes.Active = false;
            return;
        }

        if (IsBlocked(context.SchedulingContext->GetNow())) {
            attributes.Active = false;
            return;
        }

        TSchedulerElementBase::PrescheduleJob(context, starvingOnly);
    }

    virtual bool ScheduleJob(TFairShareContext& context) override
    {
        YCHECK(IsActive(context.DynamicAttributesList));

        auto updateAncestorsAttributes = [&] () {
            auto* parent = GetParent();
            while (parent) {
                parent->UpdateDynamicAttributes(context.DynamicAttributesList);
                parent = parent->GetParent();
            }
        };

        auto disableOperationElement = [&] () {
            context.DynamicAttributes(this).Active = false;
            updateAncestorsAttributes();
        };

        auto now = context.SchedulingContext->GetNow();
        if (IsBlocked(now))
        {
            disableOperationElement();
            return false;
        }

        if (!SharedState_->TryStartScheduleJobCall(
            now,
            StrategyConfig_->MaxConcurrentControllerScheduleJobCalls,
            StrategyConfig_->ControllerScheduleJobFailBackoffTime))
        {
            disableOperationElement();
            return false;
        }

        auto scheduleJobGuard = Finally([&] {
            SharedState_->FinishScheduleJobCall();
        });

        NProfiling::TScopedTimer timer;
        auto scheduleJobResult = DoScheduleJob(context);
        auto scheduleJobDuration = timer.GetElapsed();
        context.TotalScheduleJobDuration += scheduleJobDuration;
        context.ExecScheduleJobDuration += scheduleJobResult->Duration;

        for (auto reason : TEnumTraits<EScheduleJobFailReason>::GetDomainValues()) {
            context.FailedScheduleJob[reason] += scheduleJobResult->Failed[reason];
        }

        if (!scheduleJobResult->JobStartRequest) {
            disableOperationElement();
            Operation_->UpdateControllerTimeStatistics("/schedule_job/fail", scheduleJobDuration);

            if (scheduleJobResult->Failed[EScheduleJobFailReason::NotEnoughResources] == 0 &&
                scheduleJobResult->Failed[EScheduleJobFailReason::NoLocalJobs] == 0)
            {
                LOG_DEBUG("Failed to schedule job, backing off (OperationId: %v, Reasons: %v)",
                    OperationId_,
                    scheduleJobResult->Failed);
                SharedState_->FailScheduleJobCall(context.SchedulingContext->GetNow());
            }

            return false;
        }

        const auto& jobStartRequest = scheduleJobResult->JobStartRequest.Get();
        context.SchedulingContext->ResourceUsage() += jobStartRequest.ResourceLimits;
        OnJobStarted(jobStartRequest.Id, jobStartRequest.ResourceLimits);
        auto job = context.SchedulingContext->StartJob(Operation_, jobStartRequest);
        context.JobToOperationElement[job] = this;

        UpdateDynamicAttributes(context.DynamicAttributesList);
        updateAncestorsAttributes();
        Operation_->UpdateControllerTimeStatistics("/schedule_job/success", scheduleJobDuration);

        // TODO(acid): Check hierarchical resource usage here.

        return true;
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

    virtual TNullable<Stroka> GetNodeTag() const override
    {
        return Spec_->SchedulingTag;
    }

    virtual ESchedulableStatus GetStatus() const override
    {
        if (!Operation_->IsSchedulable()) {
            return ESchedulableStatus::Normal;
        }

        if (GetPendingJobCount() == 0) {
            return ESchedulableStatus::Normal;
        }

        return TSchedulerElementBase::GetStatus(Attributes_.AdjustedFairShareStarvationTolerance);
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
        auto minSharePreemptionTimeout = Attributes_.AdjustedMinSharePreemptionTimeout;
        auto fairSharePreemptionTimeout = Attributes_.AdjustedFairSharePreemptionTimeout;

        double jobCountRatio = GetPendingJobCount() / StrategyConfig_->JobCountPreemptionTimeoutCoefficient;

        if (jobCountRatio < 1.0) {
            minSharePreemptionTimeout *= jobCountRatio;
            fairSharePreemptionTimeout *= jobCountRatio;
        }

        TSchedulerElementBase::CheckForStarvation(
            minSharePreemptionTimeout,
            fairSharePreemptionTimeout,
            now);
    }

    virtual void IncreaseResourceUsage(const TJobResources& delta) override
    {
        IncreaseLocalResourceUsage(delta);
        GetParent()->IncreaseResourceUsage(delta);
    }

    bool HasStarvingParent() const
    {
        auto* parent = GetParent();
        while (parent) {
            if (parent->GetStarving()) {
                return true;
            }
            parent = parent->GetParent();
        }
        return false;
    }

    void IncreaseJobResourceUsage(const TJobId& jobId, const TJobResources& resourcesDelta)
    {
        IncreaseResourceUsage(resourcesDelta);
        SharedState_->IncreaseJobResourceUsage(jobId, resourcesDelta);
        SharedState_->UpdatePreemptableJobsList(Attributes_.FairShareRatio, TotalResourceLimits_);
    }

    bool IsJobExisting(const TJobId& jobId) const
    {
        return SharedState_->IsJobExisting(jobId);
    }

    bool IsJobPreemptable(const TJobId& jobId) const
    {
        return SharedState_->IsJobPreemptable(jobId);
    }

    int GetPreemptableJobCount() const
    {
        return SharedState_->GetPreemptableJobCount();
    }

    void OnJobStarted(const TJobId& jobId, const TJobResources& resourceUsage)
    {
        SharedState_->AddJob(jobId, resourceUsage);
        IncreaseResourceUsage(resourceUsage);
    }

    void OnJobFinished(const TJobId& jobId)
    {
        auto resourceUsage = SharedState_->RemoveJob(jobId);
        IncreaseResourceUsage(-resourceUsage);
    }

    virtual ISchedulerElementPtr Clone() override
    {
        return New<TOperationElement>(*this);
    }

    DEFINE_BYVAL_RO_PROPERTY(TStrategyOperationSpecPtr, Spec);
    DEFINE_BYVAL_RO_PROPERTY(TOperationRuntimeParamsPtr, RuntimeParams);

private:
    TOperationElementSharedStatePtr SharedState_;

    bool IsBlocked(TInstant now) const
    {
        return !Operation_->IsSchedulable() ||
            GetPendingJobCount() == 0 ||
            SharedState_->IsBlocked(
                now,
                StrategyConfig_->MaxConcurrentControllerScheduleJobCalls,
                StrategyConfig_->ControllerScheduleJobFailBackoffTime);
    }

    TJobResources GetHierarchicalResourceLimits(const TFairShareContext& context) const
    {
        const auto& schedulingContext = context.SchedulingContext;

        // Bound limits with node free resources.
        auto limits =
            schedulingContext->ResourceLimits()
            - schedulingContext->ResourceUsage()
            + schedulingContext->ResourceUsageDiscount();

        // Bound limits with pool free resources.
        auto* parent = GetParent();
        while (parent) {
            auto parentLimits =
                parent->ResourceLimits()
                - parent->GetResourceUsage()
                + context.DynamicAttributes(parent).ResourceUsageDiscount;

            limits = Min(limits, parentLimits);
            parent = parent->GetParent();
        }

        // Bound limits with operation free resources.
        limits = Min(limits, ResourceLimits() - GetResourceUsage());

        return limits;
    }

    TScheduleJobResultPtr DoScheduleJob(TFairShareContext& context)
    {
        auto jobLimits = GetHierarchicalResourceLimits(context);
        auto controller = Operation_->GetController();

        auto scheduleJobResultFuture = BIND(&IOperationController::ScheduleJob, controller)
            .AsyncVia(controller->GetCancelableInvoker())
            .Run(context.SchedulingContext, jobLimits);

        auto scheduleJobResultFutureWithTimeout = scheduleJobResultFuture
            .WithTimeout(StrategyConfig_->ControllerScheduleJobTimeLimit);

        auto scheduleJobResultWithTimeoutOrError = WaitFor(scheduleJobResultFutureWithTimeout);

        if (!scheduleJobResultWithTimeoutOrError.IsOK()) {
            auto scheduleJobResult = New<TScheduleJobResult>();
            if (scheduleJobResultWithTimeoutOrError.GetCode() == NYT::EErrorCode::Timeout) {
                LOG_WARNING("Controller is scheduling for too long, aborting ScheduleJob");
                ++scheduleJobResult->Failed[EScheduleJobFailReason::Timeout];
                // If ScheduleJob was not canceled we need to abort created job.
                scheduleJobResultFuture.Subscribe(
                    BIND([=] (const TErrorOr<TScheduleJobResultPtr>& scheduleJobResultOrError) {
                        if (scheduleJobResultOrError.IsOK()) {
                            const auto& scheduleJobResult = scheduleJobResultOrError.Value();
                            if (scheduleJobResult->JobStartRequest) {
                                const auto& jobId = scheduleJobResult->JobStartRequest->Id;
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
            }
            return scheduleJobResult;
        }

        return scheduleJobResultWithTimeoutOrError.Value();
    }

    TJobResources ComputeResourceDemand() const
    {
        if (Operation_->IsSchedulable()) {
            const auto& controller = Operation_->GetController();
            return GetResourceUsage() + controller->GetNeededResources();
        }
        return ZeroJobResources();
    }

    TJobResources ComputeResourceLimits() const
    {
        auto resourceLimits = Host_->GetResourceLimits(GetNodeTag()) * Spec_->MaxShareRatio;
        auto perTypeLimits = Spec_->ResourceLimits->ToJobResources();
        return Min(resourceLimits, perTypeLimits);
    }

    TJobResources ComputeMaxPossibleResourceUsage() const
    {
        return Min(ResourceLimits(), ResourceDemand());
    }

    int ComputePendingJobCount() const
    {
        return Operation_->GetController()->GetPendingJobCount();
    }

};

////////////////////////////////////////////////////////////////////

class TRootElementFixedState
{
protected:
    DEFINE_BYVAL_RO_PROPERTY(int, TreeSize);

};

class TRootElement
    : public TCompositeSchedulerElement
    , public TRootElementFixedState
{
public:
    TRootElement(
        ISchedulerStrategyHost* host,
        TFairShareStrategyConfigPtr strategyConfig)
        : TCompositeSchedulerElement(host, strategyConfig)
    {
        Attributes_.FairShareRatio = 1.0;
        Attributes_.AdjustedMinShareRatio = 1.0;
        Mode_ = ESchedulingMode::FairShare;
        Attributes_.AdjustedFairShareStarvationTolerance = GetFairShareStarvationTolerance();
        Attributes_.AdjustedMinSharePreemptionTimeout = GetMinSharePreemptionTimeout();
        Attributes_.AdjustedFairSharePreemptionTimeout = GetFairSharePreemptionTimeout();
        AdjustedFairShareStarvationToleranceLimit_ = GetFairShareStarvationToleranceLimit();
        AdjustedMinSharePreemptionTimeoutLimit_ = GetMinSharePreemptionTimeoutLimit();
        AdjustedFairSharePreemptionTimeoutLimit_ = GetFairSharePreemptionTimeoutLimit();
    }

    virtual void Update(TDynamicAttributesList& dynamicAttributesList) override
    {
        TreeSize_ = 0;
        TCompositeSchedulerElement::Annotate(TreeSize_);

        dynamicAttributesList.assign(TreeSize_, TDynamicAttributes());
        TCompositeSchedulerElement::Update(dynamicAttributesList);
    }

    virtual bool IsRoot() const override
    {
        return true;
    }

    virtual TNullable<Stroka> GetNodeTag() const override
    {
        return Null;
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

    virtual double GetFairShareStarvationTolerance() const override
    {
        return StrategyConfig_->FairShareStarvationTolerance;
    }

    virtual TDuration GetMinSharePreemptionTimeout() const override
    {
        return StrategyConfig_->MinSharePreemptionTimeout;
    }

    virtual TDuration GetFairSharePreemptionTimeout() const override
    {
        return StrategyConfig_->FairSharePreemptionTimeout;
    }

    virtual void CheckForStarvation(TInstant now) override
    {
        YUNREACHABLE();
    }

    virtual int GetMaxRunningOperationCount() const override
    {
        return StrategyConfig_->MaxRunningOperationCount;
    }

    virtual int GetMaxOperationCount() const override
    {
        return StrategyConfig_->MaxOperationCount;
    }

    virtual ISchedulerElementPtr Clone() override
    {
        return New<TRootElement>(*this);
    }

    TRootElementPtr CloneRoot()
    {
        return New<TRootElement>(*this);
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
        , NonPreemptiveProfilingCounters("/non_preemptive")
        , PreemptiveProfilingCounters("/preemptive")
    {
        Host->SubscribeOperationRegistered(BIND(&TFairShareStrategy::OnOperationRegistered, this));
        Host->SubscribeOperationUnregistered(BIND(&TFairShareStrategy::OnOperationUnregistered, this));

        Host->SubscribeJobFinished(BIND(&TFairShareStrategy::OnJobFinished, this));
        Host->SubscribeJobUpdated(BIND(&TFairShareStrategy::OnJobUpdated, this));
        Host->SubscribePoolsUpdated(BIND(&TFairShareStrategy::OnPoolsUpdated, this));

        Host->SubscribeOperationRuntimeParamsUpdated(
            BIND(&TFairShareStrategy::OnOperationRuntimeParamsUpdated, this));

        RootElement = New<TRootElement>(Host, config);

        FairShareUpdateExecutor_ = New<TPeriodicExecutor>(
            GetCurrentInvoker(),
            BIND(&TFairShareStrategy::OnFairShareUpdate, this),
            Config->FairShareUpdatePeriod);

        FairShareLoggingExecutor_ = New<TPeriodicExecutor>(
            GetCurrentInvoker(),
            BIND(&TFairShareStrategy::OnFairShareLogging, this),
            Config->FairShareLogPeriod);
    }

    virtual void ScheduleJobs(const ISchedulingContextPtr& schedulingContext) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto guard = WaitFor(TAsyncLockReaderGuard::Acquire(&ScheduleJobsLock))
            .Value();

        TFairShareContext context(schedulingContext, RootElementSnapshot->GetTreeSize());

        for (const auto& job : schedulingContext->RunningJobs()) {
            context.JobToOperationElement[job] = FindOperationElement(job->GetOperationId());
        }

        DoScheduleJobs(context, RootElementSnapshot);
    }

    virtual void StartPeriodicActivity() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        FairShareLoggingExecutor_->Start();
        FairShareUpdateExecutor_->Start();
    }

    virtual void ResetState() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        FairShareLoggingExecutor_->Stop();
        FairShareUpdateExecutor_->Stop();

        LastPoolsNodeUpdate.Reset();
    }

    virtual TError CanAddOperation(TOperationPtr operation) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

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
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& element = GetOperationElement(operationId);
        auto serializedParams = ConvertToAttributes(element->GetRuntimeParams());
        BuildYsonMapFluently(consumer)
            .Items(*serializedParams);
    }

    virtual void BuildOperationProgress(const TOperationId& operationId, IYsonConsumer* consumer) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& element = FindOperationElement(operationId);
        if (!element) {
            return;
        }

        auto dynamicAttributes = GetGlobalDynamicAttributes(element);

        auto* parent = element->GetParent();
        BuildYsonMapFluently(consumer)
            .Item("pool").Value(parent->GetId())
            .Item("start_time").Value(dynamicAttributes.MinSubtreeStartTime)
            .Item("preemptable_job_count").Value(element->GetPreemptableJobCount())
            .Do(BIND(&TFairShareStrategy::BuildElementYson, this, element));
    }

    virtual void BuildBriefOperationProgress(const TOperationId& operationId, IYsonConsumer* consumer) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& element = FindOperationElement(operationId);
        if (!element) {
            return;
        }

        auto* parent = element->GetParent();
        const auto& attributes = element->Attributes();
        BuildYsonMapFluently(consumer)
            .Item("pool").Value(parent->GetId())
            .Item("fair_share_ratio").Value(attributes.FairShareRatio);
    }

    virtual void BuildOrchid(IYsonConsumer* consumer) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        BuildPoolsInformation(consumer);
    }

    virtual Stroka GetOperationLoggingProgress(const TOperationId& operationId) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& element = GetOperationElement(operationId);
        const auto& attributes = element->Attributes();
        auto dynamicAttributes = GetGlobalDynamicAttributes(element);

        return Format(
            "Scheduling = {Status: %v, DominantResource: %v, Demand: %.4lf, "
            "Usage: %.4lf, FairShare: %.4lf, Satisfaction: %.4lg, AdjustedMinShare: %.4lf, "
            "MaxPossibleUsage: %.4lf,  BestAllocation: %.4lf, "
            "Starving: %v, Weight: %v, "
            "PreemptableRunningJobs: %v}",
            element->GetStatus(),
            attributes.DominantResource,
            attributes.DemandRatio,
            element->GetResourceUsageRatio(),
            attributes.FairShareRatio,
            dynamicAttributes.SatisfactionRatio,
            attributes.AdjustedMinShareRatio,
            attributes.MaxPossibleUsageRatio,
            attributes.BestAllocationRatio,
            element->GetStarving(),
            element->GetWeight(),
            element->GetPreemptableJobCount());
    }

    virtual void BuildBriefSpec(const TOperationId& operationId, IYsonConsumer* consumer) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& element = GetOperationElement(operationId);
        BuildYsonMapFluently(consumer)
            .Item("pool").Value(element->GetParent()->GetId());
    }

    // NB: This function is public for testing purposes.
    virtual void OnFairShareUpdateAt(TInstant now) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Run periodic update.
        PROFILE_TIMING ("/fair_share_update_time") {
            // The root element gets the whole cluster.
            RootElement->Update(GlobalDynamicAttributes_);
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

        RootElementSnapshot = RootElement->CloneRoot();
    }

    // NB: This function is public for testing purposes.
    virtual void OnFairShareLoggingAt(TInstant now) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

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

    TRootElementPtr RootElement;
    TRootElementPtr RootElementSnapshot;

    TAsyncReaderWriterLock ScheduleJobsLock;

    TDynamicAttributesList GlobalDynamicAttributes_;

    struct TProfilingCounters
    {
        TProfilingCounters(const Stroka& prefix)
            : PrescheduleJobTimeCounter(prefix + "/preschedule_job_time")
            , TotalControllerScheduleJobTimeCounter(prefix + "/controller_schedule_job_time/total")
            , ExecControllerScheduleJobTimeCounter(prefix + "/controller_schedule_job_time/exec")
            , StrategyScheduleJobTimeCounter(prefix + "/strategy_schedule_job_time")
            , ScheduleJobCallCounter(prefix + "/schedule_job_count")
        {
            for (auto reason : TEnumTraits<EScheduleJobFailReason>::GetDomainValues())
            {
                ControllerScheduleJobFailCounter[reason] = NProfiling::TSimpleCounter(
                    prefix + "/controller_schedule_job_fail",
                    GetFailReasonProfilingTags(reason));
            }
        }

        NProfiling::TAggregateCounter PrescheduleJobTimeCounter;
        NProfiling::TAggregateCounter TotalControllerScheduleJobTimeCounter;
        NProfiling::TAggregateCounter ExecControllerScheduleJobTimeCounter;
        NProfiling::TAggregateCounter StrategyScheduleJobTimeCounter;
        NProfiling::TAggregateCounter ScheduleJobCallCounter;

        TEnumIndexedVector<NProfiling::TSimpleCounter, EScheduleJobFailReason> ControllerScheduleJobFailCounter;
    };

    TProfilingCounters NonPreemptiveProfilingCounters;
    TProfilingCounters PreemptiveProfilingCounters;

    TPeriodicExecutorPtr FairShareUpdateExecutor_;
    TPeriodicExecutorPtr FairShareLoggingExecutor_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    TDynamicAttributes GetGlobalDynamicAttributes(const ISchedulerElementPtr& element) const
    {
        int index = element->GetTreeIndex();
        if (index == UNASSIGNED_TREE_INDEX) {
            return TDynamicAttributes();
        } else {
            return GlobalDynamicAttributes_[index];
        }
    }

    void OnFairShareUpdate()
    {
        OnFairShareUpdateAt(TInstant::Now());
    }

    void OnFairShareLogging()
    {
        OnFairShareLoggingAt(TInstant::Now());
    }

    void DoScheduleJobs(TFairShareContext& context, TRootElementPtr rootElement)
    {
        const auto& schedulingContext = context.SchedulingContext;

        auto profileTimings = [&] (
            TProfilingCounters& counters,
            int scheduleJobCount,
            TDuration scheduleJobDurationWithoutControllers)
        {
            Profiler.Update(
                counters.StrategyScheduleJobTimeCounter,
                scheduleJobDurationWithoutControllers.MicroSeconds());

            Profiler.Update(
                counters.TotalControllerScheduleJobTimeCounter,
                context.TotalScheduleJobDuration.MicroSeconds());

            Profiler.Update(
                counters.ExecControllerScheduleJobTimeCounter,
                context.ExecScheduleJobDuration.MicroSeconds());

            Profiler.Update(counters.ScheduleJobCallCounter, scheduleJobCount);

            for (auto reason : TEnumTraits<EScheduleJobFailReason>::GetDomainValues()) {
                Profiler.Update(
                    counters.ControllerScheduleJobFailCounter[reason],
                    context.FailedScheduleJob[reason]);
            }
        };

        // First-chance scheduling.
        int nonPreemptiveScheduleJobCount = 0;
        {
            LOG_DEBUG("Scheduling new jobs");
            PROFILE_AGGREGATED_TIMING(NonPreemptiveProfilingCounters.PrescheduleJobTimeCounter) {
                rootElement->PrescheduleJob(context, false);
            }

            NProfiling::TScopedTimer timer;
            while (schedulingContext->CanStartMoreJobs()) {
                ++nonPreemptiveScheduleJobCount;
                if (!rootElement->ScheduleJob(context)) {
                    break;
                }
            }
            profileTimings(
                NonPreemptiveProfilingCounters,
                nonPreemptiveScheduleJobCount,
                timer.GetElapsed() - context.TotalScheduleJobDuration);
        }

        // Compute discount to node usage.
        LOG_DEBUG("Looking for preemptable jobs");
        // TODO(acid): Put raw pointers here.
        yhash_set<TCompositeSchedulerElementPtr> discountedPools;
        std::vector<TJobPtr> preemptableJobs;
        PROFILE_TIMING ("/analyze_preemptable_jobs_time") {
            for (const auto& job : schedulingContext->RunningJobs()) {
                const auto& operationElement = context.JobToOperationElement.at(job);
                if (!operationElement || !operationElement->IsJobExisting(job->GetId())) {
                    LOG_DEBUG("Dangling running job found (JobId: %v, OperationId: %v)",
                        job->GetId(),
                        job->GetOperationId());
                    continue;
                }

                if (IsJobPreemptable(job, operationElement) && !operationElement->HasStarvingParent()) {
                    auto* parent = operationElement->GetParent();
                    while (parent) {
                        discountedPools.insert(parent);
                        context.DynamicAttributes(parent).ResourceUsageDiscount += job->ResourceUsage();
                        parent = parent->GetParent();
                    }
                    schedulingContext->ResourceUsageDiscount() += job->ResourceUsage();
                    preemptableJobs.push_back(job);
                    LOG_DEBUG("Job is preemptable (JobId: %v)",
                        job->GetId());
                }
            }
        }

        auto resourceDiscount = schedulingContext->ResourceUsageDiscount();
        int startedBeforePreemption = schedulingContext->StartedJobs().size();

        // Second-chance scheduling.
        // NB: Schedule at most one job.
        int preemptiveScheduleJobCount = 0;
        {
            LOG_DEBUG("Scheduling new jobs with preemption");
            PROFILE_AGGREGATED_TIMING(PreemptiveProfilingCounters.PrescheduleJobTimeCounter) {
                rootElement->PrescheduleJob(context, true);
            }

            // Clean data from previous profiling.
            context.TotalScheduleJobDuration = TDuration::Zero();
            context.ExecScheduleJobDuration = TDuration::Zero();
            std::fill(context.FailedScheduleJob.begin(), context.FailedScheduleJob.end(), 0);

            NProfiling::TScopedTimer timer;
            while (schedulingContext->CanStartMoreJobs()) {
                ++preemptiveScheduleJobCount;
                if (!rootElement->ScheduleJob(context)) {
                    break;
                }
                if (schedulingContext->StartedJobs().size() > startedBeforePreemption) {
                    break;
                }
            }
            profileTimings(
                PreemptiveProfilingCounters,
                preemptiveScheduleJobCount,
                timer.GetElapsed() - context.TotalScheduleJobDuration);
        }

        int startedAfterPreemption = schedulingContext->StartedJobs().size();
        int scheduledDuringPreemption = startedAfterPreemption - startedBeforePreemption;

        // Reset discounts.
        schedulingContext->ResourceUsageDiscount() = ZeroJobResources();
        for (const auto& pool : discountedPools) {
            context.DynamicAttributes(pool.Get()).ResourceUsageDiscount = ZeroJobResources();
        }

        // Preempt jobs if needed.
        std::sort(
            preemptableJobs.begin(),
            preemptableJobs.end(),
            [] (const TJobPtr& lhs, const TJobPtr& rhs) {
                return lhs->GetStartTime() > rhs->GetStartTime();
            });

        auto poolLimitsViolated = [&] (const TJobPtr& job) -> bool {
            const auto& operationElement = context.JobToOperationElement.at(job);
            if (!operationElement) {
                return false;
            }

            auto* parent = operationElement->GetParent();
            while (parent) {
                if (!Dominates(parent->ResourceLimits(), parent->GetResourceUsage())) {
                    return true;
                }
                parent = parent->GetParent();
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
            const auto& operationElement = context.JobToOperationElement.at(job);
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
                PreemptJob(job, operationElement, context);
            }
        }

        LOG_DEBUG("Heartbeat info (StartedJobs: %v, PreemptedJobs: %v, "
            "JobsScheduledDuringPreemption: %v, PreemptableJobs: %v, PreemptableResources: %v, "
            "NonPreemptiveScheduleJobCount: %v, PreemptiveScheduleJobCount: %v)",
            schedulingContext->StartedJobs().size(),
            schedulingContext->PreemptedJobs().size(),
            scheduledDuringPreemption,
            preemptableJobs.size(),
            FormatResources(resourceDiscount),
            nonPreemptiveScheduleJobCount,
            preemptiveScheduleJobCount);
    }

    bool IsJobPreemptable(const TJobPtr& job, const TOperationElementPtr& element)
    {
        double usageRatio = element->GetResourceUsageRatio();
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

    void PreemptJob(
        const TJobPtr& job,
        const TOperationElementPtr& operationElement,
        TFairShareContext& context)
    {
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

    bool CanAddOperationToPool(TCompositeSchedulerElement* pool)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        while (pool) {
            if (pool->RunningOperationCount() >= pool->GetMaxRunningOperationCount()) {
                return false;
            }
            pool = pool->GetParent();
        }
        return true;
    }

    void OnOperationRegistered(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

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
        pool->IncreaseResourceUsage(operationElement->GetResourceUsage());
        operationElement->SetParent(pool.Get());

        if (CanAddOperationToPool(pool.Get())) {
            ActivateOperation(operation->GetId());
        } else {
            OperationQueue.push_back(operation);
        }
    }

    TCompositeSchedulerElementPtr FindPoolWithViolatedOperationCountLimit(TCompositeSchedulerElement* element)
    {
        while (element) {
            if (element->OperationCount() >= element->GetMaxOperationCount()) {
                return element;
            }
            element = element->GetParent();
        }
        return nullptr;
    }

    void IncreaseOperationCount(TCompositeSchedulerElement* element, int delta)
    {
        while (element) {
            element->OperationCount() += delta;
            element = element->GetParent();
        }
    }

    void IncreaseRunningOperationCount(TCompositeSchedulerElement* element, int delta)
    {
        while (element) {
            element->RunningOperationCount() += delta;
            element = element->GetParent();
        }
    }

    void ActivateOperation(const TOperationId& operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& operationElement = GetOperationElement(operationId);
        auto* parent = operationElement->GetParent();
        parent->EnableChild(operationElement);
        IncreaseRunningOperationCount(parent, 1);

        Host->ActivateOperation(operationId);

        LOG_INFO("Operation added to pool (OperationId: %v, Pool: %v)",
            operationId,
            parent->GetId());
    }

    void OnOperationUnregistered(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operationElement = GetOperationElement(operation->GetId());
        auto* pool = static_cast<TPool*>(operationElement->GetParent());

        YCHECK(OperationToElement.erase(operation->GetId()) == 1);
        operationElement->SetAlive(false);
        pool->RemoveChild(operationElement);
        pool->IncreaseResourceUsage(-operationElement->GetResourceUsage());
        IncreaseOperationCount(pool, -1);

        LOG_INFO("Operation removed from pool (OperationId: %v, Pool: %v)",
            operation->GetId(),
            pool->GetId());

        bool isPending = false;
        for (auto it = OperationQueue.begin(); it != OperationQueue.end(); ++it) {
            if (*it == operationElement->GetOperation()) {
                isPending = true;
                OperationQueue.erase(it);
                break;
            }
        }

        if (!isPending) {
            IncreaseRunningOperationCount(pool, -1);

            // Try to run operations from queue.
            auto it = OperationQueue.begin();
            while (it != OperationQueue.end() && RootElement->RunningOperationCount() < Config->MaxRunningOperationCount) {
                const auto& operation = *it;
                auto* operationPool = GetOperationElement(operation->GetId())->GetParent();
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
        VERIFY_THREAD_AFFINITY(ControlThread);

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
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& element = GetOperationElement(job->GetOperationId());
        element->OnJobFinished(job->GetId());
    }

    void OnJobUpdated(const TJobPtr& job, const TJobResources& resourcesDelta)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& element = GetOperationElement(job->GetOperationId());
        element->IncreaseJobResourceUsage(job->GetId(), resourcesDelta);
    }

    void RegisterPool(const TPoolPtr& pool)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(Pools.insert(std::make_pair(pool->GetId(), pool)).second);
        LOG_INFO("Pool registered (Pool: %v)", pool->GetId());
    }

    void RegisterPool(const TPoolPtr& pool, const TCompositeSchedulerElementPtr& parent)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(Pools.insert(std::make_pair(pool->GetId(), pool)).second);
        pool->SetParent(parent.Get());
        parent->AddChild(pool);

        LOG_INFO("Pool registered (Pool: %v, Parent: %v)",
            pool->GetId(),
            parent->GetId());
    }

    void UnregisterPool(const TPoolPtr& pool)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(Pools.erase(pool->GetId()) == 1);
        pool->SetAlive(false);
        auto parent = pool->GetParent();
        SetPoolParent(pool, nullptr);

        LOG_INFO("Pool unregistered (Pool: %v, Parent: %v)",
            pool->GetId(),
            parent->GetId());
    }

    void SetPoolParent(const TPoolPtr& pool, const TCompositeSchedulerElementPtr& parent)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (pool->GetParent() == parent)
            return;

        auto* oldParent = pool->GetParent();
        if (oldParent) {
            oldParent->IncreaseResourceUsage(-pool->GetResourceUsage());
            IncreaseRunningOperationCount(oldParent, -pool->RunningOperationCount());
            oldParent->RemoveChild(pool);
        }

        pool->SetParent(parent.Get());
        if (parent) {
            parent->AddChild(pool);
            parent->IncreaseResourceUsage(pool->GetResourceUsage());
            IncreaseRunningOperationCount(parent.Get(), pool->RunningOperationCount());

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
        VERIFY_THREAD_AFFINITY(ControlThread);

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
        VERIFY_THREAD_AFFINITY(ControlThread);

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
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (LastPoolsNodeUpdate && NYTree::AreNodesEqual(LastPoolsNodeUpdate, poolsNode)) {
            LOG_INFO("Pools are not changed, skipping update");
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

            RootElement->Update(GlobalDynamicAttributes_);
            RootElementSnapshot = RootElement->CloneRoot();

            LOG_INFO("Pools updated");
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error updating pools");
        }
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
                        .Item("running_operation_count").Value(pool->RunningOperationCount())
                        .Item("operation_count").Value(pool->OperationCount())
                        .Item("max_running_operation_count").Value(pool->GetMaxRunningOperationCount())
                        .Item("max_operation_count").Value(pool->GetMaxOperationCount())
                        .DoIf(config->Mode == ESchedulingMode::Fifo, [&] (TFluentMap fluent) {
                            fluent
                                .Item("fifo_sort_parameters").Value(config->FifoSortParameters);
                        })
                        .DoIf(pool->GetParent(), [&] (TFluentMap fluent) {
                            fluent
                                .Item("parent").Value(pool->GetParent()->GetId());
                        })
                        .Do(BIND(&TFairShareStrategy::BuildElementYson, this, pool))
                    .EndMap();
            });
    }

    void BuildElementYson(const ISchedulerElementPtr& element, IYsonConsumer* consumer)
    {
        const auto& attributes = element->Attributes();
        auto dynamicAttributes = GetGlobalDynamicAttributes(element);

        BuildYsonMapFluently(consumer)
            .Item("scheduling_status").Value(element->GetStatus())
            .Item("starving").Value(element->GetStarving())
            .Item("fair_share_starvation_tolerance").Value(element->GetFairShareStarvationTolerance())
            .Item("min_share_preemption_timeout").Value(element->GetMinSharePreemptionTimeout())
            .Item("fair_share_preemption_timeout").Value(element->GetFairSharePreemptionTimeout())
            .Item("adjusted_fair_share_starvation_tolerance").Value(attributes.AdjustedFairShareStarvationTolerance)
            .Item("adjusted_min_share_preemption_timeout").Value(attributes.AdjustedMinSharePreemptionTimeout)
            .Item("adjusted_fair_share_preemption_timeout").Value(attributes.AdjustedFairSharePreemptionTimeout)
            .Item("resource_demand").Value(element->ResourceDemand())
            .Item("resource_usage").Value(element->GetResourceUsage())
            .Item("resource_limits").Value(element->ResourceLimits())
            .Item("dominant_resource").Value(attributes.DominantResource)
            .Item("weight").Value(element->GetWeight())
            .Item("min_share_ratio").Value(element->GetMinShareRatio())
            .Item("max_share_ratio").Value(element->GetMaxShareRatio())
            .Item("adjusted_min_share_ratio").Value(attributes.AdjustedMinShareRatio)
            .Item("max_possible_usage_ratio").Value(attributes.MaxPossibleUsageRatio)
            .Item("usage_ratio").Value(element->GetResourceUsageRatio())
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

