#include "stdafx.h"
#include "fair_share_strategy.h"
#include "scheduler_strategy.h"
#include "master_connector.h"
#include "job_resources.h"

#include <core/ytree/yson_serializable.h>
#include <core/ytree/ypath_proxy.h>
#include <core/ytree/fluent.h>

#include <ytlib/scheduler/helpers.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/node_tracker_client/helpers.h>

#include <core/logging/log.h>
#include <core/logging/tagged_logger.h>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NYson;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;

////////////////////////////////////////////////////////////////////

static auto& Logger = SchedulerLogger;
static auto& Profiler = SchedulerProfiler;

static const double RatioComputationPrecision = 1e-12;
static const double RatioComparisonPrecision = 1e-6;

////////////////////////////////////////////////////////////////////

struct ISchedulerElement;
typedef TIntrusivePtr<ISchedulerElement> ISchedulerElementPtr;

struct ISchedulableElement;
typedef TIntrusivePtr<ISchedulableElement> ISchedulableElementPtr;

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
        : Rank(0)
        , DominantResource(EResourceType::Cpu)
        , DemandRatio(0.0)
        , FairShareRatio(0.0)
        , AdjustedMinShareRatio(0.0)
        , MaxShareRatio(1.0)
    { }

    int Rank;
    EResourceType DominantResource;
    double DemandRatio;
    double FairShareRatio;
    double AdjustedMinShareRatio;
    double MaxShareRatio;
};

////////////////////////////////////////////////////////////////////

struct ISchedulerElement
    : public virtual TRefCounted
{
    virtual void Update() = 0;

    virtual bool ScheduleJobs(
        ISchedulingContext* context,
        bool starvingOnly) = 0;

    virtual const TSchedulableAttributes& Attributes() const = 0;
    virtual TSchedulableAttributes& Attributes() = 0;
};

////////////////////////////////////////////////////////////////////

struct ISchedulableElement
    : public virtual ISchedulerElement
{
    virtual TInstant GetStartTime() const = 0;

    virtual double GetWeight() const = 0;
    virtual double GetMinShareRatio() const = 0;

    virtual TNodeResources GetDemand() const = 0;

    virtual const TNodeResources& ResourceUsage() const = 0;
    virtual const TNodeResources& ResourceUsageDiscount() const = 0;
    virtual const TNodeResources& ResourceLimits() const = 0;

    virtual double GetUsageRatio() const = 0;
    virtual double GetDemandRatio() const = 0;
};

////////////////////////////////////////////////////////////////////

class TSchedulableElementBase
    : public ISchedulableElement
{
public:
    explicit TSchedulableElementBase(ISchedulerStrategyHost* host)
        : Host(host)
    { }

    virtual double GetUsageRatio() const override
    {
        const auto& attributes = Attributes();

        auto demand = GetDemand();
        auto usage = ResourceUsage() - ResourceUsageDiscount();
        auto limits = GetAdjustedResourceLimits(
            demand,
            Host->GetTotalResourceLimits(),
            Host->GetExecNodeCount());

        i64 dominantUsage = GetResource(usage, attributes.DominantResource);
        i64 dominantLimit = GetResource(limits, attributes.DominantResource);

        return dominantLimit == 0 ? 1.0 : (double) dominantUsage / dominantLimit;
    }

    virtual double GetDemandRatio() const override
    {
        const auto& attributes = Attributes();

        auto demand = GetDemand();
        auto limits = GetAdjustedResourceLimits(
            demand,
            Host->GetTotalResourceLimits(),
            Host->GetExecNodeCount());

        i64 dominantDemand = GetResource(demand, attributes.DominantResource);
        i64 dominantLimit = GetResource(limits, attributes.DominantResource);
        return dominantLimit == 0 ? 1.0 : (double) dominantDemand / dominantLimit;
    }

private:
    ISchedulerStrategyHost* Host;

};

////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EOperationStatus,
    (Normal)
    (BelowMinShare)
    (BelowFairShare)
);

class TOperationElement
    : public TSchedulableElementBase
{
public:
    explicit TOperationElement(
        TFairShareStrategyConfigPtr config,
        ISchedulerStrategyHost* host,
        TOperationPtr operation)
        : TSchedulableElementBase(host)
        , Operation_(operation)
        , Pool_(nullptr)
        , Starving_(false)
        , ResourceUsage_(ZeroNodeResources())
        , ResourceUsageDiscount_(ZeroNodeResources())
        , Config(config)
    { }


    virtual bool ScheduleJobs(
        ISchedulingContext* context,
        bool starvingOnly) override;

    virtual void Update() override
    { }

    virtual TInstant GetStartTime() const override
    {
        return Operation_->GetStartTime();
    }

    virtual double GetWeight() const override
    {
        return
            TInstant::Now() < GetStartTime() + Config->NewOperationWeightBoostPeriod
            ? Spec_->Weight * Config->NewOperationWeightBoostFactor
            : Spec_->Weight;
    }

    virtual double GetMinShareRatio() const override
    {
        return Spec_->MinShareRatio;
    }

    virtual TNodeResources GetDemand() const override
    {
        if (Operation_->GetSuspended()) {
            return ZeroNodeResources();
        }
        auto controller = Operation_->GetController();
        return ResourceUsage_ + controller->GetNeededResources();
    }

    virtual const TNodeResources& ResourceLimits() const
    {
        return InfiniteNodeResources();
    }


    EOperationStatus GetStatus() const
    {
        if (Operation_->GetState() != EOperationState::Running) {
            return EOperationStatus::Normal;
        }

        auto controller = Operation_->GetController();
        if (controller->GetPendingJobCount() == 0) {
            return EOperationStatus::Normal;
        }

        double usageRatio = GetUsageRatio();
        double demandRatio = GetDemandRatio();

        double tolerance =
            demandRatio < Attributes_.FairShareRatio + RatioComparisonPrecision
            ? 1.0
            : Spec_->FairShareStarvationTolerance.Get(Config->FairShareStarvationTolerance);

        if (usageRatio > Attributes_.FairShareRatio * tolerance - RatioComparisonPrecision) {
            return EOperationStatus::Normal;
        }

        return usageRatio < Attributes_.AdjustedMinShareRatio
               ? EOperationStatus::BelowMinShare
               : EOperationStatus::BelowFairShare;
    }


    DEFINE_BYREF_RW_PROPERTY(TSchedulableAttributes, Attributes);
    DEFINE_BYVAL_RO_PROPERTY(TOperationPtr, Operation);
    DEFINE_BYVAL_RW_PROPERTY(TPooledOperationSpecPtr, Spec);
    DEFINE_BYVAL_RW_PROPERTY(TPool*, Pool);
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, BelowMinShareSince);
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, BelowFairShareSince);
    DEFINE_BYVAL_RW_PROPERTY(bool, Starving);
    DEFINE_BYREF_RW_PROPERTY(TNodeResources, ResourceUsage);
    DEFINE_BYREF_RW_PROPERTY(TNodeResources, ResourceUsageDiscount);

private:
    TFairShareStrategyConfigPtr Config;

};

////////////////////////////////////////////////////////////////////

class TCompositeSchedulerElement
    : public virtual ISchedulerElement
{
public:
    explicit TCompositeSchedulerElement(ISchedulerStrategyHost* host)
        : Host(host)
        , Mode(ESchedulingMode::Fifo)
    { }

    virtual void Update() override
    {
        ComputeAll();
    }

    virtual bool ScheduleJobs(
        ISchedulingContext* context,
        bool starvingOnly) override
    {
        bool result = false;
        auto node = context->GetNode();
        auto sortedChildren = GetSortedChildren();
        FOREACH (auto child, sortedChildren) {
            if (!node->HasSpareResources()) {
                break;
            }
            if (child->ScheduleJobs(context, starvingOnly)) {
                result = true;
                // Allow at most one job in starvingOnly mode.
                if (starvingOnly) {
                    break;
                }
            }
        }
        return result;
    }

    DEFINE_BYREF_RW_PROPERTY(TSchedulableAttributes, Attributes);


    void AddChild(ISchedulableElementPtr child)
    {
        YCHECK(Children.insert(child).second);
    }

    void RemoveChild(ISchedulableElementPtr child)
    {
        YCHECK(Children.erase(child) == 1);
        // Avoid scheduling removed children.
        ComputeAll();
    }

    std::vector<ISchedulableElementPtr> GetChildren() const
    {
        return std::vector<ISchedulableElementPtr>(Children.begin(), Children.end());
    }

    bool IsEmpty() const
    {
        return Children.empty();
    }

protected:
    ISchedulerStrategyHost* Host;

    ESchedulingMode Mode;

    yhash_set<ISchedulableElementPtr> Children;


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
            FOREACH (auto child, Children) {
                sum += getter(fitFactor, child);
            }
            return sum;
        };

        // Run binary search to compute fit factor.
        double fitFactor = BinarySearch(getSum, sum);

        // Compute actual min shares from fit factor.
        FOREACH (auto child, Children) {
            double value = getter(fitFactor, child);
            setter(child, value);
        }
    }


    void ComputeAll()
    {
        // Choose dominant resource types.
        // Compute max share ratios.
        // Compute demand ratios and their sum.
        double demandRatioSum = 0.0;
        FOREACH (auto child, Children) {
            auto& childAttributes = child->Attributes();

            auto demand = child->GetDemand();
            auto totalLimits = GetAdjustedResourceLimits(
                demand,
                Host->GetTotalResourceLimits(),
                Host->GetExecNodeCount());
            auto limits = GetAdjustedResourceLimits(
                demand,
                Min(Host->GetTotalResourceLimits(), child->ResourceLimits()),
                Host->GetExecNodeCount());
            
            childAttributes.MaxShareRatio = GetMinResourceRatio(limits, totalLimits);
            childAttributes.DominantResource = GetDominantResource(demand, totalLimits);

            i64 dominantTotalLimits = GetResource(totalLimits, childAttributes.DominantResource);
            i64 dominantDemand = GetResource(demand, childAttributes.DominantResource);
            childAttributes.DemandRatio = dominantTotalLimits == 0 ? 0.0 : (double) dominantDemand / dominantTotalLimits;
            demandRatioSum += std::min(childAttributes.DemandRatio, childAttributes.MaxShareRatio);
        }

        switch (Mode) {
            case ESchedulingMode::Fifo:
                // Easy case -- the first child get everything, others get none.
                ComputeFifo();
                break;

            case ESchedulingMode::FairShare:
                // Hard case -- compute fair shares using fit factor.
                ComputeFairShare();
                break;

            default:
                YUNREACHABLE();
        }

        // Propagate updates to children.
        FOREACH (auto child, Children) {
            child->Update();
        }
    }

    void ComputeFifo()
    {
        FOREACH (auto child, Children) {
            auto& childAttributes = child->Attributes();
            if (childAttributes.Rank == 0) {
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

    void ComputeFairShare()
    {
        ComputeByFitting(
            [&] (double fitFactor, ISchedulableElementPtr child) -> double {
                const auto& childAttributes = child->Attributes();
                double result = child->GetMinShareRatio() * fitFactor;
                // Never give more than max share allows.
                result = std::min(result, childAttributes.MaxShareRatio);
                // Never give more than demanded.
                result = std::min(result, childAttributes.DemandRatio);
                return result;
            },
            [&] (ISchedulableElementPtr child, double value) {
                auto& attributes = child->Attributes();
                attributes.AdjustedMinShareRatio = value;
            },
            Attributes_.AdjustedMinShareRatio);

        ComputeByFitting(
            [&] (double fitFactor, ISchedulableElementPtr child) -> double {
                const auto& childAttributes = child->Attributes();
                double result = child->GetWeight() * fitFactor;
                // Never give less than promised by min share.
                result = std::max(result, childAttributes.AdjustedMinShareRatio);
                // Never give more than demanded.
                result = std::min(result, childAttributes.DemandRatio);
                // Never give more than max share allows.
                result = std::min(result, childAttributes.MaxShareRatio);
                return result;
            },
            [&] (ISchedulableElementPtr child, double value) {
                auto& attributes = child->Attributes();
                attributes.FairShareRatio = value;
            },
            Attributes_.FairShareRatio);
    }


    const std::vector<ISchedulableElementPtr> GetSortedChildren()
    {
        PROFILE_TIMING ("/fair_share_sort_time") {
            std::vector<ISchedulableElementPtr> sortedChildren;
            FOREACH (auto child, Children) {
                sortedChildren.push_back(child);
            }

            switch (Mode) {
                case ESchedulingMode::Fifo:
                    SortChildrenFifo(&sortedChildren);
                    break;
                case ESchedulingMode::FairShare:
                    SortChildrenFairShare(&sortedChildren);
                    break;
                default:
                    YUNREACHABLE();
            }

            // Update ranks.
            for (int rank = 0; rank < static_cast<int>(sortedChildren.size()); ++rank) {
                sortedChildren[rank]->Attributes().Rank = rank;
            }

            return sortedChildren;
        }
    }

    void SortChildrenFifo(std::vector<ISchedulableElementPtr>* sortedChildren)
    {
        // Sort by weight (desc), then by start time (asc).
        std::sort(
            sortedChildren->begin(),
            sortedChildren->end(),
            [] (const ISchedulableElementPtr& lhs, const ISchedulableElementPtr& rhs) -> bool {
                if (lhs->GetWeight() > rhs->GetWeight()) {
                    return true;
                }
                if (lhs->GetWeight() < rhs->GetWeight()) {
                    return false;
                }
                return lhs->GetStartTime() < rhs->GetStartTime();
            });
    }

    void SortChildrenFairShare(std::vector<ISchedulableElementPtr>* sortedChildren)
    {
        std::sort(
            sortedChildren->begin(),
            sortedChildren->end(),
            [&] (const ISchedulableElementPtr& lhs, const ISchedulableElementPtr& rhs) -> bool {
                bool lhsNeedy = IsNeedy(lhs);
                bool rhsNeedy = IsNeedy(rhs);

                if (lhsNeedy && !rhsNeedy) {
                    return true;
                }

                if (!lhsNeedy && rhsNeedy) {
                    return false;
                }

                if (lhsNeedy && rhsNeedy) {
                    return GetUsageToMinShareRatio(lhs) < GetUsageToMinShareRatio(rhs);
                }

                return GetUsageToWeightRatio(lhs) < GetUsageToWeightRatio(rhs);
            });
    }


    bool IsNeedy(ISchedulableElementPtr element) const
    {
        double usageRatio = element->GetUsageRatio();
        double minShareRatio = element->Attributes().AdjustedMinShareRatio;
        return minShareRatio > 1e-3 && usageRatio < minShareRatio;
    }

    double GetUsageToMinShareRatio(ISchedulableElementPtr element) const
    {
        double usageRatio = element->GetUsageRatio();
        double minShareRatio = element->Attributes().AdjustedMinShareRatio;
        return usageRatio / minShareRatio;
    }

    static double GetUsageToWeightRatio(ISchedulableElementPtr element)
    {
        double usageRatio = element->GetUsageRatio();
        double weight = element->GetWeight();
        return usageRatio / weight;
    }


    void SetMode(ESchedulingMode mode)
    {
        if (Mode != mode) {
            Mode = mode;
            ComputeAll();
        }
    }

};

////////////////////////////////////////////////////////////////////

class TPool
    : public TCompositeSchedulerElement
    , public TSchedulableElementBase
{
public:
    TPool(
        ISchedulerStrategyHost* host,
        const Stroka& id)
        : TCompositeSchedulerElement(host)
        , TSchedulableElementBase(host)
        , ResourceUsage_(ZeroNodeResources())
        , ResourceUsageDiscount_(ZeroNodeResources())
        , ResourceLimits_(InfiniteNodeResources())
        , Id(id)
    {
        SetDefaultConfig();
    }


    const Stroka& GetId() const
    {
        return Id;
    }

    bool IsDefaultConfigured() const
    {
        return DefaultConfigured;
    }


    TPoolConfigPtr GetConfig()
    {
        return Config;
    }

    void SetConfig(TPoolConfigPtr newConfig)
    {
        Config = newConfig;
        SetMode(Config->Mode);
        DefaultConfigured = false;

        ResourceLimits_ = InfiniteNodeResources();
        if (Config->ResourceLimits->UserSlots) {
            ResourceLimits_.set_user_slots(*Config->ResourceLimits->UserSlots);
        }
        if (Config->ResourceLimits->Cpu) {
            ResourceLimits_.set_cpu(*Config->ResourceLimits->Cpu);
        }
        if (Config->ResourceLimits->Memory) {
            ResourceLimits_.set_memory(*Config->ResourceLimits->Memory);
        }
    }

    void SetDefaultConfig()
    {
        SetConfig(New<TPoolConfig>());
        DefaultConfigured = true;
    }


    virtual TInstant GetStartTime() const override
    {
        // Makes no sense for pools since the root is in fair-share mode.
        return TInstant();
    }

    virtual double GetWeight() const override
    {
        return Config->Weight;
    }

    virtual double GetMinShareRatio() const override
    {
        return Config->MinShareRatio;
    }

    virtual TNodeResources GetDemand() const override
    {
        auto result = ZeroNodeResources();
        FOREACH (auto child, Children) {
            result += child->GetDemand();
        }
        return result;
    }


    DEFINE_BYREF_RW_PROPERTY(TNodeResources, ResourceUsage);
    DEFINE_BYREF_RW_PROPERTY(TNodeResources, ResourceUsageDiscount);
    DEFINE_BYREF_RO_PROPERTY(TNodeResources, ResourceLimits);

private:
    Stroka Id;

    TPoolConfigPtr Config;
    bool DefaultConfigured;

};

////////////////////////////////////////////////////////////////////

class TRootElement
    : public TCompositeSchedulerElement
{
public:
    explicit TRootElement(ISchedulerStrategyHost* host)
        : TCompositeSchedulerElement(host)
    {
        SetMode(ESchedulingMode::FairShare);
        Attributes_.FairShareRatio = 1.0;
        Attributes_.AdjustedMinShareRatio = 1.0;
    }

};

////////////////////////////////////////////////////////////////////

class TFairShareStrategy
    : public ISchedulerStrategy
{
public:
    explicit TFairShareStrategy(
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

        auto* masterConnector = Host->GetMasterConnector();
        masterConnector->AddGlobalWatcherRequester(BIND(&TFairShareStrategy::RequestPools, this));
        masterConnector->AddGlobalWatcherHandler(BIND(&TFairShareStrategy::HandlePools, this));

        RootElement = New<TRootElement>(Host);
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
        FOREACH (const auto& pair, OperationToElement) {
            CheckForStarvation(pair.second);
        }

        // First-chance scheduling.
        LOG_DEBUG("Scheduling new jobs");
        RootElement->ScheduleJobs(context, false);

        // Compute discount to node usage.
        LOG_DEBUG("Looking for preemptable jobs");
        yhash_set<TOperationElementPtr> discountedOperations;
        yhash_set<TPoolPtr> discountedPools;
        std::vector<TJobPtr> preemptableJobs;
        FOREACH (auto job, context->RunningJobs()) {
            auto operation = job->GetOperation();
            auto operationElement = GetOperationElement(operation);
            operationElement->ResourceUsageDiscount() += job->ResourceUsage();
            discountedOperations.insert(operationElement);
            if (IsJobPreemptable(job)) {
                auto* pool = operationElement->GetPool();
                discountedPools.insert(pool);
                node->ResourceUsageDiscount() += job->ResourceUsage();
                pool->ResourceUsageDiscount() += job->ResourceUsage();
                preemptableJobs.push_back(job);
                LOG_DEBUG("Job is preemptable (JobId: %s)",
                    ~ToString(job->GetId()));
            }
        }

        // Second-chance scheduling.
        LOG_DEBUG("Scheduling new jobs with preemption");
        bool needsPreemption = RootElement->ScheduleJobs(context, true);

        // Reset discounts.
        node->ResourceUsageDiscount() = ZeroNodeResources();
        FOREACH (auto operationElement, discountedOperations) {
            operationElement->ResourceUsageDiscount() = ZeroNodeResources();
        }
        FOREACH (auto pool, discountedPools) {
            pool->ResourceUsageDiscount() = ZeroNodeResources();
        }

        // Preempt jobs if needed.
        if (!needsPreemption)
            return;

        std::sort(
            preemptableJobs.begin(),
            preemptableJobs.end(),
            [] (const TJobPtr& lhs, const TJobPtr& rhs) {
                return lhs->GetStartTime() > rhs->GetStartTime();
            });

        auto checkPoolLimits = [&] (TJobPtr job) -> bool {
            auto operation = job->GetOperation();
            auto operationElement = GetOperationElement(operation);
            auto* pool = operationElement->GetPool();
            return Dominates(pool->ResourceLimits(), pool->ResourceUsage());
        };

        auto checkAllLimits = [&] () -> bool {
            if (!Dominates(node->ResourceLimits(), node->ResourceUsage())) {
                return false;
            }

            FOREACH (auto job, context->StartedJobs()) {
                if (!checkPoolLimits(job)) {
                    return false;
                }
            }

            return true;
        };

        FOREACH (auto job, preemptableJobs) {
            if (checkAllLimits())
                break;

            context->PreemptJob(job);
        }
    }


    virtual void BuildOperationProgressYson(TOperationPtr operation, IYsonConsumer* consumer) override
    {
        auto element = GetOperationElement(operation);
        auto pool = element->GetPool();
        BuildYsonMapFluently(consumer)
            .Item("pool").Value(pool->GetId())
            .Item("start_time").Value(element->GetStartTime())
            .Item("scheduling_status").Value(element->GetStatus())
            .Item("starving").Value(element->GetStarving())
            .Item("usage_ratio").Value(element->GetUsageRatio())
            .Do(BIND(&TFairShareStrategy::BuildElementYson, pool, element));
    }

    virtual Stroka GetOperationLoggingProgress(TOperationPtr operation) override
    {
        auto element = GetOperationElement(operation);
        const auto& attributes = element->Attributes();
        return Sprintf(
            "Scheduling = {Status: %s, Rank: %d+%d, DominantResource: %s, Demand: %.4lf, "
            "Usage: %.4lf, FairShare: %.4lf, AdjustedMinShare: %.4lf, MaxShare: %.4lf, Starving: %s, Weight: %lf}",
            ~element->GetStatus().ToString(),
            element->GetPool()->Attributes().Rank,
            attributes.Rank,
            ~attributes.DominantResource.ToString(),
            attributes.DemandRatio,
            element->GetUsageRatio(),
            attributes.FairShareRatio,
            attributes.AdjustedMinShareRatio,
            attributes.MaxShareRatio,
            ~FormatBool(element->GetStarving()),
            element->GetWeight());
    }

    virtual void BuildOrchidYson(IYsonConsumer* consumer) override
    {
        BuildYsonMapFluently(consumer)
            .Item("pools").DoMapFor(Pools, [&] (TFluentMap fluent, const TPoolMap::value_type& pair) {
                const auto& id = pair.first;
                auto pool = pair.second;
                auto config = pool->GetConfig();
                fluent
                    .Item(id).BeginMap()
                        .Item("mode").Value(config->Mode)
                        .Do(BIND(&TFairShareStrategy::BuildElementYson, RootElement, pool))
                    .EndMap();
            });
    }

private:
    TFairShareStrategyConfigPtr Config;
    ISchedulerStrategyHost* Host;

    typedef yhash_map<Stroka, TPoolPtr> TPoolMap;
    TPoolMap Pools;

    yhash_map<TOperationPtr, TOperationElementPtr> OperationToElement;

    typedef std::list<TJobPtr> TJobList;
    TJobList JobList;
    yhash_map<TJobPtr, TJobList::iterator> JobToIterator;

    TRootElementPtr RootElement;
    TNullable<TInstant> LastUpdateTime;


    bool IsJobPreemptable(TJobPtr job)
    {
        auto operation = job->GetOperation();
        if (operation->GetState() != EOperationState::Running) {
            return false;
        }

        auto element = GetOperationElement(operation);
        auto spec = element->GetSpec();

        double usageRatio = element->GetUsageRatio();
        if (usageRatio < Config->MinPreemptableRatio) {
            return false;
        }

        const auto& attributes = element->Attributes();
        if (usageRatio < attributes.FairShareRatio) {
            return false;
        }

        return true;
    }


    TPooledOperationSpecPtr ParseSpec(TOperationPtr operation, INodePtr specNode)
    {
        try {
            return ConvertTo<TPooledOperationSpecPtr>(specNode);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error parsing spec of pooled operation %s, defaults will be used",
                ~ToString(operation->GetOperationId()));
            return New<TPooledOperationSpec>();
        }
    }


    void OnOperationRegistered(TOperationPtr operation)
    {
        auto operationElement = New<TOperationElement>(Config, Host, operation);
        YCHECK(OperationToElement.insert(std::make_pair(operation, operationElement)).second);

        auto spec = ParseSpec(operation, operation->GetSpec());

        auto poolId = spec->Pool ? *spec->Pool : operation->GetAuthenticatedUser();
        auto pool = FindPool(poolId);
        if (!pool) {
            pool = New<TPool>(Host, poolId);
            RegisterPool(pool);
        }

        operationElement->SetSpec(spec);

        operationElement->SetPool(~pool);
        pool->AddChild(operationElement);
        pool->ResourceUsage() += operationElement->ResourceUsage();

        Host->GetMasterConnector()->AddOperationWatcherRequester(
            operation,
            BIND(&TFairShareStrategy::RequestOperationSpec, this, operation));
        Host->GetMasterConnector()->AddOperationWatcherHandler(
            operation,
            BIND(&TFairShareStrategy::HandleOperationSpec, this, operation));

        LOG_INFO("Operation added to pool (OperationId: %s, Pool: %s)",
            ~ToString(operation->GetOperationId()),
            ~pool->GetId());
    }

    void OnOperationUnregistered(TOperationPtr operation)
    {
        auto operationElement = GetOperationElement(operation);
        auto pool = operationElement->GetPool();

        YCHECK(OperationToElement.erase(operation) == 1);
        pool->RemoveChild(operationElement);
        pool->ResourceUsage() -= operationElement->ResourceUsage();

        LOG_INFO("Operation removed from pool (OperationId: %s, Pool: %s)",
            ~ToString(operation->GetOperationId()),
            ~pool->GetId());

        if (pool->IsEmpty() && pool->IsDefaultConfigured()) {
            UnregisterPool(pool);
        }
    }


    void RequestOperationSpec(
        TOperationPtr operation,
        TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        auto operationPath = GetOperationPath(operation->GetOperationId());
        auto req = TYPathProxy::Get(operationPath + "/@spec");
        batchReq->AddRequest(req, "get_spec");
    }

    void HandleOperationSpec(
        TOperationPtr operation,
        TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto element = FindOperationElement(operation);
        if (!element)
            return;

        NLog::TTaggedLogger Logger(SchedulerLogger);
        Logger.AddTag(Sprintf("OperationId: %s", ~ToString(operation->GetOperationId())));

        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_spec");
        if (!rsp->IsOK()) {
            LOG_ERROR(*rsp, "Error updating operation spec");
            return;
        }

        try {
            if (!ReconfigureYsonSerializable(element->GetSpec(), TYsonString(rsp->value())))
                return;
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error parsing updated operation spec");
        }

        LOG_INFO("Operation spec updated");
    }


    void OnJobStarted(TJobPtr job)
    {
        auto it = JobList.insert(JobList.begin(), job);
        YCHECK(JobToIterator.insert(std::make_pair(job, it)).second);

        UpdateResourceUsage(job, job->ResourceUsage());
    }

    void OnJobFinished(TJobPtr job)
    {
        auto it = JobToIterator.find(job);
        YASSERT(it != JobToIterator.end());
        JobList.erase(it->second);
        JobToIterator.erase(it);

        UpdateResourceUsage(job, -job->ResourceUsage());
    }

    void OnJobUpdated(TJobPtr job, const TNodeResources& resourcesDelta)
    {
        UpdateResourceUsage(job, resourcesDelta);
    }


    void RegisterPool(TPoolPtr pool)
    {
        YCHECK(Pools.insert(std::make_pair(pool->GetId(), pool)).second);
        RootElement->AddChild(pool);

        LOG_INFO("Pool registered (Pool: %s)", ~pool->GetId());
    }

    void UnregisterPool(TPoolPtr pool)
    {
        YCHECK(pool->IsEmpty());

        YCHECK(Pools.erase(pool->GetId()) == 1);
        RootElement->RemoveChild(pool);

        LOG_INFO("Pool unregistered (Pool: %s)", ~pool->GetId());
    }

    TPoolPtr FindPool(const Stroka& id)
    {
        auto it = Pools.find(id);
        return it == Pools.end() ? NULL : it->second;
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
        return it == OperationToElement.end() ? NULL : it->second;
    }

    TOperationElementPtr GetOperationElement(TOperationPtr operation)
    {
        auto element = FindOperationElement(operation);
        YCHECK(element);
        return element;
    }


    void RequestPools(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        LOG_INFO("Updating pools");

        auto req = TYPathProxy::Get("//sys/pools");
        static auto poolConfigTemplate = New<TPoolConfig>();
        static auto poolConfigKeys = poolConfigTemplate->GetRegisteredKeys();
        TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly, poolConfigKeys);
        ToProto(req->mutable_attribute_filter(), attributeFilter);
        batchReq->AddRequest(req, "get_pools");
    }

    void HandlePools(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_pools");
        if (!rsp->IsOK()) {
            LOG_ERROR(*rsp, "Error updating pools");
            return;
        }

        // Build the set of potential orphans.
        yhash_set<Stroka> orphanPoolIds;
        FOREACH (const auto& pair, Pools) {
            YCHECK(orphanPoolIds.insert(pair.first).second);
        }

        auto newPoolsNode = ConvertToNode(TYsonString(rsp->value()));
        auto newPoolsMapNode = newPoolsNode->AsMap();
        FOREACH (const auto& pair, newPoolsMapNode->GetChildren()) {
            const auto& id = pair.first;
            const auto& poolNode = pair.second;

            // Parse config.
            auto configNode = ConvertToNode(poolNode->Attributes());
            TPoolConfigPtr config;
            try {
                config = ConvertTo<TPoolConfigPtr>(configNode);
            } catch (const std::exception& ex) {
                LOG_ERROR(ex, "Error parsing configuration of pool %s, defaults will be used",
                    ~id.Quote());
                config = New<TPoolConfig>();
            }

            auto existingPool = FindPool(id);
            if (existingPool) {
                // Reconfigure existing pool.
                existingPool->SetConfig(config);
                YCHECK(orphanPoolIds.erase(id) == 1);
            } else {
                // Create new pool.
                auto newPool = New<TPool>(Host, id);
                newPool->SetConfig(config);
                RegisterPool(newPool);
            }
        }

        // Unregister orphan pools.
        FOREACH (const auto& id, orphanPoolIds) {
            auto pool = GetPool(id);
            if (pool->IsEmpty()) {
                UnregisterPool(pool);
            } else {
                pool->SetDefaultConfig();
            }
        }

        LOG_INFO("Pools updated");
    }


    void CheckForStarvation(TOperationElementPtr element)
    {
        auto status = element->GetStatus();
        auto now = TInstant::Now();
        auto spec = element->GetSpec();
        auto minSharePreemptionTimeout = spec->MinSharePreemptionTimeout.Get(Config->MinSharePreemptionTimeout);
        auto fairSharePreemptionTimeout = spec->FairSharePreemptionTimeout.Get(Config->FairSharePreemptionTimeout);
        switch (status) {
            case EOperationStatus::BelowMinShare:
                if (!element->GetBelowMinShareSince()) {
                    element->SetBelowMinShareSince(now);
                } else if (element->GetBelowMinShareSince().Get() < now - minSharePreemptionTimeout) {
                    SetStarving(element, status);
                }
                break;

            case EOperationStatus::BelowFairShare:
                if (!element->GetBelowFairShareSince()) {
                    element->SetBelowFairShareSince(now);
                } else if (element->GetBelowFairShareSince().Get() < now - fairSharePreemptionTimeout) {
                    SetStarving(element, status);
                }
                element->SetBelowMinShareSince(Null);
                break;

            case EOperationStatus::Normal:
                element->SetBelowMinShareSince(Null);
                element->SetBelowFairShareSince(Null);
                ResetStarving(element);
                break;

            default:
                YUNREACHABLE();
        }
    }

    void SetStarving(TOperationElementPtr element, EOperationStatus status)
    {
        if (!element->GetStarving()) {
            element->SetStarving(true);
            LOG_INFO("Operation starvation timeout (OperationId: %s, Status: %s)",
                ~ToString(element->GetOperation()->GetOperationId()),
                ~status.ToString());
        }
    }

    void ResetStarving(TOperationElementPtr element)
    {
        if (element->GetStarving()) {
            element->SetStarving(false);
            LOG_INFO("Operation is no longer starving (OperationId: %s)",
                ~ToString(element->GetOperation()->GetOperationId()));
        }
    }


    void UpdateResourceUsage(TJobPtr job, const TNodeResources& resourcesDelta)
    {
        auto operationElement = GetOperationElement(job->GetOperation());
        operationElement->ResourceUsage() += resourcesDelta;
        operationElement->GetPool()->ResourceUsage() += resourcesDelta;
    }


    static void BuildElementYson(
        TCompositeSchedulerElementPtr composite,
        ISchedulableElementPtr element,
        IYsonConsumer* consumer)
    {
        const auto& attributes = element->Attributes();
        BuildYsonMapFluently(consumer)
            .Item("scheduling_rank").Value(attributes.Rank)
            .Item("resource_demand").Value(element->GetDemand())
            .Item("resource_usage").Value(element->ResourceUsage())
            .Item("dominant_resource").Value(attributes.DominantResource)
            .Item("weight").Value(element->GetWeight())
            .Item("min_share_ratio").Value(element->GetMinShareRatio())
            .Item("adjusted_min_share_ratio").Value(attributes.AdjustedMinShareRatio)
            .Item("max_share_ratio").Value(attributes.MaxShareRatio)
            .Item("demand_ratio").Value(attributes.DemandRatio)
            .Item("fair_share_ratio").Value(attributes.FairShareRatio);
    }

};

bool TOperationElement::ScheduleJobs(
    ISchedulingContext* context,
    bool starvingOnly)
{
    if (starvingOnly && !Starving_) {
        return false;
    }

    auto node = context->GetNode();
    auto controller = Operation_->GetController();

    bool result =  false;
    while (Operation_->GetState() == EOperationState::Running && context->CanStartMoreJobs()) {
        auto poolLimits = Pool_->ResourceLimits() - Pool_->ResourceUsage() + Pool_->ResourceUsageDiscount();
        auto nodeLimits = node->ResourceLimits() - node->ResourceUsage() + node->ResourceUsageDiscount();
        auto jobLimits = Min(poolLimits, nodeLimits);
        auto job = controller->ScheduleJob(context, jobLimits);
        if (!job) {
            // The first failure means that no more jobs can be scheduled.
            break;
        }

        result = true;

        // Allow at most one job in starvingOnly mode.
        if (starvingOnly) {
            break;
        }
    }

    return result;
}

std::unique_ptr<ISchedulerStrategy> CreateFairShareStrategy(
    TFairShareStrategyConfigPtr config,
    ISchedulerStrategyHost* host)
{
    return std::unique_ptr<ISchedulerStrategy>(new TFairShareStrategy(config, host));
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

