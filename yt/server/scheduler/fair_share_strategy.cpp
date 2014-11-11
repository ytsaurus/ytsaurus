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

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NYson;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;

////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;
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
        : DominantResource(EResourceType::Cpu)
        , DemandRatio(0.0)
        , FairShareRatio(0.0)
        , AdjustedMinShareRatio(0.0)
        , MaxShareRatio(1.0)
        , SatisfactionRatio(0.0)
        , Active(true)
    { }

    EResourceType DominantResource;
    double DemandRatio;
    double FairShareRatio;
    double AdjustedMinShareRatio;
    double MaxShareRatio;
    double SatisfactionRatio;
    bool Active;
};

////////////////////////////////////////////////////////////////////

struct ISchedulerElement
    : public virtual TRefCounted
{
    virtual void Update() = 0;

    virtual void BeginHeartbeat() = 0;
    virtual void PrescheduleJob(TExecNodePtr node, bool starvingOnly) = 0;
    virtual bool ScheduleJob(ISchedulingContext* context, bool starvingOnly) = 0;
    virtual void EndHeartbeat() = 0;

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
    virtual double GetMaxShareRatio() const = 0;

    virtual TNodeResources GetDemand() const = 0;

    virtual const TNodeResources& ResourceUsage() const = 0;
    virtual const TNodeResources& ResourceUsageDiscount() const = 0;
    virtual const TNodeResources& ResourceLimits() const = 0;

    virtual double GetUsageRatio() const = 0;
    virtual double GetDemandRatio() const = 0;
};

////////////////////////////////////////////////////////////////////

class TSchedulerElementBase
    : public virtual ISchedulerElement
{
public:
    virtual void BeginHeartbeat() override
    {
        Attributes_.Active = true;
    }

    virtual void PrescheduleJob(TExecNodePtr /*node*/, bool /*starvingOnly*/) override
    {
        Attributes_.SatisfactionRatio = ComputeLocalSatisfactionRatio();
    }

    virtual TNullable<Stroka> GetSchedulingTag() const
    {
        return Null;
    }

    DEFINE_BYREF_RW_PROPERTY(TSchedulableAttributes, Attributes);

protected:
    ISchedulerStrategyHost* Host;


    explicit TSchedulerElementBase(ISchedulerStrategyHost* host)
        : Host(host)
    { }

    virtual double ComputeLocalSatisfactionRatio() const
    {
        return 1.0;
    }


};

////////////////////////////////////////////////////////////////////

class TSchedulableElementBase
    : public virtual TSchedulerElementBase
    , public virtual ISchedulableElement
{
public:
    explicit TSchedulableElementBase(ISchedulerStrategyHost* host)
        : TSchedulerElementBase(host)
    { }

    virtual void Update() override
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

 protected:
    virtual double ComputeLocalSatisfactionRatio() const override
    {
        double minShareRatio = Attributes_.AdjustedMinShareRatio;
        double fairShareRatio = Attributes_.FairShareRatio;
        double usageRatio = GetUsageRatio();

        // Check for corner cases.
        if (fairShareRatio < RatioComparisonPrecision) {
            return 1.0;
        }

        if (minShareRatio > RatioComparisonPrecision && usageRatio < minShareRatio) {
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
    : public virtual TSchedulerElementBase
{
public:
    explicit TCompositeSchedulerElement(ISchedulerStrategyHost* host)
        : TSchedulerElementBase(host)
        , Mode(ESchedulingMode::Fifo)
    { }

    virtual void Update() override
    {
        ComputeAll();
    }

    virtual void BeginHeartbeat() override
    {
        TSchedulerElementBase::BeginHeartbeat();
        for (const auto& child : Children) {
            child->BeginHeartbeat();
        }
    }

    virtual void PrescheduleJob(TExecNodePtr node, bool starvingOnly)
    {
        // Compute local satisfaction ratio.
        TSchedulerElementBase::PrescheduleJob(node, starvingOnly);

        if (!Attributes_.Active)
            return;

        // Adjust satisfaction ratio using children.
        // Declare the element passive if all children are passive.
        Attributes_.Active = false;
        if (!node->CanSchedule(GetSchedulingTag())) {
            return;
        }

        for (const auto& child : GetActiveChildren()) {
            child->PrescheduleJob(node, starvingOnly);
            if (child->Attributes().Active) {
                Attributes_.SatisfactionRatio = std::min(
                    Attributes_.SatisfactionRatio,
                    child->Attributes().SatisfactionRatio);
                Attributes_.Active = true;
            }
        }
    }

    virtual bool ScheduleJob(ISchedulingContext* context, bool starvingOnly) override
    {
        ISchedulableElementPtr bestChild;
        for (const auto& child : GetActiveChildren()) {
            if (!bestChild ||
                child->Attributes().SatisfactionRatio < bestChild->Attributes().SatisfactionRatio)
            {
                bestChild = child;
            }
        }

        if (!bestChild) {
            return false;
        }

        // NB: Ignore the child's result.
        bestChild->ScheduleJob(context, starvingOnly);

        return true;
    }

    virtual void EndHeartbeat() override
    { }


    void AddChild(ISchedulableElementPtr child)
    {
        YCHECK(Children.insert(child).second);
    }

    void RemoveChild(ISchedulableElementPtr child)
    {
        YCHECK(Children.erase(child) == 1);
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


    void ComputeAll()
    {
        // Choose dominant resource types.
        // Compute max share ratios.
        // Compute demand ratios.
        for (const auto& child : Children) {
            auto& childAttributes = child->Attributes();

            auto demand = child->GetDemand();

            auto totalLimits = GetAdjustedResourceLimits(
                demand,
                Host->GetTotalResourceLimits(),
                Host->GetExecNodeCount());
            auto limits = Min(Host->GetTotalResourceLimits(), child->ResourceLimits());

            childAttributes.MaxShareRatio = std::min(
                GetMinResourceRatio(limits, totalLimits),
                child->GetMaxShareRatio());

            childAttributes.DominantResource = GetDominantResource(demand, totalLimits);

            i64 dominantTotalLimits = GetResource(totalLimits, childAttributes.DominantResource);
            i64 dominantDemand = GetResource(demand, childAttributes.DominantResource);
            childAttributes.DemandRatio = dominantTotalLimits == 0 ? 0.0 : (double) dominantDemand / dominantTotalLimits;
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
        for (const auto& child : Children) {
            child->Update();
        }
    }

    void ComputeFifo()
    {
        auto bestChild = GetBestFifoChild(false);
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

    void ComputeFairShare()
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
            childAttributes.AdjustedMinShareRatio = result;
            minShareSum += result;

            if (child->GetWeight() > RatioComparisonPrecision) {
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
            [&] (double fitFactor, const ISchedulableElementPtr& child) -> double {
                const auto& childAttributes = child->Attributes();
                double result = fitFactor * child->GetWeight() / minWeight;
                // Never give less than promised by min share.
                result = std::max(result, childAttributes.AdjustedMinShareRatio);
                // Never give more than demanded.
                result = std::min(result, childAttributes.DemandRatio);
                // Never give more than max share allows.
                result = std::min(result, childAttributes.MaxShareRatio);
                return result;
            },
            [&] (const ISchedulableElementPtr& child, double value) {
                auto& attributes = child->Attributes();
                attributes.FairShareRatio = value;
            },
            Attributes_.FairShareRatio);
    }


    std::vector<ISchedulableElementPtr> GetActiveChildren()
    {
        switch (Mode) {
            case ESchedulingMode::Fifo:
                return GetActiveChildrenFifo();
            case ESchedulingMode::FairShare:
                return GetActiveChildrenFairShare();
            default:
                YUNREACHABLE();
        }
    }

    std::vector<ISchedulableElementPtr> GetActiveChildrenFairShare()
    {
        std::vector<ISchedulableElementPtr> result;
        result.reserve(Children.size());
        for (const auto& child : Children) {
            if (child->Attributes().Active) {
                result.push_back(child);
            }
        }
        return result;
    }

    std::vector<ISchedulableElementPtr> GetActiveChildrenFifo()
    {
        auto bestChild = GetBestFifoChild(true);
        return bestChild
            ? std::vector<ISchedulableElementPtr>(1, bestChild)
            : std::vector<ISchedulableElementPtr>();
    }

    ISchedulableElementPtr GetBestFifoChild(bool needsActive)
    {
        auto isBetter = [] (const ISchedulableElementPtr& lhs, const ISchedulableElementPtr& rhs) -> bool {
            if (lhs->GetWeight() > rhs->GetWeight()) {
                return true;
            }
            if (lhs->GetWeight() < rhs->GetWeight()) {
                return false;
            }
            return lhs->GetStartTime() < rhs->GetStartTime();
        };

        ISchedulableElementPtr bestChild;
        for (const auto& child : Children) {
            if (needsActive && !child->Attributes().Active)
                continue;

            if (bestChild && isBetter(bestChild, child))
                continue;

            bestChild = child;
        }

        return bestChild;
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
        : TSchedulerElementBase(host)
        , TCompositeSchedulerElement(host)
        , TSchedulableElementBase(host)
        , Parent_(nullptr)
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

    virtual double GetMaxShareRatio() const override
    {
        return Config->MaxShareRatio;
    }

    virtual TNullable<Stroka> GetSchedulingTag() const override
    {
        return Config->SchedulingTag;
    }

    virtual TNodeResources GetDemand() const override
    {
        auto result = ZeroNodeResources();
        for (const auto& child : Children) {
            result += child->GetDemand();
        }
        return result;
    }

    virtual void Update() override
    {
        TCompositeSchedulerElement::Update();
        ComputeResourceLimits();
    }


    DEFINE_BYVAL_RW_PROPERTY(TPool*, Parent);

    DEFINE_BYREF_RW_PROPERTY(TNodeResources, ResourceUsage);
    DEFINE_BYREF_RW_PROPERTY(TNodeResources, ResourceUsageDiscount);
    DEFINE_BYREF_RO_PROPERTY(TNodeResources, ResourceLimits);

private:
    Stroka Id;

    TPoolConfigPtr Config;
    bool DefaultConfigured;


    void DoSetConfig(TPoolConfigPtr newConfig)
    {
        Config = newConfig;
        SetMode(Config->Mode);
        ComputeResourceLimits();
    }

    void ComputeResourceLimits()
    {
        auto combinedLimits = Host->GetResourceLimits(GetSchedulingTag()) * Config->MaxShareRatio;
        auto perTypeLimits = InfiniteNodeResources();
        if (Config->ResourceLimits->UserSlots) {
            perTypeLimits.set_user_slots(*Config->ResourceLimits->UserSlots);
        }
        if (Config->ResourceLimits->Cpu) {
            perTypeLimits.set_cpu(*Config->ResourceLimits->Cpu);
        }
        if (Config->ResourceLimits->Memory) {
            perTypeLimits.set_memory(*Config->ResourceLimits->Memory);
        }

        ResourceLimits_ = Min(combinedLimits, perTypeLimits);
    }

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
        TStrategyOperationSpecPtr spec,
        TOperationRuntimeParamsPtr runtimeParams,
        ISchedulerStrategyHost* host,
        TOperationPtr operation)
        : TSchedulerElementBase(host)
        , TSchedulableElementBase(host)
        , Operation_(operation)
        , Spec_(spec)
        , RuntimeParams_(runtimeParams)
        , Pool_(nullptr)
        , Starving_(false)
        , ResourceUsage_(ZeroNodeResources())
        , ResourceUsageDiscount_(ZeroNodeResources())
        , NonpreemptableResourceUsage_(ZeroNodeResources())
        , Config(config)
    { }


    virtual void PrescheduleJob(TExecNodePtr node, bool starvingOnly) override
    {
        TSchedulableElementBase::PrescheduleJob(node, starvingOnly);

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

    virtual bool ScheduleJob(ISchedulingContext* context, bool starvingOnly) override
    {
        if (starvingOnly && !Starving_) {
            return false;
        }

        auto node = context->GetNode();
        auto controller = Operation_->GetController();

        // Compute job limits from node limits and pool limits.
        auto jobLimits = node->ResourceLimits() - node->ResourceUsage() + node->ResourceUsageDiscount();
        auto* pool = Pool_;
        while (pool) {
            auto poolLimits = pool->ResourceLimits() - pool->ResourceUsage() + pool->ResourceUsageDiscount();
            jobLimits = Min(jobLimits, poolLimits);
            pool = pool->GetParent();
        }

        auto job = controller->ScheduleJob(context, jobLimits);
        if (job) {
            return true;
        } else {
            Attributes_.Active = false;
            return false;
        }
    }

    virtual void EndHeartbeat() override
    { }

    virtual TInstant GetStartTime() const override
    {
        return Operation_->GetStartTime();
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
        ResourceLimits_ = Host->GetResourceLimits(GetSchedulingTag());
        return ResourceLimits_;
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


    DEFINE_BYVAL_RO_PROPERTY(TOperationPtr, Operation);
    DEFINE_BYVAL_RO_PROPERTY(TStrategyOperationSpecPtr, Spec);
    DEFINE_BYVAL_RO_PROPERTY(TOperationRuntimeParamsPtr, RuntimeParams);
    DEFINE_BYVAL_RW_PROPERTY(TPool*, Pool);
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, BelowMinShareSince);
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, BelowFairShareSince);
    DEFINE_BYVAL_RW_PROPERTY(bool, Starving);
    DEFINE_BYREF_RW_PROPERTY(TNodeResources, ResourceUsage);
    DEFINE_BYREF_RW_PROPERTY(TNodeResources, ResourceUsageDiscount);

    DEFINE_BYREF_RW_PROPERTY(TNodeResources, NonpreemptableResourceUsage);
    DEFINE_BYREF_RW_PROPERTY(TJobList, NonpreemptableJobs);
    DEFINE_BYREF_RW_PROPERTY(TJobList, PreemptableJobs);

private:
    mutable TNodeResources ResourceLimits_;

    TFairShareStrategyConfigPtr Config;
};

////////////////////////////////////////////////////////////////////

class TRootElement
    : public TCompositeSchedulerElement
{
public:
    explicit TRootElement(ISchedulerStrategyHost* host)
        : TSchedulerElementBase(host)
        , TCompositeSchedulerElement(host)
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
        Host->SubscribePoolsUpdated(BIND(&TFairShareStrategy::OnPoolsUpdated, this));

        Host->SubscribeOperationRuntimeParamsUpdated(
            BIND(&TFairShareStrategy::OnOperationRuntimeParamsUpdated, this));

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

        // Run periodic logging.
        if (!LastLogTime || now > LastLogTime.Get() + Config->FairShareLogPeriod) {
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
            LastLogTime = now;
        }

        // Update starvation flags for all operations.
        for (const auto& pair : OperationToElement) {
            CheckForStarvation(pair.second);
        }

        RootElement->BeginHeartbeat();

        // First-chance scheduling.
        LOG_DEBUG("Scheduling new jobs");
        while (context->CanStartMoreJobs()) {
            RootElement->PrescheduleJob(context->GetNode(), false);
            if (!RootElement->ScheduleJob(context, false)) {
                break;
            }
        }

        // Compute discount to node usage.
        LOG_DEBUG("Looking for preemptable jobs");
        yhash_set<TOperationElementPtr> discountedOperations;
        yhash_set<TPoolPtr> discountedPools;
        std::vector<TJobPtr> preemptableJobs;
        for (const auto& job : context->RunningJobs()) {
            auto operation = job->GetOperation();
            auto operationElement = GetOperationElement(operation);
            operationElement->ResourceUsageDiscount() += job->ResourceUsage();
            discountedOperations.insert(operationElement);
            if (IsJobPreemptable(job)) {
                auto* pool = operationElement->GetPool();
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

        // Second-chance scheduling.
        // NB: Schedule at most one job.
        LOG_DEBUG("Scheduling new jobs with preemption");
        if (context->CanStartMoreJobs()) {
            RootElement->PrescheduleJob(context->GetNode(), true);
            RootElement->ScheduleJob(context, true);
        }

        // Reset discounts.
        node->ResourceUsageDiscount() = ZeroNodeResources();
        for (const auto& operationElement : discountedOperations) {
            operationElement->ResourceUsageDiscount() = ZeroNodeResources();
        }
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

        auto checkPoolLimits = [&] (TJobPtr job) -> bool {
            auto operation = job->GetOperation();
            auto operationElement = GetOperationElement(operation);
            auto* pool = operationElement->GetPool();
            while (pool) {
                if (!Dominates(pool->ResourceLimits(), pool->ResourceUsage())) {
                    return false;
                }
                pool = pool->GetParent();
            }
            return true;
        };

        auto checkAllLimits = [&] () -> bool {
            if (!Dominates(node->ResourceLimits(), node->ResourceUsage())) {
                return false;
            }

            for (const auto& job : context->StartedJobs()) {
                if (!checkPoolLimits(job)) {
                    return false;
                }
            }

            return true;
        };

        for (const auto& job : preemptableJobs) {
            if (checkAllLimits())
                break;

            context->PreemptJob(job);
        }

        RootElement->EndHeartbeat();
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
            .Item("scheduling_status").Value(element->GetStatus())
            .Item("starving").Value(element->GetStarving())
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
            "Usage: %.4lf, FairShare: %.4lf, Satisfaction: %.4lf, AdjustedMinShare: %.4lf, MaxShare: %.4lf, "
            "Starving: %lv, Weight: %v, "
            "PreemptableRunningJobs: %v}",
            element->GetStatus(),
            attributes.DominantResource,
            attributes.DemandRatio,
            element->GetUsageRatio(),
            attributes.FairShareRatio,
            attributes.SatisfactionRatio,
            attributes.AdjustedMinShareRatio,
            attributes.MaxShareRatio,
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

        double usageRatio = element->GetUsageRatio();
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


    void OnOperationRegistered(TOperationPtr operation)
    {
        auto spec = ParseSpec(operation, operation->GetSpec());
        auto params = BuildInitialRuntimeParams(spec);

        auto poolId = spec->Pool ? *spec->Pool : operation->GetAuthenticatedUser();
        auto pool = FindPool(poolId);
        if (!pool) {
            pool = New<TPool>(Host, poolId);
            RegisterPool(pool);
        }

        auto operationElement = New<TOperationElement>(
            Config,
            spec,
            params,
            Host,
            operation);
        YCHECK(OperationToElement.insert(std::make_pair(operation, operationElement)).second);

        operationElement->SetPool(pool.Get());
        pool->AddChild(operationElement);
        IncreasePoolUsage(pool, operationElement->ResourceUsage());

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
        IncreasePoolUsage(pool, -operationElement->ResourceUsage());

        LOG_INFO("Operation removed from pool (OperationId: %v, Pool: %v)",
            operation->GetId(),
            pool->GetId());

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

        NLog::TLogger Logger(SchedulerLogger);
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


    TCompositeSchedulerElementPtr GetPoolParentElement(TPoolPtr pool)
    {
        auto* parentPool = pool->GetParent();
        return parentPool ? TCompositeSchedulerElementPtr(parentPool) : RootElement;
    }

    // Handles nullptr (aka "root") properly.
    Stroka GetPoolId(TPoolPtr pool)
    {
        return pool ? pool->GetId() : Stroka("<Root>");
    }


    void RegisterPool(TPoolPtr pool)
    {
        YCHECK(Pools.insert(std::make_pair(pool->GetId(), pool)).second);
        GetPoolParentElement(pool)->AddChild(pool);

        LOG_INFO("Pool registered (Pool: %v, Parent: %v)",
            GetPoolId(pool),
            GetPoolId(pool->GetParent()));
    }

    void UnregisterPool(TPoolPtr pool)
    {
        YCHECK(Pools.erase(pool->GetId()) == 1);
        SetPoolParent(pool, nullptr);
        GetPoolParentElement(pool)->RemoveChild(pool);

        LOG_INFO("Pool unregistered (Pool: %v, Parent: %v)",
            GetPoolId(pool),
            GetPoolId(pool->GetParent()));
    }

    void SetPoolParent(TPoolPtr pool, TPoolPtr parent)
    {
        if (pool->GetParent() == parent)
            return;

        auto* oldParent = pool->GetParent();
        if (oldParent) {
            IncreasePoolUsage(oldParent, -pool->ResourceUsage());
        }
        GetPoolParentElement(pool)->RemoveChild(pool);

        pool->SetParent(parent.Get());

        GetPoolParentElement(pool)->AddChild(pool);
        if (parent) {
            IncreasePoolUsage(parent, pool->ResourceUsage());
        }
    }

    void IncreasePoolUsage(TPoolPtr pool, const TNodeResources& delta)
    {
        auto* currentPool = pool.Get();
        while (currentPool) {
            currentPool->ResourceUsage() += delta;
            currentPool = currentPool->GetParent();
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
            std::function<void(INodePtr, TPoolPtr)> parseConfig =
                [&] (INodePtr configNode, TPoolPtr parent) {
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
                            pool = New<TPool>(Host, childId);
                            pool->SetConfig(config);
                            RegisterPool(pool);
                        }
                        SetPoolParent(pool, parent);

                        // Parse children.
                        parseConfig(childNode, pool.Get());
                    }
                };

            // Run recursive descent parsing.
            parseConfig(poolsNode, nullptr);

            // Unregister orphan pools.
            for (const auto& id : orphanPoolIds) {
                auto pool = GetPool(id);
                if (pool->IsEmpty()) {
                    UnregisterPool(pool);
                } else {
                    pool->SetDefaultConfig();
                    SetPoolParent(pool, nullptr);
                }
            }

            RootElement->Update();

            LOG_INFO("Pools updated");
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error updating pools");
        }
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
            LOG_INFO("Operation starvation timeout (OperationId: %v, Status: %v)",
                element->GetOperation()->GetId(),
                status);
        }
    }

    void ResetStarving(TOperationElementPtr element)
    {
        if (element->GetStarving()) {
            element->SetStarving(false);
            LOG_INFO("Operation is no longer starving (OperationId: %v)",
                element->GetOperation()->GetId());
        }
    }


    void OnJobResourceUsageUpdated(
        TJobPtr job,
        TOperationElementPtr element,
        const TNodeResources& resourcesDelta)
    {
        element->ResourceUsage() += resourcesDelta;
        IncreasePoolUsage(element->GetPool(), resourcesDelta);

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
        ISchedulableElementPtr element,
        IYsonConsumer* consumer)
    {
        const auto& attributes = element->Attributes();
        BuildYsonMapFluently(consumer)
            .Item("resource_demand").Value(element->GetDemand())
            .Item("resource_usage").Value(element->ResourceUsage())
            .Item("resource_limits").Value(element->ResourceLimits())
            .Item("dominant_resource").Value(attributes.DominantResource)
            .Item("weight").Value(element->GetWeight())
            .Item("min_share_ratio").Value(element->GetMinShareRatio())
            .Item("adjusted_min_share_ratio").Value(attributes.AdjustedMinShareRatio)
            .Item("max_share_ratio").Value(attributes.MaxShareRatio)
            .Item("usage_ratio").Value(element->GetUsageRatio())
            .Item("demand_ratio").Value(attributes.DemandRatio)
            .Item("fair_share_ratio").Value(attributes.FairShareRatio)
            .Item("satisfaction_ratio").Value(attributes.SatisfactionRatio);
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

