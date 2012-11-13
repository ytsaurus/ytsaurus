#include "stdafx.h"
#include "fair_share_strategy.h"
#include "scheduler_strategy.h"
#include "master_connector.h"
#include "job_resources.h"

#include <ytlib/ytree/yson_serializable.h>
#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/ytree/fluent.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/logging/log.h>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NObjectClient;
using NScheduler::NProto::TNodeResources;

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = SchedulerLogger;
static NProfiling::TProfiler& Profiler = SchedulerProfiler;
static Stroka DefaultPoolId("default");
static const double RatioPrecision = 1e-12;

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
        , Weight(0.0)
        , DemandRatio(0.0)
        , FairShareRatio(0.0)
        , AdjustedMinShareRatio(0.0)
    { }

    int Rank;
    EResourceType DominantResource;
    double Weight;
    double DemandRatio;
    double FairShareRatio;
    double AdjustedMinShareRatio;
};

////////////////////////////////////////////////////////////////////

struct ISchedulerElement
    : public virtual TRefCounted
{
    virtual void Update(double limitsRatio) = 0;
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
    virtual TNodeResources GetUtilization() const = 0;
    virtual TNodeResources GetLimits() const = 0;

};

////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EOperationStatus,
    (Normal)
    (BelowMinShare)
    (BelowFairShare)
);

class TOperationElement
    : public ISchedulableElement
{
public:
    explicit TOperationElement(
        TFairShareStrategyConfigPtr config,
        ISchedulerStrategyHost* host,
        TOperationPtr operation)
        : Config(config)
        , Host(host)
        , Operation_(operation)
        , Pool_(NULL)
        , Starving_(false)
    { }


    virtual bool ScheduleJobs(
        ISchedulingContext* context,
        bool starvingOnly) override
    {
        if (starvingOnly && !Starving_) {
            return false;
        }

        auto node = context->GetNode();
        auto controller = Operation_->GetController();

        bool result =  false;
        while (Operation_->GetState() == EOperationState::Running) {
            if (!node->HasSpareResources()) {
                break;
            }

            auto job = controller->ScheduleJob(context, Starving_);
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

    virtual void Update(double limitsRatio) override
    {
        UNUSED(limitsRatio);

        Limits_ = Host->GetTotalResourceLimits() * limitsRatio;
    }

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
        auto controller = Operation_->GetController();
        return controller->GetUsedResources() + controller->GetNeededResources();
    }

    virtual TNodeResources GetUtilization() const override
    {
        auto controller = Operation_->GetController();
        return controller->GetUsedResources();
    }


    double GetUtilizationRatio() const
    {
        const auto& attributes = Attributes();
        auto utilization = GetUtilization() - UtilizationDiscount_;
        i64 utilizationComponent = GetResource(utilization, attributes.DominantResource);
        i64 limitsComponent = GetResource(Limits_, attributes.DominantResource);
        return limitsComponent == 0 ? 1.0 : (double) utilizationComponent / limitsComponent;
    }

    EOperationStatus GetStatus() const
    {
        double utilizationRatio = GetUtilizationRatio();

        if (utilizationRatio < Attributes_.AdjustedMinShareRatio * Config->MinShareTolerance) {
            return EOperationStatus::BelowMinShare;
        }

        if (utilizationRatio < Attributes_.FairShareRatio * Config->FairShareTolerance) {
            return EOperationStatus::BelowFairShare;
        }

        return EOperationStatus::Normal;
    }


    DEFINE_BYREF_RW_PROPERTY(TSchedulableAttributes, Attributes);
    DEFINE_BYVAL_RO_PROPERTY(TOperationPtr, Operation);
    DEFINE_BYVAL_RO_PROPERTY(TNodeResources, Limits);
    DEFINE_BYVAL_RW_PROPERTY(TPooledOperationSpecPtr, Spec);
    DEFINE_BYVAL_RW_PROPERTY(TPool*, Pool);
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, BelowMinShareSince);
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, BelowFairShareSince);
    DEFINE_BYVAL_RW_PROPERTY(bool, Starving);
    DEFINE_BYREF_RW_PROPERTY(TNodeResources, UtilizationDiscount);

private:
    TFairShareStrategyConfigPtr Config;
    ISchedulerStrategyHost* Host;

};

////////////////////////////////////////////////////////////////////

class TCompositeSchedulerElement
    : public virtual ISchedulerElement
{
public:
    explicit TCompositeSchedulerElement(ISchedulerStrategyHost* host)
        : Host(host)
        , Mode(ESchedulingMode::Fifo)
        , LimitsRatio(0.0)
        , Limits(ZeroNodeResources())
    { }

    virtual void Update(double limitsRatio) override
    {
        LimitsRatio = limitsRatio;
        Limits = Host->GetTotalResourceLimits() * limitsRatio;

        ComputeFairShares();
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
        ComputeFairShares();
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
    
    double LimitsRatio;
    TNodeResources Limits;


    void ComputeFairShares()
    {
        // Choose dominant resource types.
        // Precache weights.
        // Precache min share ratios and compute their sum.
        // Compute demand ratios and their sum.
        double demandRatioSum = 0.0;
        double minShareRatioSum = 0.0;
        auto totalLimits = Host->GetTotalResourceLimits();
        FOREACH (auto child, Children) {
            auto& attributes = child->Attributes();

            auto demand = child->GetDemand();
            attributes.DominantResource = GetDominantResource(demand, totalLimits);
            i64 dominantLimits = GetResource(totalLimits, attributes.DominantResource);
            i64 dominantDemand = GetResource(demand, attributes.DominantResource);
            attributes.DemandRatio = dominantLimits == 0 ? LimitsRatio : (double) dominantDemand / dominantLimits;
            demandRatioSum += attributes.DemandRatio;

            attributes.Weight = child->GetWeight();

            if (dominantDemand != 0) {
                attributes.AdjustedMinShareRatio = child->GetMinShareRatio() * LimitsRatio;
                minShareRatioSum += attributes.AdjustedMinShareRatio;
            } else {
                attributes.AdjustedMinShareRatio = 0.0;
            }
        }

        // Scale down weights if needed.
        if (minShareRatioSum > LimitsRatio) {
            FOREACH (auto child, Children) {
                auto& attributes = child->Attributes();
                attributes.AdjustedMinShareRatio *= (LimitsRatio / minShareRatioSum);
            }
        }

        // Check for FIFO mode.
        // Check if we have more resources than totally demanded by children.
        if (Mode == ESchedulingMode::Fifo) {
            // Set fair shares equal to limits ratio. This is done just for convenience.
            SetFifoFairShares();
        } else if (demandRatioSum <= LimitsRatio) {
            // Easy case -- just give everyone what he needs.
            SetDemandedFairShares();
        } else {
            // Hard case -- compute fair shares using fit factor.
            ComputeFairSharesByFitting();
        }

        // Propagate updates to children.
        FOREACH (auto child, Children) {
            auto& attributes = child->Attributes();           
            child->Update(attributes.FairShareRatio);
        }
    }

    void SetFifoFairShares()
    {
        // The first child gets everything, others get none.
        FOREACH (auto child, Children) {
            auto& attributes = child->Attributes();
            attributes.FairShareRatio = attributes.Rank == 0 ? LimitsRatio : 0.0;
        }
    }

    void SetDemandedFairShares()
    {
        FOREACH (auto child, Children) {
            auto& attributes = child->Attributes();
            attributes.FairShareRatio = attributes.DemandRatio;
        }
    }

    void ComputeFairSharesByFitting()
    {
        auto computeFairShareRatio = [&] (double fitFactor, const TSchedulableAttributes& attributes) -> double {
            double result = attributes.Weight * fitFactor;
            // Never give less than promised by min share.
            result = std::max(result, attributes.AdjustedMinShareRatio);
            // Never give more than demanded.
            result = std::min(result, attributes.DemandRatio);
            return result;
        };

        // Run binary search to compute fit factor.
        double fitFactorLo = 0.0;
        double fitFactorHi = 1.0;
        while (fitFactorHi - fitFactorLo > RatioPrecision) {
            double fitFactor = (fitFactorLo + fitFactorHi) / 2.0;
            double fairShareRatioSum = 0.0;
            FOREACH (auto child, Children) {
                fairShareRatioSum += computeFairShareRatio(fitFactor, child->Attributes());
            }
            if (fairShareRatioSum < LimitsRatio) {
                fitFactorLo = fitFactor;
            } else {
                fitFactorHi = fitFactor;
            }
        }

        // Compute fair share ratios.
        double fitFactor = (fitFactorLo + fitFactorHi) / 2.0;
        FOREACH (auto child, Children) {
            auto& attributes = child->Attributes();
            attributes.FairShareRatio = computeFairShareRatio(fitFactor, attributes);
        }
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
                    double lhsUseToTotalRatio = GetUtilizationToLimitsRatio(lhs);
                    double rhsUseToTotalRatio = GetUtilizationToLimitsRatio(rhs);
                    return lhsUseToTotalRatio < rhsUseToTotalRatio;
                }

                {
                    double lhsUseToWeightRatio = GetUtilizationToLimitsRatio(lhs);
                    double rhsUseToWeightRatio = GetUtilizationToLimitsRatio(rhs);
                    return lhsUseToWeightRatio < rhsUseToWeightRatio;
                }
            });
    }


    bool IsNeedy(ISchedulableElementPtr element)
    {
        const auto& attributes = element->Attributes();
        i64 demand = GetResource(element->GetDemand(), attributes.DominantResource);
        i64 utilization = GetResource(element->GetUtilization(), attributes.DominantResource);
        i64 limits = GetResource(Limits, attributes.DominantResource);
        return utilization < demand && utilization < limits * attributes.AdjustedMinShareRatio;
    }

    double GetUtilizationToLimitsRatio(ISchedulableElementPtr element)
    {
        const auto& attributes = element->Attributes();
        i64 utilization = GetResource(element->GetUtilization(), attributes.DominantResource);
        i64 limits = GetResource(Limits, attributes.DominantResource);
        return limits == 0 ? 1.0 : (double) utilization / limits;
    }

    static double GetUtilizationToWeightRatio(ISchedulableElementPtr element)
    {
        const auto& attributes = element->Attributes();
        i64 utilization = GetResource(element->GetUtilization(), attributes.DominantResource);
        i64 weight = attributes.Weight;
        return weight == 0 ? 1.0 : (double) utilization / weight;
    }


    void SetMode(ESchedulingMode mode)
    {
        if (Mode != mode) {
            Mode = mode;
            ComputeFairShares();
        }
    }

};

////////////////////////////////////////////////////////////////////

class TPool
    : public TCompositeSchedulerElement
    , public virtual ISchedulableElement
{
public:
    TPool(
        ISchedulerStrategyHost* host,
        const Stroka& id)
        : TCompositeSchedulerElement(host)
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

    virtual TNodeResources GetUtilization() const override
    {
        auto result = ZeroNodeResources();
        FOREACH (auto child, Children) {
            result += child->GetUtilization();
        }
        return result;
    }

    virtual TNodeResources GetLimits() const override
    {
        auto result = ZeroNodeResources();
        FOREACH (auto child, Children) {
            result += child->GetLimits();
        }
        return result;
    }

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
        Host->SubscribeOperationStarted(BIND(&TFairShareStrategy::OnOperationStarted, this));
        Host->SubscribeOperationFinished(BIND(&TFairShareStrategy::OnOperationFinished, this));

        Host->SubscribeJobStarted(BIND(&TFairShareStrategy::OnJobStarted, this));
        Host->SubscribeJobFinished(BIND(&TFairShareStrategy::OnJobFinished, this));

        auto* masterConnector = Host->GetMasterConnector();
        masterConnector->SubscribeWatcherRequest(BIND(&TFairShareStrategy::OnPoolsRequest, this));
        masterConnector->SubscribeWatcherResponse(BIND(&TFairShareStrategy::OnPoolsResponse, this));

        RootElement = New<TRootElement>(Host);
    }

    virtual void ScheduleJobs(ISchedulingContext* context) override
    {
        // Run periodic update.
        auto now = TInstant::Now();
        if (!LastUpdateTime || now > LastUpdateTime.Get() + Config->FairShareUpdatePeriod) {
            PROFILE_TIMING ("/fair_share_update_time") {
                // The root element get the whole cluster.
                RootElement->Update(1.0);

                // Update starvation flags for all operations.
                FOREACH (const auto& pair, OperationToElement) {
                    CheckForStarvation(pair.second);
                }
            }
            LastUpdateTime = now;
        }

        // First-chance scheduling.
        RootElement->ScheduleJobs(context, false);

        // Compute discount to node utilization.
        auto node = context->GetNode();
        yhash_set<TOperationElementPtr> discountedElements;
        std::vector<TJobPtr> preemptableJobs;
        FOREACH (auto job, context->RunningJobs()) {
            auto operation = job->GetOperation();
            if (operation->GetState() != EOperationState::Running) {
                continue;
            }

            auto element = GetOperationElement(operation);
            discountedElements.insert(element);
            element->UtilizationDiscount() += job->ResourceUtilization();

            double utilizationRatio = element->GetUtilizationRatio();
            if (utilizationRatio > Config->MinPreemptionRatio &&
                utilizationRatio > element->Attributes().FairShareRatio * Config->PreemptionTolerance)
            {
                preemptableJobs.push_back(job);
                node->ResourceUtilizationDiscount() += job->ResourceUtilization();
            }
        }

        // Second-chance scheduling.
        bool needsPreemption = RootElement->ScheduleJobs(context, true);

        // Reset discounts.
        node->ResourceUtilizationDiscount() = ZeroNodeResources();
        FOREACH (auto element, discountedElements) {
            element->UtilizationDiscount() = ZeroNodeResources();
        }

        // Preempt jobs if needed.
        if (needsPreemption) {
            std::sort(
                preemptableJobs.begin(),
                preemptableJobs.end(),
                [] (const TJobPtr& lhs, const TJobPtr& rhs) {
                    return lhs->GetStartTime() > rhs->GetStartTime();
                });

            FOREACH (auto job, preemptableJobs) {
                if (Dominates(node->ResourceLimits(), node->ResourceUtilization())) {
                    break;
                }
                context->PreemptJob(job);
            }
        }
    }


    virtual void BuildOperationProgressYson(TOperationPtr operation, IYsonConsumer* consumer) override
    {
        auto element = GetOperationElement(operation);
        auto pool = element->GetPool();
        BuildYsonMapFluently(consumer)
            .Item("pool").Scalar(pool->GetId())
            .Item("start_time").Scalar(element->GetStartTime())
            .Item("resource_limits").Scalar(element->GetLimits())
            .Item("scheduling_status").Scalar(element->GetStatus())
            .Item("starving").Scalar(element->GetStarving())
            .Item("utilization_ratio").Scalar(element->GetUtilizationRatio())
            .Do(BIND(&TFairShareStrategy::BuildElementYson, pool, element));
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
                        .Item("mode").Scalar(config->Mode)
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


    void OnOperationStarted(TOperationPtr operation)
    {
        auto operationElement = New<TOperationElement>(Config, Host, operation);
        YCHECK(OperationToElement.insert(std::make_pair(operation, operationElement)).second);

        TPooledOperationSpecPtr spec;
        try {
            spec = ConvertTo<TPooledOperationSpecPtr>(operation->GetSpec());
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error parsing spec of pooled operation %s, defaults will be used",
                ~ToString(operation->GetOperationId()));
            spec = New<TPooledOperationSpec>();
        }

        auto poolId = spec->Pool ? spec->Pool.Get() : DefaultPoolId;
        auto pool = FindPool(poolId);
        if (!pool) {
            pool = New<TPool>(Host, poolId);
            RegisterPool(pool);
        }

        operationElement->SetSpec(spec);

        operationElement->SetPool(~pool);
        pool->AddChild(operationElement);

        LOG_INFO("Operation added to pool (OperationId: %s, PoolId: %s)",
            ~ToString(operation->GetOperationId()),
            ~pool->GetId());
    }

    void OnOperationFinished(TOperationPtr operation)
    {
        auto element = GetOperationElement(operation);
        auto pool = element->GetPool();

        YCHECK(OperationToElement.erase(operation) == 1);
        pool->RemoveChild(element);

        LOG_INFO("Operation removed from pool (OperationId: %s, PoolId: %s)",
            ~ToString(operation->GetOperationId()),
            ~pool->GetId());

        if (pool->IsEmpty() && pool->IsDefaultConfigured()) {
            UnregisterPool(pool);
        }
    }


    void OnJobStarted(TJobPtr job)
    {
        auto it = JobList.insert(JobList.begin(), job);
        YCHECK(JobToIterator.insert(std::make_pair(job, it)).second);
    }

    void OnJobFinished(TJobPtr job)
    {
        auto it = JobToIterator.find(job);
        YASSERT(it != JobToIterator.end());
        JobList.erase(it->second);
        JobToIterator.erase(it);
    }


    void RegisterPool(TPoolPtr pool)
    {
        YCHECK(Pools.insert(std::make_pair(pool->GetId(), pool)).second);
        RootElement->AddChild(pool);

        LOG_INFO("Pool registered: %s", ~pool->GetId());
    }

    void UnregisterPool(TPoolPtr pool)
    {
        YCHECK(pool->IsEmpty());

        YCHECK(Pools.erase(pool->GetId()) == 1);
        RootElement->RemoveChild(pool);

        LOG_INFO("Pool unregistered: %s", ~pool->GetId());
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


    void OnPoolsRequest(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        LOG_INFO("Updating pools");

        auto req = TYPathProxy::Get("//sys/scheduler/pools");
        static auto poolConfigTemplate = New<TPoolConfig>();
        auto poolConfigKeys = poolConfigTemplate->GetRegisteredKeys();
        TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly, poolConfigKeys);
        *req->mutable_attribute_filter() = ToProto(attributeFilter);
        batchReq->AddRequest(req, "get_pools");
    }

    void OnPoolsResponse(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_pools");
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

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
        switch (status) {
            case EOperationStatus::BelowMinShare:
                if (!element->GetBelowMinShareSince()) {
                    element->SetBelowMinShareSince(now);
                } else if (element->GetBelowMinShareSince().Get() < now - spec->MinSharePreemptionTimeout) {
                    SetStarving(element, status);
                }
                break;

            case EOperationStatus::BelowFairShare:
                if (!element->GetBelowFairShareSince()) {
                    element->SetBelowFairShareSince(now);
                } else if (element->GetBelowFairShareSince().Get() < now - spec->FairSharePreemptionTimeout) {
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


    static void BuildElementYson(
        TCompositeSchedulerElementPtr composite,
        ISchedulableElementPtr element,
        IYsonConsumer* consumer)
    {
        const auto& attributes = element->Attributes();
        BuildYsonMapFluently(consumer)
            .Item("scheduling_rank").Scalar(attributes.Rank)
            .Item("resource_demand").Scalar(element->GetDemand())
            .Item("resource_utilization").Scalar(element->GetUtilization())
            .Item("dominant_resource").Scalar(attributes.DominantResource)
            .Item("weight").Scalar(element->GetWeight())
            .Item("min_share_ratio").Scalar(element->GetMinShareRatio())
            .Item("adjusted_min_share_ratio").Scalar(attributes.AdjustedMinShareRatio)
            .Item("demand_ratio").Scalar(attributes.DemandRatio)
            .Item("fair_share_ratio").Scalar(attributes.FairShareRatio);
    }

};

TAutoPtr<ISchedulerStrategy> CreateFairShareStrategy(
    TFairShareStrategyConfigPtr config,
    ISchedulerStrategyHost* host)
{
    return new TFairShareStrategy(config, host);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

