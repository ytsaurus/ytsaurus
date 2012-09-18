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

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = SchedulerLogger;
static NProfiling::TProfiler& Profiler = SchedulerProfiler;
static Stroka DefaultPoolId("default");

////////////////////////////////////////////////////////////////////

struct ISchedulerElement;
typedef TIntrusivePtr<ISchedulerElement> ISchedulerElementPtr;

struct ISchedulableElement;
typedef TIntrusivePtr<ISchedulableElement> ISchedulableElementPtr;

struct IElementRanking;

class TOperationElement;
typedef TIntrusivePtr<TOperationElement> TOperationElementPtr;

class TCompositeSchedulerElement;
typedef TIntrusivePtr<TCompositeSchedulerElement> TCompositeSchedulerElementPtr;

class TPool;
typedef TIntrusivePtr<TPool> TPoolPtr;

class TRootElement;
typedef TIntrusivePtr<TRootElement> TRootElementPtr;

////////////////////////////////////////////////////////////////////

struct ISchedulerElement
    : public virtual TRefCounted
{
    virtual void Update(const NProto::TNodeResources& limits) = 0;
    virtual void ScheduleJobs(ISchedulingContext* context) = 0;
};

////////////////////////////////////////////////////////////////////

struct ISchedulableElement
    : public virtual ISchedulerElement
{
    virtual TInstant GetStartTime() const = 0;
    virtual double GetWeight() const = 0;
    virtual double GetMinShareRatio() const = 0;
    virtual NProto::TNodeResources GetDemand() const = 0;
    virtual NProto::TNodeResources GetUtilization() const = 0;
};

////////////////////////////////////////////////////////////////////

class TOperationElement
    : public ISchedulableElement
{
public:
    explicit TOperationElement(TOperationPtr operation)
        : Operation(operation)
        , Pool(NULL)
    { }


    virtual void ScheduleJobs(ISchedulingContext* context) override
    {
        while (context->HasSpareResources()) {
            if (Operation->GetState() != EOperationState::Running) {
                break;
            }
            auto job = Operation->GetController()->ScheduleJob(context);
            if (!job) {
                break;
            }
        }
    }

    virtual void Update(const NProto::TNodeResources& limits) override
    {
        UNUSED(limits);
    }


    virtual TInstant GetStartTime() const override
    {
        return Operation->GetStartTime();
    }

    virtual double GetWeight() const override
    {
        return Spec->Weight;
    }

    virtual double GetMinShareRatio() const override
    {
        return Spec->MinShareRatio;
    }

    virtual NProto::TNodeResources GetDemand() const override
    {
        auto controller = Operation->GetController();
        auto result = controller->GetUsedResources();
        AddResources(&result, controller->GetNeededResources());
        return result;
    }

    virtual NProto::TNodeResources GetUtilization() const override
    {
        auto controller = Operation->GetController();
        return controller->GetUsedResources();
    }

    TPooledOperationSpecPtr GetSpec() const
    {
        return Spec;
    }

    void SetSpec(TPooledOperationSpecPtr newSpec)
    {
        Spec = newSpec;
    }


    TPool* GetPool() const
    {
        return Pool;
    }

    void SetPool(TPool* pool)
    {
        Pool = pool;
    }

private:
    TOperationPtr Operation;
    TPool* Pool;
    TPooledOperationSpecPtr Spec;

};

////////////////////////////////////////////////////////////////////

struct TFairShareAttributes
{
    explicit TFairShareAttributes(ISchedulableElementPtr element)
        : Element(element)
        , DominantResource(EResourceType::Cpu)
        , Weight(0.0)
        , DemandRatio(0.0)
        , FairShareRatio(0.0)
        , AdjustedMinShareRatio(0.0)
    { }

    ISchedulableElementPtr Element;
    EResourceType DominantResource;
    double Weight;
    double DemandRatio;
    double FairShareRatio;
    double AdjustedMinShareRatio;
};

class TCompositeSchedulerElement
    : public virtual ISchedulerElement
{
public:
    TCompositeSchedulerElement()
        : Mode(EPoolMode::Fifo)
    { }

    virtual void Update(const NProto::TNodeResources& limits) override
    {
        Limits = limits;

        // Choose dominant resource types.
        // Precache weights.
        // Precache min share ratios and compute their sum.
        // Compute demand ratios and their sum.
        double demandRatioSum = 0.0;
        double minShareRatioSum = 0.0;
        FOREACH (auto& pair, Children) {
            auto& attributes = pair.second;
            
            auto demand = attributes.Element->GetDemand();
            attributes.DominantResource = GetDominantResource(demand, limits);
            i64 dominantTotal = std::max(
                GetResource(limits, attributes.DominantResource),
                static_cast<i64>(1));
            
            attributes.Weight = attributes.Element->GetWeight();
            
            attributes.AdjustedMinShareRatio = attributes.Element->GetMinShareRatio();
            minShareRatioSum += attributes.AdjustedMinShareRatio;

            attributes.DemandRatio = (double) GetResource(demand, attributes.DominantResource) / dominantTotal;
            demandRatioSum += attributes.DemandRatio;
        }

        // Scale down weights if needed.
        if (minShareRatioSum > 1.0) {
            FOREACH (auto& pair, Children) {
                auto& attributes = pair.second;
                attributes.AdjustedMinShareRatio /= minShareRatioSum;
            }
        }
        
        // Check if have more resources than totally demanded by children.
        // Additionally check for FIFO mode.
        if (demandRatioSum <= 1.0 || Mode == EPoolMode::Fifo) {
            // Easy case -- just give everyone what he needs.
            SetFairSharesFromDemands();
        } else {
            // Hard case -- compute fair shares using fit factor.
            ComputeFairSharesByFitting();
        }

        // Propagate updates to children.
        FOREACH (auto& pair, Children) {
            auto& attributes = pair.second;
            auto childLimits = Limits;
            MultiplyResources(&childLimits, attributes.FairShareRatio);
            attributes.Element->Update(childLimits);
        }
    }

    virtual void ScheduleJobs(ISchedulingContext* context) override
    {
        auto sortedChildren = GetSortedChildren();
        FOREACH (auto* attributes, sortedChildren) {
            if (!context->HasSpareResources()) {
                break;
            }
            attributes->Element->ScheduleJobs(context);
        }
    }


    void AddChild(ISchedulableElementPtr child)
    {
        TFairShareAttributes info(child);
        YCHECK(Children.insert(std::make_pair(child, info)).second);
        Update();
    }

    void RemoveChild(ISchedulableElementPtr child)
    {
        YCHECK(Children.erase(child) == 1);
        Update();
    }

    const TFairShareAttributes& GetChildAttributes(ISchedulableElementPtr child) const
    {
        auto it = Children.find(child);
        YCHECK(it != Children.end());
        return it->second;
    }

protected:
    EPoolMode Mode;

    yhash_map<ISchedulableElementPtr, TFairShareAttributes> Children;
    NProto::TNodeResources Limits;


    void Update()
    {
        Update(Limits);
    }

    void SetFairSharesFromDemands()
    {
        FOREACH (auto& pair, Children) {
            auto& attributes = pair.second;
            attributes.FairShareRatio = attributes.DemandRatio;
        }
    }

    void ComputeFairSharesByFitting()
    {
        auto computeFairShareRatio = [&] (double fitFactor, const TFairShareAttributes& attributes) -> double {
            double result = attributes.Weight * fitFactor;
            // Never give less than promised by min share.
            result = std::max(result, attributes.AdjustedMinShareRatio);
            // Never give more than demanded.
            result = std::min(result, attributes.DemandRatio);
            return result;
        };

        // Run binary search to compute fit factor.
        const double FitFactorPrecision = 1e-12;
        double fitFactorLo = 0.0;
        double fitFactorHi = 1.0;
        while (fitFactorHi - fitFactorLo > FitFactorPrecision) {
            double fitFactor = (fitFactorLo + fitFactorHi) / 2.0;
            double fairShareRatioSum = 0.0;
            FOREACH (const auto& pair, Children) {
                fairShareRatioSum += computeFairShareRatio(fitFactor, pair.second);
            }
            if (fairShareRatioSum < 1.0) {
                fitFactorLo = fitFactor;
            } else {
                fitFactorHi = fitFactor;
            }
        }

        // Compute fair share ratios.
        double fitFactor = (fitFactorLo + fitFactorHi) / 2.0;
        FOREACH (auto& pair, Children) {
            auto& attributes = pair.second;
            attributes.FairShareRatio = computeFairShareRatio(fitFactor, attributes);
        }
    }

    const std::vector<const TFairShareAttributes*> GetSortedChildren()
    {
        PROFILE_TIMING ("/fair_share_sort_time") {
            std::vector<const TFairShareAttributes*> sortedChildren;
            FOREACH (const auto& pair, Children) {
                sortedChildren.push_back(&pair.second);
            }

            switch (Mode) {
                case EPoolMode::Fifo:
                    SortChildrenFifo(&sortedChildren);
                    break;
                case EPoolMode::FairShare:
                    SortChildrenFairShare(&sortedChildren);
                    break;
                default:
                    YUNREACHABLE();
            }

            return sortedChildren;
        }
    }

    void SortChildrenFifo(std::vector<const TFairShareAttributes*>* sortedChildren)
    {
        // Sort by weight (desc), then by start time (asc).
        std::sort(
            sortedChildren->begin(),
            sortedChildren->end(),
            [] (const TFairShareAttributes* lhs, const TFairShareAttributes* rhs) -> bool {
                const auto& lhsElement = lhs->Element;
                const auto& rhsElement = rhs->Element;
                if (lhsElement->GetWeight() > rhsElement->GetWeight()) {
                    return true;
                }
                if (lhsElement->GetWeight() < rhsElement->GetWeight()) {
                    return false;
                }
                return lhsElement->GetStartTime() < rhsElement->GetStartTime();
            });
    }

    void SortChildrenFairShare(std::vector<const TFairShareAttributes*>* sortedChildren)
    {
        std::sort(
            sortedChildren->begin(),
            sortedChildren->end(),
            [&] (const TFairShareAttributes* lhs, const TFairShareAttributes* rhs) -> bool {
                bool lhsNeedy = IsNeedy(*lhs);
                bool rhsNeedy = IsNeedy(*rhs);

                if (lhsNeedy && !rhsNeedy) {
                    return true;
                }

                if (!lhsNeedy && rhsNeedy) {
                    return false;
                }

                if (lhsNeedy && rhsNeedy) {
                    double lhsUseToTotalRatio = GetUtilizationToTotalRatio(*lhs);
                    double rhsUseToTotalRatio = GetUtilizationToTotalRatio(*rhs);
                    return lhsUseToTotalRatio < rhsUseToTotalRatio;
                }

                {
                    double lhsUseToWeightRatio = GetUtilizationToTotalRatio(*lhs);
                    double rhsUseToWeightRatio = GetUtilizationToTotalRatio(*rhs);
                    return lhsUseToWeightRatio < rhsUseToWeightRatio;
                }
            });
    }

    bool IsNeedy(const TFairShareAttributes& attributes)
    {
        i64 demand = GetResource(attributes.Element->GetDemand(), attributes.DominantResource);
        i64 use = GetResource(attributes.Element->GetUtilization(), attributes.DominantResource);
        if (use >= demand) {
            return false;
        }

        i64 total = GetResource(Limits, attributes.DominantResource);
        double useRatio = (double) use / total;
        if (useRatio >= attributes.AdjustedMinShareRatio) {
            return false;
        }

        return true;
    }

    double GetUtilizationToTotalRatio(const TFairShareAttributes& attributes)
    {
        i64 use = GetResource(attributes.Element->GetUtilization(), attributes.DominantResource);
        i64 total = std::max(GetResource(Limits, attributes.DominantResource), static_cast<i64>(1));
        return (double) use / total;
    }

    static double GetUtilizationToWeightRatio(const TFairShareAttributes& attributes)
    {
        i64 use = GetResource(attributes.Element->GetUtilization(), attributes.DominantResource);
        double weight = std::max(attributes.Weight, 1.0);
        return (double) use / weight;
    }


    void SetMode(EPoolMode mode)
    {
        if (Mode != mode) {
            Mode = mode;
            Update();
        }
    }

};

////////////////////////////////////////////////////////////////////

class TPool
    : public TCompositeSchedulerElement
    , public virtual ISchedulableElement
{
public:
    explicit TPool(const Stroka& id)
        : Id(id)
    {
        SetDefaultConfig();
    }


    const Stroka& GetId() const
    {
        return Id;
    }


    TPoolConfigPtr GetConfig()
    {
        return Config;
    }

    void SetConfig(TPoolConfigPtr newConfig)
    {
        Config = newConfig;
        SetMode(Config->Mode);
    }

    void SetDefaultConfig()
    {
        SetConfig(New<TPoolConfig>());
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

    virtual NProto::TNodeResources GetDemand() const override
    {
        auto result = ZeroResources();
        FOREACH (const auto& pair, Children) {
            AddResources(&result, pair.second.Element->GetDemand());
        }
        return result;
    }

    virtual NProto::TNodeResources GetUtilization() const override
    {
        auto result = ZeroResources();
        FOREACH (const auto& pair, Children) {
            AddResources(&result, pair.second.Element->GetUtilization());
        }
        return result;
    }

private:
    Stroka Id;
    TPoolConfigPtr Config;

};

////////////////////////////////////////////////////////////////////

class TRootElement
    : public TCompositeSchedulerElement
{
public:
    TRootElement()
    {
        SetMode(EPoolMode::FairShare);
    }
};

////////////////////////////////////////////////////////////////////

static TDuration UpdateInterval = TDuration::Seconds(1);

class TFairShareStrategy
    : public ISchedulerStrategy
{
public:
    explicit TFairShareStrategy(ISchedulerStrategyHost* host)
        : Host(host)
    {
        host->SubscribeOperationStarted(BIND(&TFairShareStrategy::OnOperationStarted, this));
        host->SubscribeOperationFinished(BIND(&TFairShareStrategy::OnOperationFinished, this));

        auto* masterConnector = host->GetMasterConnector();
        masterConnector->SubscribeWatcherRequest(BIND(&TFairShareStrategy::OnPoolsRequest, this));
        masterConnector->SubscribeWatcherResponse(BIND(&TFairShareStrategy::OnPoolsResponse, this));

        RootElement = New<TRootElement>();

        DefaultPool = New<TPool>(DefaultPoolId);
        RegisterPool(DefaultPool);
    }

    virtual void ScheduleJobs(ISchedulingContext* context) override
    {
        auto now = TInstant::Now();
        if (!LastUpdateTime || now > LastUpdateTime.Get() + UpdateInterval) {
            PROFILE_TIMING ("/fair_share_update_time") {
                RootElement->Update(Host->GetTotalResourceLimits());
            }
            LastUpdateTime = now;
        }
        return RootElement->ScheduleJobs(context);
    }

    virtual void BuildOperationProgressYson(TOperationPtr operation, IYsonConsumer* consumer) override
    {
        auto element = GetOperationElement(operation);
        auto pool = element->GetPool();
        const auto& attributes = pool->GetChildAttributes(element);
        BuildYsonMapFluently(consumer)
            .Item("pool").Scalar(pool->GetId())
            .Item("start_time").Scalar(element->GetStartTime())
            .Item("min_share_ratio").Scalar(element->GetMinShareRatio())
            .Do(BIND(&TFairShareStrategy::BuildElementYson, pool, element));
    }

    virtual void BuildOrchidYson(NYTree::IYsonConsumer* consumer) override
    {
        BuildYsonMapFluently(consumer)
            .Item("pools").DoMapFor(Pools, [&] (TFluentMap fluent, const TPoolMap::value_type& pair) {
                const auto& id = pair.first;
                auto pool = pair.second;
                auto config = pool->GetConfig();
                fluent
                    .Item(id).BeginMap()
                        .Item("mode").Scalar(config->Mode)
                        .Item("weight").Scalar(config->Weight)
                        .Item("min_share_ratio").Scalar(config->MinShareRatio)
                        .Do(BIND(&TFairShareStrategy::BuildElementYson, RootElement, pool))
                    .EndMap();
            });
    }

private:
    ISchedulerStrategyHost* Host;

    typedef yhash_map<Stroka, TPoolPtr> TPoolMap;
    TPoolMap Pools;
    TPoolPtr DefaultPool;

    yhash_map<TOperationPtr, TOperationElementPtr> OperationToElement;

    TRootElementPtr RootElement;
    TNullable<TInstant> LastUpdateTime;

    void OnOperationStarted(TOperationPtr operation)
    {
        auto operationElement = New<TOperationElement>(operation);
        YCHECK(OperationToElement.insert(std::make_pair(operation, operationElement)).second);

        TPooledOperationSpecPtr spec;
        try {
            spec = ConvertTo<TPooledOperationSpecPtr>(operation->GetSpec());
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error parsing spec of pooled operation %s, defaults will be used",
                ~ToString(operation->GetOperationId()));
            spec = New<TPooledOperationSpec>();
        }

        TPoolPtr pool;
        if (spec->Pool) {
            pool = FindPool(spec->Pool.Get());
            if (!pool) {
                LOG_ERROR("Invalid pool %s specified for operation %s, using %s",
                    ~spec->Pool.Get().Quote(),
                    ~ToString(operation->GetOperationId()),
                    ~DefaultPool->GetId().Quote());
            }
        }
        if (!pool) {
            pool = DefaultPool;
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
    }


    void RegisterPool(TPoolPtr pool)
    {
        LOG_INFO("Pool registered: %s", ~pool->GetId());
        
        YCHECK(Pools.insert(std::make_pair(pool->GetId(), pool)).second);
        RootElement->AddChild(pool);
    }

    void UnregisterPool(TPoolPtr pool)
    {
        LOG_INFO("Pool unregistered: %s", ~pool->GetId());

        YCHECK(Pools.erase(pool->GetId()) == 1);
        RootElement->RemoveChild(pool);
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
        batchReq->AddRequest(req, "get_pools");
    }

    void OnPoolsResponse(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_pools");
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

        // Build the set of potential orphans.
        yhash_set<Stroka> orphanPoolIds;
        FOREACH (const auto& pair, Pools) {
            const auto& id = pair.first;
            const auto& pool = pair.second;
            YCHECK(orphanPoolIds.insert(pair.first).second);
        }

        auto newPoolsNode = ConvertToNode(TYsonString(rsp->value()));
        auto newPoolsMapNode = newPoolsNode->AsMap();
        FOREACH (const auto& pair, newPoolsMapNode->GetChildren()) {
            const auto& id = pair.first;
            const auto& poolNode = pair.second;
            auto existingPool = FindPool(id);
            if (existingPool) {
                // Reconfigure existing pool.
                auto configNode = ConvertToNode(poolNode->Attributes());
                TPoolConfigPtr config;
                try {
                    config = ConvertTo<TPoolConfigPtr>(configNode);
                } catch (const std::exception& ex) {
                    LOG_ERROR(ex, "Error parsing configuration of pool %s, defauls will be used",
                        ~id.Quote());
                    config = New<TPoolConfig>();
                }
                existingPool->SetConfig(config);
                YCHECK(orphanPoolIds.erase(id) == 1);
            } else {
                // Create new pool.
                auto newPool = New<TPool>(id);
                RegisterPool(newPool);
            }
        }

        // Unregister orphan pools.
        FOREACH (const auto& id, orphanPoolIds) {
            auto pool = GetPool(id);
            if (pool == DefaultPool) {
                // Default pool is always present.
                // When it's configuration vanishes it just gets the default config.
                pool->SetDefaultConfig();
            } else {
                UnregisterPool(pool);
            }
        }

        LOG_INFO("Pools updated");
    }


    static void BuildElementYson(
        TCompositeSchedulerElementPtr composite,
        ISchedulableElementPtr element,
        IYsonConsumer* consumer)
    {
        const auto& attributes = composite->GetChildAttributes(element);
        BuildYsonMapFluently(consumer)
            .Item("resource_demand").Do(BIND(&BuildNodeResourcesYson, element->GetDemand()))
            .Item("resource_utilization").Do(BIND(&BuildNodeResourcesYson, element->GetUtilization()))
            .Item("dominant_resource").Scalar(attributes.DominantResource)
            .Item("adjusted_min_share_ratio").Scalar(attributes.AdjustedMinShareRatio)
            .Item("demand_ratio").Scalar(attributes.DemandRatio)
            .Item("fair_share_ratio").Scalar(attributes.FairShareRatio);

    }

};

TAutoPtr<ISchedulerStrategy> CreateFairShareStrategy(ISchedulerStrategyHost* host)
{
    return new TFairShareStrategy(host);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

