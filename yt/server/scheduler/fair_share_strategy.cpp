#include "stdafx.h"
#include "fair_share_strategy.h"
#include "scheduler_strategy.h"
#include "master_connector.h"
#include "job_resources.h"

#include <ytlib/ytree/yson_serializable.h>

#include <ytlib/ytree/ypath_proxy.h>

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

class TPool;
typedef TIntrusivePtr<TPool> TPoolPtr;

class TRootElement;
typedef TIntrusivePtr<TRootElement> TRootElementPtr;

////////////////////////////////////////////////////////////////////

struct ISchedulerElement
    : public virtual TRefCounted
{
    virtual void Update(const NProto::TNodeResources& totalResources) = 0;
    virtual void ScheduleJobs(ISchedulingContext* context) = 0;
};

////////////////////////////////////////////////////////////////////

struct ISchedulableElement
    : public virtual ISchedulerElement
{
    virtual TInstant GetStartTime() const = 0;
    virtual double GetWeight() const = 0;
    virtual double GetMinShare() const = 0;
    virtual NProto::TNodeResources GetDemand() const = 0;
    virtual NProto::TNodeResources GetUse() const = 0;
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
            auto job = Operation->GetController()->ScheduleJob(context);
            if (!job) {
                break;
            }
        }
    }

    virtual void Update(const NProto::TNodeResources& totalResources) override
    {
        UNUSED(totalResources);
    }


    virtual TInstant GetStartTime() const override
    {
        return Operation->GetStartTime();
    }

    virtual double GetWeight() const override
    {
        return Spec->Weight;
    }

    virtual double GetMinShare() const override
    {
        // TODO(babenko): implement
        return 0;
    }

    virtual NProto::TNodeResources GetDemand() const override
    {
        auto controller = Operation->GetController();
        auto result = controller->GetUsedResources();
        AddResources(&result, controller->GetNeededResources());
        return result;
    }

    virtual NProto::TNodeResources GetUse() const override
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

class TCompositeSchedulerElement
    : public virtual ISchedulerElement
{
public:
    TCompositeSchedulerElement()
        : Mode(EPoolMode::Fifo)
    { }

    virtual void Update(const NProto::TNodeResources& totalResources) override
    {
        TotalResources = totalResources;

        // Choose dominant resource types.
        // Precache weights and min share ratios.
        // Compute demand ratios and their sum.
        double demandRatioSum = 0.0;
        FOREACH (auto& pair, Children) {
            auto& childInfo = pair.second;
            auto demand = childInfo.Element->GetDemand();
            childInfo.DominantType = GetDominantResource(demand, totalResources);
            i64 dominantTotal = GetResource(totalResources, childInfo.DominantType);
            childInfo.Weight = childInfo.Element->GetWeight();
            childInfo.MinShareRatio = (double) childInfo.Element->GetMinShare() / dominantTotal;
            childInfo.DemandRatio = (double) GetResource(demand, childInfo.DominantType) / dominantTotal;
            demandRatioSum += childInfo.DemandRatio;
        }
        
        // Check if use have more resources than demanded.
        if (demandRatioSum <= 1.0) {
            FOREACH (auto& pair, Children) {
                auto& childInfo = pair.second;
                childInfo.FairShareRatio = std::min(childInfo.MinShareRatio, childInfo.DemandRatio);
            }
            return;
        }


        // Compute fit factor.
        auto computeFairShareRatio = [&] (double fitFactor, const TChildInfo& childInfo) -> double {
            double result = childInfo.Weight * fitFactor;
            result = std::max(result, childInfo.MinShareRatio);
            result = std::min(result, childInfo.DemandRatio);
            return result;
        };

        const double FitFactorPrecision = 1e-6;

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
            auto& childInfo = pair.second;
            childInfo.FairShareRatio = computeFairShareRatio(fitFactor, childInfo);
        }
    }

    virtual void ScheduleJobs(ISchedulingContext* context) override
    {
        auto sortedChildren = GetSortedChildren();
        FOREACH (auto* childInfo, sortedChildren) {
            if (!context->HasSpareResources()) {
                break;
            }
            childInfo->Element->ScheduleJobs(context);
        }
    }


    void AddChild(ISchedulableElementPtr child)
    {
        TChildInfo info(child);
        YCHECK(Children.insert(std::make_pair(child, info)).second);
        Update();
    }

    void RemoveChild(ISchedulableElementPtr child)
    {
        YCHECK(Children.erase(child) == 1);
        Update();
    }

protected:
    EPoolMode Mode;

    struct TChildInfo
    {
        explicit TChildInfo(ISchedulableElementPtr element)
            : Element(element)
            , Weight(0.0)
            , DemandRatio(0.0)
            , FairShareRatio(0.0)
            , MinShareRatio(0.0)
        { }

        ISchedulableElementPtr Element;
        EResourceType DominantType;
        double Weight;
        double DemandRatio;
        double FairShareRatio;
        double MinShareRatio;
    };

    yhash_map<ISchedulableElementPtr, TChildInfo> Children;
    NProto::TNodeResources TotalResources;


    void Update()
    {
        Update(TotalResources);
    }

    const std::vector<const TChildInfo*> GetSortedChildren()
    {
        PROFILE_TIMING ("/fair_share_sort_time") {
            std::vector<const TChildInfo*> sortedChildren;
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

    void SortChildrenFifo(std::vector<const TChildInfo*>* sortedChildren)
    {
        // Sort by weight (desc), then by start time (asc).
        std::sort(
            sortedChildren->begin(),
            sortedChildren->end(),
            [] (const TChildInfo* lhs, const TChildInfo* rhs) -> bool {
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

    void SortChildrenFairShare(std::vector<const TChildInfo*>* sortedChildren)
    {
        std::sort(
            sortedChildren->begin(),
            sortedChildren->end(),
            [&] (const TChildInfo* lhs, const TChildInfo* rhs) -> bool {
                bool lhsNeedy = IsNeedy(*lhs);
                bool rhsNeedy = IsNeedy(*rhs);

                if (lhsNeedy && !rhsNeedy) {
                    return true;
                }

                if (!lhsNeedy && rhsNeedy) {
                    return false;
                }

                if (lhsNeedy && rhsNeedy) {
                    double lhsUseToTotalRatio = GetUseToTotalRatio(*lhs);
                    double rhsUseToTotalRatio = GetUseToTotalRatio(*rhs);
                    return lhsUseToTotalRatio < rhsUseToTotalRatio;
                }

                {
                    double lhsUseToWeightRatio = GetUseToTotalRatio(*lhs);
                    double rhsUseToWeightRatio = GetUseToTotalRatio(*rhs);
                    return lhsUseToWeightRatio < rhsUseToWeightRatio;
                }
            });
    }

    bool IsNeedy(const TChildInfo& childInfo)
    {
        i64 demand = GetResource(childInfo.Element->GetDemand(), childInfo.DominantType);
        i64 use = GetResource(childInfo.Element->GetUse(), childInfo.DominantType);
        if (use >= demand) {
            return false;
        }

        i64 total = GetResource(TotalResources, childInfo.DominantType);
        double useRatio = (double) use / total;
        if (useRatio >= childInfo.MinShareRatio) {
            return false;
        }

        return true;
    }

    double GetUseToTotalRatio(const TChildInfo& childInfo)
    {
        i64 use = GetResource(childInfo.Element->GetUse(), childInfo.DominantType);
        i64 total = std::max(GetResource(TotalResources, childInfo.DominantType), static_cast<i64>(1));
        return (double) use / total;
    }

    double GetUseToWeightRatio(const TChildInfo& childInfo)
    {
        i64 use = GetResource(childInfo.Element->GetUse(), childInfo.DominantType);
        double weight = std::max(childInfo.Weight, 1.0);
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
        // Makes no sense to pools since the root is in fair-share mode.
        return TInstant();
    }

    virtual double GetWeight() const override
    {
        return Config->Weight;
    }

    virtual double GetMinShare() const override
    {
        return Config->MinShare;
    }

    virtual NProto::TNodeResources GetDemand() const override
    {
        auto result = ZeroResources();
        FOREACH (const auto& pair, Children) {
            AddResources(&result, pair.second.Element->GetDemand());
        }
        return result;
    }

    virtual NProto::TNodeResources GetUse() const override
    {
        auto result = ZeroResources();
        FOREACH (const auto& pair, Children) {
            AddResources(&result, pair.second.Element->GetUse());
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
        SetMode(EPoolMode::Fifo);
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

private:
    ISchedulerStrategyHost* Host;

    yhash_map<Stroka, TPoolPtr> IdToPool;
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
        auto it = OperationToElement.find(operation);
        YCHECK(it != OperationToElement.end());
        auto element = it->second;
        auto pool = element->GetPool();

        OperationToElement.erase(it);
        pool->RemoveChild(element);

        LOG_INFO("Operation removed from pool (OperationId: %s, PoolId: %s)",
            ~ToString(operation->GetOperationId()),
            ~pool->GetId());
    }


    void RegisterPool(TPoolPtr pool)
    {
        LOG_INFO("Pool registered: %s", ~pool->GetId());
        
        YCHECK(IdToPool.insert(std::make_pair(pool->GetId(), pool)).second);
        RootElement->AddChild(pool);
    }

    void UnregisterPool(TPoolPtr pool)
    {
        LOG_INFO("Pool unregistered: %s", ~pool->GetId());

        YCHECK(IdToPool.erase(pool->GetId()) == 1);
        RootElement->RemoveChild(pool);
    }

    TPoolPtr FindPool(const Stroka& id)
    {
        auto it = IdToPool.find(id);
        return it == IdToPool.end() ? NULL : it->second;
    }

    TPoolPtr GetPool(const Stroka& id)
    {
        auto pool = FindPool(id);
        YCHECK(pool);
        return pool;
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
        FOREACH (const auto& pair, IdToPool) {
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

};

TAutoPtr<ISchedulerStrategy> CreateFairShareStrategy(ISchedulerStrategyHost* host)
{
    return new TFairShareStrategy(host);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

