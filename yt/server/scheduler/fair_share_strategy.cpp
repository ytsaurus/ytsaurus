#include "stdafx.h"
#include "fair_share_strategy.h"
#include "scheduler_strategy.h"
#include "master_connector.h"

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
    virtual void ScheduleJobs(ISchedulingContext* context) = 0;
};

////////////////////////////////////////////////////////////////////

struct ISchedulableElement
    : public virtual ISchedulerElement
{
    virtual TInstant GetStartTime() const = 0;
    virtual int GetWeight() const = 0;
    virtual double GetMinShare() const = 0;
};

////////////////////////////////////////////////////////////////////

struct IElementRanking
{
    virtual ~IElementRanking()
    { }

    virtual void AddElement(ISchedulableElementPtr element) = 0;
    virtual void RemoveElement(ISchedulableElementPtr element) = 0;
    virtual const std::vector<ISchedulableElementPtr>& GetRankedElements() = 0;
};

////////////////////////////////////////////////////////////////////

class TOperationElement
    : public ISchedulableElement
{
public:
    explicit TOperationElement(TOperationPtr operation)
        : Operation(operation)
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

    virtual TInstant GetStartTime() const override
    {
        return Operation->GetStartTime();
    }

    virtual int GetWeight() const
    {
        // TODO(babenko): implement
        return 0;
    }

    virtual double GetMinShare() const
    {
        // TODO(babenko): implement
        return 0;
    }


    TPooledOperationSpecPtr GetSpec() const
    {
        return Spec;
    }

    void SetSpec(TPooledOperationSpecPtr newSpec)
    {
        Spec = newSpec;
    }


    TPoolPtr GetPool() const
    {
        return Pool;
    }

    void SetPool(TPoolPtr pool)
    {
        Pool = pool;
    }

private:
    TOperationPtr Operation;
    TPooledOperationSpecPtr Spec;
    TPoolPtr Pool;

};

////////////////////////////////////////////////////////////////////

class TCompositeSchedulerElement
    : public virtual ISchedulerElement
{
public:
    virtual void ScheduleJobs(ISchedulingContext* context) override
    {
        auto rankedChildren = Ranking->GetRankedElements();
        FOREACH (auto child, rankedChildren) {
            if (!context->HasSpareResources()) {
                break;
            }
            child->ScheduleJobs(context);
        }
    }

    void AddChild(ISchedulableElementPtr child)
    {
        Ranking->AddElement(child);
    }

    void RemoveChild(ISchedulableElementPtr child)
    {
        Ranking->RemoveElement(child);
    }

protected:
    void SetRanking(TAutoPtr<IElementRanking> newRanking)
    {
        // Just a precaution.
        if (Ranking == newRanking)
            return;

        // If another ranking is already set then copy its elements to the new one.
        if (~Ranking) {
            const auto& elements = Ranking->GetRankedElements();
            FOREACH (auto element, elements) {
                newRanking->AddElement(element);
            }
        }

        Ranking = newRanking;
    }

private:
    TAutoPtr<IElementRanking> Ranking;

};

////////////////////////////////////////////////////////////////////

class TFifoRanking
    : public IElementRanking
{
public:
    virtual void AddElement(ISchedulableElementPtr element) override
    {
        YCHECK(Elements.insert(element).second);
        CachedRankedElements.clear();
    }

    virtual void RemoveElement(ISchedulableElementPtr element) override
    {
        YCHECK(Elements.erase(element) == 1);
        CachedRankedElements.clear();
    }

    virtual const std::vector<ISchedulableElementPtr>& GetRankedElements() override
    {
        if (!CachedRankedElements.empty()) {
            return CachedRankedElements;
        }

        FOREACH (auto element, Elements) {
            CachedRankedElements.push_back(element);
        }

        // Sort by weight (desc), then by start time (asc).
        std::sort(
            CachedRankedElements.begin(),
            CachedRankedElements.end(),
            [] (const ISchedulableElementPtr& lhs, const ISchedulableElementPtr& rhs) -> bool {
                if (lhs->GetWeight() > rhs->GetWeight()) {
                    return true;
                }
                if (lhs->GetWeight() < rhs->GetWeight()) {
                    return false;
                }
                return lhs->GetStartTime() < rhs->GetStartTime();
            });

        return CachedRankedElements;
    }

private:
    yhash_set<ISchedulableElementPtr> Elements;
    std::vector<ISchedulableElementPtr> CachedRankedElements;

};

////////////////////////////////////////////////////////////////////

class TFairShareRanking
    : public IElementRanking
{
public:
    virtual void AddElement(ISchedulableElementPtr element) override
    {
    }

    virtual void RemoveElement(ISchedulableElementPtr element) override
    {
    }

    virtual const std::vector<ISchedulableElementPtr>& GetRankedElements() override
    {
        return CachedRankedElements;
    }

private:
    std::vector<ISchedulableElementPtr> CachedRankedElements;

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

        switch (Config->Mode) {
            case EPoolMode::Fifo:
                SetRanking(new TFifoRanking());
                break;

            case EPoolMode::FairShare:
                SetRanking(new TFairShareRanking());
                break;

            default:
                YUNREACHABLE();
        }
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

    virtual int GetWeight() const override
    {
        return Config->Weight;
    }

    virtual double GetMinShare() const override
    {
        return Config->MinShare;
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
        SetRanking(new TFifoRanking());
    }
};

////////////////////////////////////////////////////////////////////

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
        return RootElement->ScheduleJobs(context);
    }

private:
    ISchedulerStrategyHost* Host;

    yhash_map<Stroka, TPoolPtr> IdToPool;
    TPoolPtr DefaultPool;

    yhash_map<TOperationPtr, TOperationElementPtr> OperationToElement;

    TRootElementPtr RootElement;


    void OnOperationStarted(TOperationPtr operation)
    {
        auto element = New<TOperationElement>(operation);
        YCHECK(OperationToElement.insert(std::make_pair(operation, element)).second);

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
                LOG_ERROR("Invalid pool %s for operation %s, using %s",
                    ~spec->Pool.Get().Quote(),
                    ~ToString(operation->GetOperationId()),
                    ~DefaultPool->GetId().Quote());
            }
        }
        if (!pool) {
            pool = DefaultPool;
        }

        element->SetSpec(spec);
        element->SetPool(pool);

        pool->AddChild(element);

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

