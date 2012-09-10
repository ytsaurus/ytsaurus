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

struct TPoolConfig
    : public TYsonSerializable
{

};

typedef TIntrusivePtr<TPoolConfig> TPoolConfigPtr;

////////////////////////////////////////////////////////////////////

class TPool
    : public TRefCounted
{
public:
    explicit TPool(const Stroka& id)
        : Id(id)
        , Config(New<TPoolConfig>())
    { }

    const Stroka& GetId() const
    {
        return Id;
    }

    TPoolConfigPtr GetConfig()
    {
        return Config;
    }

private:
    Stroka Id;
    TPoolConfigPtr Config;

};

typedef TIntrusivePtr<TPool> TPoolPtr;

////////////////////////////////////////////////////////////////////

class TFairShareStrategy
    : public ISchedulerStrategy
{
public:
    explicit TFairShareStrategy(ISchedulerStrategyHost* host)
    {
        host->SubscribeOperationStarted(BIND(&TFairShareStrategy::OnOperationStarted, this));
        host->SubscribeOperationFinished(BIND(&TFairShareStrategy::OnOperationFinished, this));

        auto* masterConnector = host->GetMasterConnector();
        masterConnector->SubscribeWatcherRequest(BIND(&TFairShareStrategy::OnPoolsRequest, this));
        masterConnector->SubscribeWatcherResponse(BIND(&TFairShareStrategy::OnPoolsResponse, this));

        InitDefaultPool();
    }

    virtual void ScheduleJobs(ISchedulingContext* context) override
    {
    }

private:
    yhash_map<Stroka, TPoolPtr> Pools;
    TPoolPtr DefaultPool;


    void OnOperationStarted(TOperationPtr operation)
    {
    }

    void OnOperationFinished(TOperationPtr operation)
    {
    }


    void InitDefaultPool()
    {
        DefaultPool = New<TPool>(DefaultPoolId);
        RegisterPool(DefaultPool);
    }

    void RegisterPool(TPoolPtr pool)
    {
        LOG_INFO("Pool registered: %s", ~pool->GetId());
        YCHECK(Pools.insert(std::make_pair(pool->GetId(), pool)).second);
    }

    void UnregisterPool(TPoolPtr pool)
    {
        LOG_INFO("Pool unregistered: %s", ~pool->GetId());
        YCHECK(Pools.erase(pool->GetId()) == 1);
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

        // Build the set of potential orphans (skipping the default pool).
        yhash_set<Stroka> orphanPoolIds;
        FOREACH (const auto& pair, Pools) {
            const auto& id = pair.first;
            const auto& pool = pair.second;
            if (pool != DefaultPool) {
                YCHECK(orphanPoolIds.insert(pair.first).second);
            }
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
                existingPool->GetConfig()->Load(configNode);
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
            UnregisterPool(pool);
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

