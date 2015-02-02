#pragma once

#include "public.h"

#include <core/concurrency/action_queue.h>

#include <core/bus/public.h>

#include <core/rpc/public.h>

#include <ytlib/monitoring/http_server.h>

#include <ytlib/api/public.h>

#include <ytlib/hive/public.h>

#include <ytlib/transaction_client/public.h>

#include <server/scheduler/public.h>


namespace NYT {
namespace NCellScheduler {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EControlQueue,
    (Default)
    (Heartbeat)
);

class TBootstrap
{
public:
    TBootstrap(
        const Stroka& configFileName,
        TCellSchedulerConfigPtr config);
    ~TBootstrap();

    TCellSchedulerConfigPtr GetConfig() const;
    NApi::IClientPtr GetMasterClient() const;
    const Stroka& GetLocalAddress() const;
    IInvokerPtr GetControlInvoker(EControlQueue queue = EControlQueue::Default) const;
    NScheduler::TSchedulerPtr GetScheduler() const;
    NHive::TClusterDirectoryPtr GetClusterDirectory() const;
    NRpc::IResponseKeeperPtr GetResponseKeeper() const;
    NConcurrency::IThroughputThrottlerPtr GetChunkLocationThrottler() const;

    void Run();

private:
    Stroka ConfigFileName_;
    TCellSchedulerConfigPtr Config_;

    NConcurrency::TFairShareActionQueuePtr ControlQueue_;
    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    std::unique_ptr<NHttp::TServer> HttpServer_;
    NApi::IClientPtr MasterClient_;
    Stroka LocalAddress_;
    NScheduler::TSchedulerPtr Scheduler_;
    NHive::TClusterDirectoryPtr ClusterDirectory_;
    NRpc::IResponseKeeperPtr ResponseKeeper_;
    NConcurrency::IThroughputThrottlerPtr ChunkLocationThrottler_;

    void DoRun();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellScheduler
} // namespace NYT
