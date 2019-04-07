#pragma once

#include "private.h"

#include "host.h"

#include <yt/ytlib/api/public.h>
#include <yt/ytlib/api/native/public.h>
#include <yt/ytlib/monitoring/public.h>

#include <yt/core/actions/public.h>
#include <yt/core/bus/public.h>
#include <yt/core/concurrency/public.h>
#include <yt/core/misc/public.h>
#include <yt/core/rpc/public.h>
#include <yt/core/ytree/public.h>
#include <yt/core/http/public.h>

#include <util/generic/string.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
private:
    const TClickHouseServerBootstrapConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;
    TString InstanceId_;
    TString CliqueId_;
    ui16 RpcPort_;
    ui16 MonitoringPort_;
    ui16 TcpPort_;
    ui16 HttpPort_;

    NConcurrency::TActionQueuePtr ControlQueue_;

    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;
    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    ICoreDumperPtr CoreDumper_;

    NApi::NNative::IConnectionPtr Connection_;
    NApi::NNative::TClientCachePtr ClientCache_;
    NApi::NNative::IClientPtr RootClient_;

    ICoordinationServicePtr CoordinationService;
    TClickHouseHostPtr ClickHouseHost_;

    NConcurrency::TThreadPoolPtr WorkerThreadPool_;
    IInvokerPtr WorkerInvoker_;
    IInvokerPtr SerializedWorkerInvoker_;

public:
    TBootstrap(
        TClickHouseServerBootstrapConfigPtr config,
        NYTree::INodePtr configNode,
        TString instanceId,
        TString cliqueId,
        ui16 rpcPort,
        ui16 monitoringPort,
        ui16 tcpPort,
        ui16 httpPort);

    void Run();

    const TClickHouseServerBootstrapConfigPtr& GetConfig() const;
    const IInvokerPtr& GetControlInvoker() const;
    const IInvokerPtr& GetWorkerInvoker() const;
    const IInvokerPtr& GetSerializedWorkerInvoker() const;
    const NApi::NNative::IConnectionPtr& GetConnection() const;
    const NApi::NNative::TClientCachePtr& GetClientCache() const;
    const NApi::NNative::IClientPtr& GetRootClient() const;
    const TClickHouseHostPtr& GetHost() const;

private:
    void DoRun();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
