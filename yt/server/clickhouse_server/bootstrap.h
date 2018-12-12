#pragma once

#include <yt/server/clickhouse_server/native/public.h>

#include <yt/server/clickhouse_server/engine/server.h>

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
    const NNative::TConfigPtr Config;
    const NYTree::INodePtr ConfigNode;
    TString InstanceId_;
    TString CliqueId_;
    ui16 RpcPort_;
    ui16 MonitoringPort_;
    ui16 TcpPort_;
    ui16 HttpPort_;

    NConcurrency::TActionQueuePtr ControlQueue;

    NBus::IBusServerPtr BusServer;
    NRpc::IServerPtr RpcServer;
    NHttp::IServerPtr HttpServer;
    NMonitoring::TMonitoringManagerPtr MonitoringManager;
    ICoreDumperPtr CoreDumper;

    NApi::NNative::IConnectionPtr Connection;
    NNative::INativeClientCachePtr NativeClientCache;
    NConcurrency::IThroughputThrottlerPtr ScanThrottler;

    NNative::IStoragePtr Storage;
    NNative::ICoordinationServicePtr CoordinationService;
    NNative::ICliqueAuthorizationManagerPtr CliqueAuthorizationManager;
    std::unique_ptr<NEngine::TServer> Server;

public:
    TBootstrap(
        NNative::TConfigPtr config,
        NYTree::INodePtr configNode,
        TString instanceId,
        TString cliqueId,
        ui16 rpcPort,
        ui16 monitoringPort,
        ui16 tcpPort,
        ui16 httpPort);
    ~TBootstrap();

    void Initialize();
    void Run();

    NNative::TConfigPtr GetConfig() const;
    IInvokerPtr GetControlInvoker() const;
    NApi::NNative::IConnectionPtr GetConnection() const;
    NConcurrency::IThroughputThrottlerPtr GetScanThrottler() const;

private:
    void DoInitialize();
    void DoRun();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
