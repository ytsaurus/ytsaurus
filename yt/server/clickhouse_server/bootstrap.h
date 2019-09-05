#pragma once

#include "private.h"

#include "host.h"

#include <yt/ytlib/monitoring/public.h>

#include <yt/ytlib/api/public.h>
#include <yt/ytlib/api/native/public.h>

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
public:
    DEFINE_BYVAL_RO_PROPERTY(TClickHouseServerBootstrapConfigPtr, Config);
    DEFINE_BYVAL_RO_PROPERTY(NApi::NNative::IClientPtr, RootClient)
    DEFINE_BYVAL_RO_PROPERTY(TClickHouseHostPtr, Host);
    DEFINE_BYVAL_RO_PROPERTY(NApi::NNative::IConnectionPtr, Connection);
    DEFINE_BYVAL_RO_PROPERTY(NApi::NNative::TClientCachePtr, ClientCache);
    DEFINE_BYVAL_RO_PROPERTY(IInvokerPtr, WorkerInvoker);
    DEFINE_BYVAL_RO_PROPERTY(IInvokerPtr, SerializedWorkerInvoker);
    DEFINE_BYVAL_RO_PROPERTY(TQueryRegistryPtr, QueryRegistry);
    DEFINE_BYVAL_RO_PROPERTY(TString, CliqueId);

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

    const IInvokerPtr& GetControlInvoker() const;

    EInstanceState GetState() const;

private:
    const NYTree::INodePtr ConfigNode_;
    TString InstanceId_;
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

    NConcurrency::TThreadPoolPtr WorkerThreadPool_;

    std::atomic<int> SigintCounter_ = {0};

    static constexpr int InterruptionExitCode = 0;

    void DoRun();

    void SigintHandler();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
