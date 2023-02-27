#pragma once

#include "private.h"

#include <yt/yt/library/monitoring/public.h>

#include <yt/yt/library/coredumper/public.h>

#include <yt/yt/core/bus/public.h>
#include <yt/yt/core/rpc/public.h>
#include <yt/yt/core/http/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(
        TClickHouseServerBootstrapConfigPtr config,
        NYTree::INodePtr configNode);

    void Run();

    const IInvokerPtr& GetControlInvoker() const;

private:
    TClickHouseServerBootstrapConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;
    THostPtr Host_;
    IClickHouseServerPtr ClickHouseServer_;

    NConcurrency::TActionQueuePtr ControlQueue_;
    NConcurrency::IThreadPoolPtr RpcQueryServiceThreadPool_;

    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;
    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    NCoreDump::ICoreDumperPtr CoreDumper_;

    std::atomic<int> SigintCounter_ = {0};

    void DoRun();

    void HandleSigint();
    void HandleCrashSignal();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
