#pragma once

#include "private.h"

#include <yt/ytlib/monitoring/public.h>

#include <yt/core/bus/public.h>
#include <yt/core/rpc/public.h>
#include <yt/core/http/public.h>

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

    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;
    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    ICoreDumperPtr CoreDumper_;

    std::atomic<int> SigintCounter_ = {0};

    static constexpr int InterruptionExitCode = 0;

    void DoRun();

    void HandleSigint();
    void HandleCrashSignal();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
