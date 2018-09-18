#pragma once

#include <yt/ytlib/auth/public.h>

#include <yt/ytlib/monitoring/public.h>

#include <yt/core/actions/public.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/http/public.h>

#include <yt/core/ytree/public.h>

#include "public.h"

namespace NYT {
namespace NClickHouseProxy {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(TClickHouseProxyServerConfigPtr config, NYTree::INodePtr configNode);
    ~TBootstrap();

    const TClickHouseProxyServerConfigPtr& GetConfig() const;
    const IInvokerPtr& GetControlInvoker() const;
    const IInvokerPtr& GetWorkerInvoker() const;
    const NAuth::ITokenAuthenticatorPtr& GetTokenAuthenticator() const;

    void Run();

private:
    const TClickHouseProxyServerConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;

    const NConcurrency::TActionQueuePtr ControlQueue_;
    const NConcurrency::TThreadPoolPtr WorkerPool_;

    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    std::unique_ptr<NLFAlloc::TLFAllocProfiler> LFAllocProfiler_;
    NHttp::IServerPtr MonitoringHttpServer_;
    NHttp::IServerPtr ClickHouseProxyServer_;
    TClickHouseProxyHandlerPtr ClickHouseProxy_;

    NAuth::TAuthenticationManagerPtr AuthenticationManager_;
    NAuth::ITokenAuthenticatorPtr TokenAuthenticator_;

    void DoRun();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NClickHouseProxy
} // namespace NYT
