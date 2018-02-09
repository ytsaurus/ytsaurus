#pragma once

#include "public.h"

#include <yt/server/blackbox/public.h>

#include <yt/server/rpc_proxy/public.h>

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/monitoring/public.h>

#include <yt/core/bus/public.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/misc/public.h>

#include <yt/core/rpc/public.h>

#include <yt/core/http/public.h>

namespace NYT {
namespace NCellProxy {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(TCellProxyConfigPtr config, NYTree::INodePtr configNode);
    ~TBootstrap();

    const TCellProxyConfigPtr& GetConfig() const;
    const IInvokerPtr& GetControlInvoker() const;
    const NApi::INativeConnectionPtr& GetNativeConnection() const;
    const NApi::INativeClientPtr& GetNativeClient() const;
    const NBlackbox::ITokenAuthenticatorPtr& GetTokenAuthenticator() const;
    const NBlackbox::ICookieAuthenticatorPtr& GetCookieAuthenticator() const;
    const NRpcProxy::IProxyCoordinatorPtr& GetProxyCoordinator() const;
    const NNodeTrackerClient::TAddressMap& GetLocalAddresses() const;

    void Run();

private:
    const TCellProxyConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;

    NConcurrency::TActionQueuePtr ControlQueue_;
    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    std::unique_ptr<NLFAlloc::TLFAllocProfiler> LFAllocProfiler_;
    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;
    TCoreDumperPtr CoreDumper_;

    NApi::INativeConnectionPtr NativeConnection_;
    NApi::INativeClientPtr NativeClient_;
    NBlackbox::ITokenAuthenticatorPtr TokenAuthenticator_;
    NBlackbox::ICookieAuthenticatorPtr CookieAuthenticator_;
    NRpcProxy::IProxyCoordinatorPtr ProxyCoordinator_;
    NNodeTrackerClient::TAddressMap LocalAddresses_;

    void DoRun();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellProxy
} // namespace NYT
