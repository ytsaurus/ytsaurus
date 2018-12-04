#pragma once

#include "public.h"

#include <yt/server/rpc_proxy/public.h>

#include <yt/ytlib/auth/public.h>

#include <yt/ytlib/api/public.h>
#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/driver/public.h>

#include <yt/ytlib/monitoring/public.h>

#include <yt/core/bus/public.h>

#include <yt/core/actions/public.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/misc/public.h>

#include <yt/core/rpc/public.h>

#include <yt/core/http/public.h>

#include <yt/core/http/http.h>

#include <yt/core/https/public.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public NHttp::IHttpHandler
{
public:
    TBootstrap(TProxyConfigPtr config, NYTree::INodePtr configNode);
    ~TBootstrap();

    void Run();

    const IInvokerPtr& GetControlInvoker() const;

    const TProxyConfigPtr& GetConfig() const;
    const NApi::IClientPtr& GetRootClient() const;
    const NDriver::IDriverPtr& GetDriverV3() const;
    const NDriver::IDriverPtr& GetDriverV4() const;
    const TCoordinatorPtr& GetCoordinator() const;
    const THttpAuthenticatorPtr& GetHttpAuthenticator() const;

    virtual void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) override;

private:
    const TProxyConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;
    const TInstant StartTime_ = TInstant::Now();

    NConcurrency::TActionQueuePtr Control_;
    NConcurrency::IPollerPtr Poller_;

    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    NHttp::IServerPtr MonitoringServer_;

    NApi::NNative::IConnectionPtr Connection_;
    NApi::IClientPtr Client_;
    NDriver::IDriverPtr DriverV3_;
    NDriver::IDriverPtr DriverV4_;

    NConcurrency::TThreadPoolPtr BlackboxThreadPool_;
    NAuth::ITokenAuthenticatorPtr TokenAuthenticator_;
    NAuth::ICookieAuthenticatorPtr CookieAuthenticator_;
    THttpAuthenticatorPtr HttpAuthenticator_;

    NHttp::IServerPtr ApiHttpServer_;
    NHttp::IServerPtr ApiHttpsServer_;
    TApiPtr Api_;

    TCoordinatorPtr Coordinator_;
    THostsHandlerPtr HostsHandler_;
    TPingHandlerPtr PingHandler_;
    TDiscoverVersionsHandlerPtr DiscoverVersionsHandler_;

    void RegisterRoutes(const NHttp::IServerPtr& server);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttpProxy
} // namespace NYT
