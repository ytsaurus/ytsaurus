#include "embedded_api_service.h"

#include "ban_service.h"
#include "bootstrap.h"
#include "dynamic_config_manager.h"
#include "private.h"

#include <yt/yt/server/lib/cypress_proxy/config.h>

#include <yt/yt/server/lib/rpc_proxy/access_checker.h>
#include <yt/yt/server/lib/rpc_proxy/api_service.h>
#include <yt/yt/server/lib/rpc_proxy/proxy_coordinator.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/library/tracing/jaeger/sampler.h>

namespace NYT::NCypressProxy {

using namespace NRpc;
using namespace NRpcProxy;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TEmbeddedApiAccessChecker
    : public IAccessChecker
{
public:
    explicit TEmbeddedApiAccessChecker(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    TError CheckAccess(const std::string& user) const override
    {
        if (Bootstrap_->GetBanService()->IsBanned(user)) {
            return TError(
                NSecurityClient::EErrorCode::UserBanned,
                "User %Qv is banned",
                user);
        }
        return {};
    }

private:
    IBootstrap* const Bootstrap_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IApiServicePtr CreateEmbeddedApiService(
    IBootstrap* bootstrap,
    IProxyCoordinatorPtr proxyCoordinator,
    IChannelPtr localChannel)
{
    auto invoker = bootstrap->GetInvoker("ApiService");
    auto service = CreateMasterMetadataApiService(
        bootstrap->GetConfig()->ApiService,
        invoker,
        [invoker] (const std::string& /*pool*/, const std::string& /*executionTag*/) {
            return invoker;
        },
        bootstrap->GetNativeConnection(),
        bootstrap->GetNativeAuthenticator(),
        std::move(proxyCoordinator),
        New<TEmbeddedApiAccessChecker>(bootstrap),
        New<NTracing::TSampler>(),
        CypressProxyLogger(),
        CypressProxyProfiler().WithPrefix("/api_service"),
        /*memoryUsageTracker*/ nullptr,
        /*stickyTransactionPool*/ nullptr,
        std::move(localChannel));

    const auto& dynamicConfig = bootstrap->GetDynamicConfigManager()->GetConfig();
    service->OnDynamicConfigChanged(dynamicConfig->ApiService);
    return service;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
