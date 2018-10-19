#include "authentication_manager.h"
#include "token_authenticator.h"
#include "ticket_authenticator.h"
#include "cookie_authenticator.h"
#include "default_blackbox_service.h"
#include "caching_tvm_service.h"
#include "default_tvm_service.h"

#include <yt/ytlib/auth/config.h>

#include <yt/core/rpc/authenticator.h>

namespace NYT {
namespace NAuth {

using namespace NApi;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

std::tuple<ITokenAuthenticatorPtr, ICookieAuthenticatorPtr> CreateAuthenticators(
    TAuthenticationManagerConfigPtr config,
    IInvokerPtr invoker,
    NApi::IClientPtr client)
{
    NAuth::ICookieAuthenticatorPtr cookieAuthenticator;
    NAuth::ITokenAuthenticatorPtr tokenAuthenticator;
    std::vector<NAuth::ITokenAuthenticatorPtr> tokenAuthenticators;

    IBlackboxServicePtr blackboxService;
    if (config->BlackboxService && invoker) {
        blackboxService = CreateDefaultBlackboxService(
            config->BlackboxService,
            invoker);
    }

    if (config->BlackboxTokenAuthenticator && blackboxService) {
        tokenAuthenticators.push_back(
            CreateCachingTokenAuthenticator(
                config->BlackboxTokenAuthenticator,
                CreateBlackboxTokenAuthenticator(
                    config->BlackboxTokenAuthenticator,
                    blackboxService)));
    }

    if (config->CypressTokenAuthenticator && client) {
        tokenAuthenticators.push_back(
            CreateCachingTokenAuthenticator(
                config->CypressTokenAuthenticator,
                CreateCypressTokenAuthenticator(
                    config->CypressTokenAuthenticator,
                    client)));
    }

    if (!tokenAuthenticators.empty()) {
        tokenAuthenticator = CreateCompositeTokenAuthenticator(std::move(tokenAuthenticators));
    }

    if (config->BlackboxCookieAuthenticator && blackboxService) {
        cookieAuthenticator = CreateCachingCookieAuthenticator(
            config->BlackboxCookieAuthenticator,
            CreateBlackboxCookieAuthenticator(
                config->BlackboxCookieAuthenticator,
                std::move(blackboxService)));
    }

    return std::make_pair(tokenAuthenticator, cookieAuthenticator);
}

////////////////////////////////////////////////////////////////////////////////

class TAuthenticationManager::TImpl
{
public:
    TImpl(
        TAuthenticationManagerConfigPtr config,
        IInvokerPtr invoker,
        IClientPtr client)
    {
        std::vector<NRpc::IAuthenticatorPtr> rpcAuthenticators;
        std::vector<NAuth::ITokenAuthenticatorPtr> tokenAuthenticators;

        auto authenticators = CreateAuthenticators(config, invoker, client);
        if (std::get<0>(authenticators)) {
            rpcAuthenticators.push_back(CreateTokenAuthenticatorWrapper(std::get<0>(authenticators)));
        }
        if (std::get<1>(authenticators)) {
            rpcAuthenticators.push_back(CreateCookieAuthenticatorWrapper(std::get<1>(authenticators)));
        }

        ITvmServicePtr tvmService;
        if (config->TvmService) {
            tvmService = CreateCachingTvmService(
                CreateDefaultTvmService(
                    config->TvmService,
                    invoker),
                config->TvmService);
        }

        if (blackboxService && tvmService && config->BlackboxTicketAuthenticator) {
            rpcAuthenticators.push_back(
                CreateTicketAuthenticatorWrapper(
                    CreateBlackboxTicketAuthenticator(
                        config->BlackboxTicketAuthenticator,
                        blackboxService,
                        tvmService)));
        }

        if (!config->RequireAuthentication) {
            rpcAuthenticators.push_back(CreateNoopAuthenticator());
            tokenAuthenticators.push_back(CreateNoopTokenAuthenticator());
        }

        RpcAuthenticator_ = CreateCompositeAuthenticator(std::move(rpcAuthenticators));
        TokenAuthenticator_= CreateCompositeTokenAuthenticator(std::move(tokenAuthenticators));
    }
    
    const NRpc::IAuthenticatorPtr& GetRpcAuthenticator() const
    {
        return RpcAuthenticator_;
    }

    const NAuth::ITokenAuthenticatorPtr& GetTokenAuthenticator() const
    {
        return TokenAuthenticator_;
    }

private:
    NRpc::IAuthenticatorPtr RpcAuthenticator_;
    NAuth::ITokenAuthenticatorPtr TokenAuthenticator_;
};

////////////////////////////////////////////////////////////////////////////////

TAuthenticationManager::TAuthenticationManager(
    TAuthenticationManagerConfigPtr config,
    IInvokerPtr invoker,
    IClientPtr client)
    : Impl_(std::make_unique<TImpl>(
        std::move(config),
        std::move(invoker),
        std::move(client)))
{ }

const NRpc::IAuthenticatorPtr& TAuthenticationManager::GetRpcAuthenticator() const
{
    return Impl_->GetRpcAuthenticator();
}

const NAuth::ITokenAuthenticatorPtr& TAuthenticationManager::GetTokenAuthenticator() const
{
    return Impl_->GetTokenAuthenticator();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
