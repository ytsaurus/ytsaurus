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

        IBlackboxServicePtr blackboxService;
        if (config->BlackboxService && invoker) {
            blackboxService = CreateDefaultBlackboxService(
                config->BlackboxService,
                invoker);
        }

        if (config->TvmService) {
            TvmService_ = CreateCachingTvmService(
                CreateDefaultTvmService(
                    config->TvmService,
                    invoker),
                config->TvmService);
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

        if (config->BlackboxCookieAuthenticator && blackboxService) {
            CookieAuthenticator_ = CreateCachingCookieAuthenticator(
                config->BlackboxCookieAuthenticator,
                CreateBlackboxCookieAuthenticator(
                    config->BlackboxCookieAuthenticator,
                    blackboxService));
            rpcAuthenticators.push_back(
                CreateCookieAuthenticatorWrapper(CookieAuthenticator_));
        }

        if (blackboxService && TvmService_  && config->BlackboxTicketAuthenticator) {
            TicketAuthenticator_ = CreateBlackboxTicketAuthenticator(
                config->BlackboxTicketAuthenticator,
                blackboxService,
                TvmService_ );
            rpcAuthenticators.push_back(
                CreateTicketAuthenticatorWrapper(TicketAuthenticator_));
        }

        if (!tokenAuthenticators.empty()) {
            rpcAuthenticators.push_back(CreateTokenAuthenticatorWrapper(
                CreateCompositeTokenAuthenticator(tokenAuthenticators)));
        }

        if (!config->RequireAuthentication) {
            tokenAuthenticators.push_back(CreateNoopTokenAuthenticator());
        }
        TokenAuthenticator_ = CreateCompositeTokenAuthenticator(tokenAuthenticators);

        if (!config->RequireAuthentication) {
            rpcAuthenticators.push_back(NRpc::CreateNoopAuthenticator());
        }
        RpcAuthenticator_ = CreateCompositeAuthenticator(std::move(rpcAuthenticators));
    }
    
    const NRpc::IAuthenticatorPtr& GetRpcAuthenticator() const
    {
        return RpcAuthenticator_;
    }

    const ITokenAuthenticatorPtr& GetTokenAuthenticator() const
    {
        return TokenAuthenticator_;
    }

    const ICookieAuthenticatorPtr& GetCookieAuthenticator() const
    {
        return CookieAuthenticator_;
    }

    const ITicketAuthenticatorPtr& GetTicketAuthenticator() const
    {
        return TicketAuthenticator_;
    }

    const ITvmServicePtr& GetTvmService() const
    {
        return TvmService_;
    }

private:
    ITvmServicePtr TvmService_;
    NRpc::IAuthenticatorPtr RpcAuthenticator_;
    ITokenAuthenticatorPtr TokenAuthenticator_;
    ICookieAuthenticatorPtr CookieAuthenticator_;
    ITicketAuthenticatorPtr TicketAuthenticator_;
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

const ITokenAuthenticatorPtr& TAuthenticationManager::GetTokenAuthenticator() const
{
    return Impl_->GetTokenAuthenticator();
}

const ICookieAuthenticatorPtr& TAuthenticationManager::GetCookieAuthenticator() const
{
    return Impl_->GetCookieAuthenticator();
}

const ITicketAuthenticatorPtr& TAuthenticationManager::GetTicketAuthenticator() const
{
    return Impl_->GetTicketAuthenticator();
}

const ITvmServicePtr& TAuthenticationManager::GetTvmService() const
{
    return Impl_->GetTvmService();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
