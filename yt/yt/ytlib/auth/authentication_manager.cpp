#include "authentication_manager.h"
#include "cookie_authenticator.h"
#include "default_blackbox_service.h"
#include "default_tvm_service.h"
#include "ticket_authenticator.h"
#include "token_authenticator.h"

#include <yt/yt/ytlib/auth/config.h>

#include <yt/yt/core/rpc/authenticator.h>

namespace NYT::NAuth {

using namespace NApi;
using namespace NRpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TAuthenticationManager::TImpl
{
public:
    TImpl(
        TAuthenticationManagerConfigPtr config,
        IPollerPtr poller,
        IClientPtr client,
        NProfiling::TProfiler profiler)
    {
        std::vector<NRpc::IAuthenticatorPtr> rpcAuthenticators;
        std::vector<NAuth::ITokenAuthenticatorPtr> tokenAuthenticators;

        if (config->TvmService && poller) {
            TvmService_ = CreateDefaultTvmService(
                config->TvmService,
                poller,
                profiler.WithPrefix("/tvm/remote"));
        }

        IBlackboxServicePtr blackboxService;
        if (config->BlackboxService && poller) {
            blackboxService = CreateDefaultBlackboxService(
                config->BlackboxService,
                TvmService_,
                poller,
                profiler.WithPrefix("/blackbox"));
        }

        if (config->BlackboxTokenAuthenticator && blackboxService) {
            tokenAuthenticators.push_back(
                CreateCachingTokenAuthenticator(
                    config->BlackboxTokenAuthenticator,
                    CreateBlackboxTokenAuthenticator(
                        config->BlackboxTokenAuthenticator,
                        blackboxService,
                        profiler.WithPrefix("/blackbox_token_authenticator/remote")),
                    profiler.WithPrefix("/blackbox_token_authenticator/cache")));
        }

        if (config->CypressTokenAuthenticator && client) {
            tokenAuthenticators.push_back(
                CreateCachingTokenAuthenticator(
                    config->CypressTokenAuthenticator,
                    CreateCypressTokenAuthenticator(
                        config->CypressTokenAuthenticator,
                        client),
                    profiler.WithPrefix("/cypress_token_authenticator/cache")));
        }

        if (config->BlackboxCookieAuthenticator && blackboxService) {
            CookieAuthenticator_ = CreateCachingCookieAuthenticator(
                config->BlackboxCookieAuthenticator,
                CreateBlackboxCookieAuthenticator(
                    config->BlackboxCookieAuthenticator,
                    blackboxService),
                profiler.WithPrefix("/blackbox_cookie_authenticator/cache"));
            rpcAuthenticators.push_back(
                CreateCookieAuthenticatorWrapper(CookieAuthenticator_));
        }

        if (blackboxService && config->BlackboxTicketAuthenticator) {
            TicketAuthenticator_ = CreateBlackboxTicketAuthenticator(
                config->BlackboxTicketAuthenticator,
                blackboxService,
                TvmService_);
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
    IPollerPtr poller,
    IClientPtr client,
    NProfiling::TProfiler profiler)
    : Impl_(std::make_unique<TImpl>(
        std::move(config),
        std::move(poller),
        std::move(client),
        std::move(profiler)))
{ }

TAuthenticationManager::~TAuthenticationManager()
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

} // namespace NYT::NAuth
