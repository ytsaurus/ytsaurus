#include "ticket_authenticator.h"
#include "blackbox_service.h"
#include "tvm_service.h"
#include "helpers.h"
#include "config.h"
#include "private.h"

#include <yt/core/rpc/authenticator.h>

namespace NYT::NAuth {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

class TBlackboxTicketAuthenticator
    : public ITicketAuthenticator
{
public:
    TBlackboxTicketAuthenticator(
        TBlackboxTicketAuthenticatorConfigPtr config,
        IBlackboxServicePtr blackboxService,
        ITvmServicePtr tvmService)
        : Config_(std::move(config))
        , BlackboxService_(std::move(blackboxService))
        , TvmService_(std::move(tvmService))
    { }

    virtual TFuture<TAuthenticationResult> Authenticate(
        const TTicketCredentials& credentials) override
    {
        return TvmService_->GetTicket(Config_->BlackboxServiceId)
            .Apply(BIND(
                &TBlackboxTicketAuthenticator::OnTvmCallResult,
                MakeStrong(this),
                credentials));
    }

private:
    const TBlackboxTicketAuthenticatorConfigPtr Config_;
    const IBlackboxServicePtr BlackboxService_;
    const ITvmServicePtr TvmService_;

private:
    TFuture<TAuthenticationResult> OnTvmCallResult(const TTicketCredentials& credentials, const TString& blackboxTicket)
    {
        const auto& ticket = credentials.Ticket;
        auto ticketHash = GetCryptoHash(ticket);

        YT_LOG_DEBUG("Validating ticket via Blackbox (TicketHash: %v)",
            ticketHash);

        return BlackboxService_->Call(
            "user_ticket",
            {{"user_ticket", ticket}},
            {{"X-Ya-Service-Ticket", blackboxTicket}})
            .Apply(BIND(
                &TBlackboxTicketAuthenticator::OnBlackboxCallResult,
                MakeStrong(this),
                ticketHash));
    }

    TAuthenticationResult OnBlackboxCallResult(const TString& ticketHash, const INodePtr& data)
    {
        auto result = OnCallResultImpl(data);
        if (!result.IsOK()) {
            YT_LOG_DEBUG(result, "Blackbox authentication failed (TicketHash: %v)",
                ticketHash);
            THROW_ERROR result
                << TErrorAttribute("ticket_hash", ticketHash);
        }

        YT_LOG_DEBUG("Blackbox authentication successful (TicketHash: %v, Login: %v, Realm: %v)",
            ticketHash,
            result.Value().Login,
            result.Value().Realm);
        return result.Value();
    }

    TErrorOr<TAuthenticationResult> OnCallResultImpl(const INodePtr& data)
    {
        static const TString ErrorPath("/error");
        auto errorNode = FindNodeByYPath(data, ErrorPath);
        if (errorNode) {
            return TError(errorNode->GetValue<TString>());
        }

        static const TString LoginPath("/users/0/login");
        auto loginNode = GetNodeByYPath(data, LoginPath);

        TAuthenticationResult result;
        result.Login = loginNode->GetValue<TString>();
        result.Realm = "blackbox:user-ticket";
        return result;
    }
};

ITicketAuthenticatorPtr CreateBlackboxTicketAuthenticator(
    TBlackboxTicketAuthenticatorConfigPtr config,
    IBlackboxServicePtr blackboxService,
    ITvmServicePtr tvmService)
{
    return New<TBlackboxTicketAuthenticator>(
        std::move(config),
        std::move(blackboxService),
        std::move(tvmService));
}

////////////////////////////////////////////////////////////////////////////////

class TTicketAuthenticatorWrapper
    : public NRpc::IAuthenticator
{
public:
    explicit TTicketAuthenticatorWrapper(ITicketAuthenticatorPtr underlying)
        : Underlying_(std::move(underlying))
    { }

    virtual TFuture<NRpc::TAuthenticationResult> Authenticate(
        const NRpc::TAuthenticationContext& context) override
    {
        if (!context.Header->HasExtension(NRpc::NProto::TCredentialsExt::credentials_ext)) {
            return std::nullopt;
        }

        const auto& ext = context.Header->GetExtension(NRpc::NProto::TCredentialsExt::credentials_ext);
        if (!ext.has_user_ticket()) {
            return std::nullopt;
        }

        TTicketCredentials credentials;
        credentials.Ticket = ext.user_ticket();
        return Underlying_->Authenticate(credentials).Apply(
            BIND([=] (const TAuthenticationResult& authResult) {
                NRpc::TAuthenticationResult rpcResult;
                rpcResult.User = authResult.Login;
                rpcResult.Realm = authResult.Realm;
                return rpcResult;
            }));
    }
private:
    const ITicketAuthenticatorPtr Underlying_;
};

NRpc::IAuthenticatorPtr CreateTicketAuthenticatorWrapper(ITicketAuthenticatorPtr underlying)
{
    return New<TTicketAuthenticatorWrapper>(std::move(underlying));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
