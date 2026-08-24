#include "ticket_authenticator.h"

#include "auth_cache.h"
#include "blackbox_service.h"
#include "config.h"
#include "credentials.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/core/rpc/authenticator.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/library/tvm/service/tvm_service.h>

#include <util/digest/multi.h>

namespace NYT::NAuth {

using namespace NYTree;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = AuthLogger;

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

    TFuture<TAuthenticationResult> Authenticate(
        const TUserTicketCredentials& credentials) override
    {
        const auto& userTicket = credentials.UserTicket;
        auto userTicketHash = GetCryptoHash(userTicket);
        auto userTicketAuthenticationConfig = UserTicketAuthenticationConfig_.Acquire();

        YT_TLOG_DEBUG("Validating user ticket")
            .With("UserTicketHash", userTicketHash);

        if (userTicketAuthenticationConfig && userTicketAuthenticationConfig->CheckServiceTickets) {
            if (!credentials.ServiceTicket) {
                return MakeFuture<TAuthenticationResult>(TError(NRpc::EErrorCode::InvalidCredentials,
                    "Service ticket is required to authorize with user ticket"));
            }
            const auto& serviceTicket = *credentials.ServiceTicket;
            auto serviceTicketHash = GetCryptoHash(serviceTicket);

            auto errorOrTvmId = ParseServiceTicket(serviceTicket, serviceTicketHash);
            if (!errorOrTvmId.IsOK()) {
                return MakeFuture<TAuthenticationResult>(TError(errorOrTvmId));
            }

            auto tvmId = errorOrTvmId.Value();
            if (!userTicketAuthenticationConfig->AllowedServiceTvmIds.contains(tvmId)) {
                YT_TLOG_DEBUG("Service is not allowed to authorize using user ticket")
                    .With("UserTicketHash", userTicketHash)
                    .With("ServiceTicketHash", serviceTicketHash)
                    .With("TvmId", tvmId);

                return MakeFuture<TAuthenticationResult>(TError(NRpc::EErrorCode::InvalidCredentials,
                    "Service is not allowed to authorize using user ticket"));
            }
        }

        if (Config_->EnableScopeCheck && TvmService_) {
            auto result = CheckScope(userTicket, userTicketHash);
            if (!result.IsOK()) {
                return MakeFuture<TAuthenticationResult>(result);
            }
        }

        YT_TLOG_DEBUG("Validating user ticket via Blackbox")
            .With("UserTicketHash", userTicketHash);

        return BlackboxService_->Call("user_ticket", {{"user_ticket", userTicket}})
            .Apply(BIND(
                &TBlackboxTicketAuthenticator::OnBlackboxCallResult,
                MakeStrong(this),
                userTicket,
                userTicketHash));
    }

    TFuture<TAuthenticationResult> Authenticate(
        const TServiceTicketCredentials& credentials) override
    {
        const auto& ticket = credentials.ServiceTicket;
        auto ticketHash = GetCryptoHash(ticket);

        YT_TLOG_DEBUG("Validating service ticket")
            .With("ServiceTicketHash", ticketHash);

        auto errorOrTvmId = ParseServiceTicket(ticket, ticketHash);
        if (!errorOrTvmId.IsOK()) {
            return MakeFuture<TAuthenticationResult>(TError(errorOrTvmId));
        }

        TAuthenticationResult result;
        result.Login = GetLoginForTvmId(errorOrTvmId.Value());
        result.Realm = "tvm:service-ticket";

        return MakeFuture(result);
    }

    bool Reconfigure(const TUserTicketAuthenticationConfigPtr& userTicketAuthenticationConfig) override
    {
        auto oldConfig = UserTicketAuthenticationConfig_.Acquire();
        UserTicketAuthenticationConfig_.Store(userTicketAuthenticationConfig);
        return !oldConfig || !AreNodesEqual(
            ConvertToNode(oldConfig),
            ConvertToNode(userTicketAuthenticationConfig));
    }

private:
    const TBlackboxTicketAuthenticatorConfigPtr Config_;
    const IBlackboxServicePtr BlackboxService_;
    const ITvmServicePtr TvmService_;

    TAtomicIntrusivePtr<TUserTicketAuthenticationConfig> UserTicketAuthenticationConfig_;

private:
    TErrorOr<TTvmId> ParseServiceTicket(const std::string& ticket, const std::string& ticketHash) const
    {
        try {
            auto parsedTicket = TvmService_->ParseServiceTicket(ticket);
            YT_TLOG_DEBUG("Parsing service ticket succeeded")
                .With("ServiceTicketHash", ticketHash)
                .With("TvmId", parsedTicket.TvmId);

            return parsedTicket.TvmId;
        } catch (const std::exception& ex) {
            auto error = TError(NRpc::EErrorCode::InvalidCredentials, "Failed to parse service ticket")
                .With(ex);
            YT_TLOG_DEBUG("Parsing service ticket failed")
                .With("ServiceTicketHash", ticketHash)
                .With(error);
            return error;
        }
    }

    TError CheckScope(const std::string& ticket, const std::string& ticketHash)
    {
        YT_TLOG_DEBUG("Validating user ticket scopes")
            .With("UserTicketHash", ticketHash);
        try {
            const auto result = TvmService_->ParseUserTicket(ticket);
            const auto& scopes = result.Scopes;
            YT_TLOG_DEBUG("Got user ticket")
                .With("Scopes", scopes);

            const auto& allowedScopes = Config_->Scopes;
            for (const auto& scope : scopes) {
                if (allowedScopes.contains(scope)) {
                    return TError();
                }
            }

            return TError(NRpc::EErrorCode::InvalidCredentials,
                "Ticket does not provide an allowed scope")
                .With("provided_scopes", scopes)
                .With("allowed_scopes", allowedScopes);
        } catch (const std::exception& ex) {
            TError error(ex);
            YT_TLOG_DEBUG("Parsing user ticket failed")
                .With("UserTicketHash", ticketHash)
                .With(error);
            return error.With("user_ticket_hash", ticketHash);
        }
    }

    TAuthenticationResult OnBlackboxCallResult(
        const std::string& ticket,
        const std::string& ticketHash,
        const INodePtr& data)
    {
        auto errorOrResult = OnCallResultImpl(data);
        if (!errorOrResult.IsOK()) {
            YT_TLOG_DEBUG("Blackbox authentication failed")
                .With("UserTicketHash", ticketHash)
                .With(errorOrResult);
            THROW_ERROR errorOrResult
                .With("user_ticket_hash", ticketHash);
        }

        auto result = errorOrResult.Value();
        result.UserTicket = ticket;

        YT_TLOG_DEBUG("Blackbox authentication successful")
            .With("UserTicketHash", ticketHash)
            .With("Login", result.Login)
            .With("Realm", result.Realm);
        return result;
    }

    TErrorOr<TAuthenticationResult> OnCallResultImpl(const INodePtr& data)
    {
        static const TYPath ErrorPath("/error");
        if (auto errorNode = FindNodeByYPath(data, ErrorPath)) {
            return TError(errorNode->GetValue<std::string>(), TError::DisableFormat);
        }

        static const std::string UserPath("/users/0");
        auto userNode = GetNodeByYPath(data, UserPath);

        auto login = BlackboxService_->GetLogin(userNode);

        // Sanity checks.
        if (!login.IsOK()) {
            return TError("Blackbox returned invalid response")
                .With(login);
        }

        TAuthenticationResult result;
        result.Login = login.Value();
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

struct TTicketAuthenticatorCacheKey
{
    std::optional<std::string> UserTicket;
    std::optional<std::string> ServiceTicket;

    operator size_t() const
    {
        return MultiHash(UserTicket, ServiceTicket);
    }

    bool operator==(const TTicketAuthenticatorCacheKey&) const = default;
};

class TCachingTicketAuthenticator
    : public ITicketAuthenticator
    , public TAuthCache<TTicketAuthenticatorCacheKey, TAuthenticationResult, std::monostate>
{
public:
    TCachingTicketAuthenticator(
        TCachingTicketAuthenticatorConfigPtr config,
        ITicketAuthenticatorPtr underlying,
        TProfiler profiler)
        : TAuthCache(config->Cache, std::move(profiler))
        , Underlying_(std::move(underlying))
    { }

    TFuture<TAuthenticationResult> Authenticate(
        const TUserTicketCredentials& credentials) override
    {
        return Get(
            TTicketAuthenticatorCacheKey{
                .UserTicket = credentials.UserTicket,
                .ServiceTicket = credentials.ServiceTicket,
            },
            std::monostate{});
    }

    TFuture<TAuthenticationResult> Authenticate(
        const TServiceTicketCredentials& credentials) override
    {
        return Get(
            TTicketAuthenticatorCacheKey{
                .ServiceTicket = credentials.ServiceTicket,
            },
            std::monostate{});
    }

    bool Reconfigure(const TUserTicketAuthenticationConfigPtr& userTicketAuthenticationConfig) override
    {
        bool changed = Underlying_->Reconfigure(userTicketAuthenticationConfig);
        if (changed) {
            Clear();
        }
        return changed;
    }

private:
    const ITicketAuthenticatorPtr Underlying_;

    TFuture<TAuthenticationResult> DoGet(
        const TTicketAuthenticatorCacheKey& key,
        const std::monostate& /*context*/) noexcept override
    {
        if (!key.UserTicket) {
            return Underlying_->Authenticate(TServiceTicketCredentials{
                .ServiceTicket = *key.ServiceTicket,
            });
        } else {
            return Underlying_->Authenticate(TUserTicketCredentials{
                .UserTicket = *key.UserTicket,
                .ServiceTicket = key.ServiceTicket,
            });
        }
    }
};

ITicketAuthenticatorPtr CreateCachingTicketAuthenticator(
    TCachingTicketAuthenticatorConfigPtr config,
    ITicketAuthenticatorPtr underlying,
    TProfiler profiler)
{
    return New<TCachingTicketAuthenticator>(
        std::move(config),
        std::move(underlying),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

class TTicketAuthenticatorWrapper
    : public NRpc::IAuthenticator
{
public:
    explicit TTicketAuthenticatorWrapper(ITicketAuthenticatorPtr underlying)
        : Underlying_(std::move(underlying))
    { }

    bool CanAuthenticate(const NRpc::TAuthenticationContext& context) override
    {
        if (!context.Header->HasExtension(NRpc::NProto::TCredentialsExt::credentials_ext)) {
            return false;
        }
        const auto& ext = context.Header->GetExtension(NRpc::NProto::TCredentialsExt::credentials_ext);
        return ext.has_user_ticket() || ext.has_service_ticket();
    }

    TFuture<NRpc::TAuthenticationResult> AsyncAuthenticate(
        const NRpc::TAuthenticationContext& context) override
    {
        YT_ASSERT(CanAuthenticate(context));
        const auto& ext = context.Header->GetExtension(NRpc::NProto::TCredentialsExt::credentials_ext);

        if (ext.has_user_ticket()) {
            TUserTicketCredentials credentials;
            credentials.UserTicket = ext.user_ticket();
            if (ext.has_service_ticket()) {
                credentials.ServiceTicket = ext.service_ticket();
            }
            return Underlying_->Authenticate(credentials).Apply(
                BIND([=] (const TAuthenticationResult& authResult) {
                    NRpc::TAuthenticationResult rpcResult;
                    rpcResult.User = authResult.Login;
                    rpcResult.Realm = authResult.Realm;
                    rpcResult.UserTicket = authResult.UserTicket;
                    return rpcResult;
                }));
        }

        if (ext.has_service_ticket()) {
            TServiceTicketCredentials credentials;
            credentials.ServiceTicket = ext.service_ticket();
            return Underlying_->Authenticate(credentials).Apply(
                BIND([=] (const TAuthenticationResult& authResult) {
                    NRpc::TAuthenticationResult rpcResult;
                    rpcResult.User = authResult.Login;
                    rpcResult.Realm = authResult.Realm;
                    return rpcResult;
                }));
        }

        YT_ABORT();
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
