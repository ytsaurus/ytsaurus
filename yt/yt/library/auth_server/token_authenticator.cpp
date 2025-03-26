#include "token_authenticator.h"

#include "auth_cache.h"
#include "blackbox_service.h"
#include "helpers.h"
#include "config.h"
#include "credentials.h"
#include "private.h"
#include "helpers.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/rpc/authenticator.h>

#include <util/digest/multi.h>

namespace NYT::NAuth {

using namespace NYTree;
using namespace NNet;
using namespace NYson;
using namespace NYPath;
using namespace NApi;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

class TBlackboxTokenAuthenticator
    : public ITokenAuthenticator
{
public:
    TBlackboxTokenAuthenticator(
        TBlackboxTokenAuthenticatorConfigPtr config,
        IBlackboxServicePtr blackboxService,
        NProfiling::TProfiler profiler)
        : Config_(std::move(config))
        , BlackboxSevice_(std::move(blackboxService))
        , RejectedTokensCounter_(profiler.Counter("/rejected_tokens"))
        , InvalidBlackboxResponsesCounter_(profiler.Counter("/invalid_responses"))
        , TokenScopeCheckErrorsCounter_(profiler.Counter("/scope_check_errors"))
    { }

    TFuture<TAuthenticationResult> Authenticate(
        const TTokenCredentials& credentials) override
    {
        const auto& token = credentials.Token;
        auto userIP = FormatUserIP(credentials.UserIP);
        auto tokenHash = GetCryptoHash(token);

        YT_LOG_DEBUG("Authenticating user with token via Blackbox (TokenHash: %v, UserIP: %v)",
            tokenHash,
            userIP);

        THashMap<TString, TString> params{
            {"oauth_token", token},
            {"userip", userIP},
        };

        if (Config_->GetUserTicket) {
            params["get_user_ticket"] = "yes";
        }

        return BlackboxSevice_->Call("oauth", params)
            .Apply(BIND(
                &TBlackboxTokenAuthenticator::OnCallResult,
                MakeStrong(this),
                std::move(tokenHash)));
    }

private:
    const TBlackboxTokenAuthenticatorConfigPtr Config_;
    const IBlackboxServicePtr BlackboxSevice_;

    const TCounter RejectedTokensCounter_;
    const TCounter InvalidBlackboxResponsesCounter_;
    const TCounter TokenScopeCheckErrorsCounter_;


    TAuthenticationResult OnCallResult(const TString& tokenHash, const INodePtr& data)
    {
        auto result = OnCallResultImpl(data);
        if (!result.IsOK()) {
            YT_LOG_DEBUG(result, "Blackbox authentication failed (TokenHash: %v)",
                tokenHash);
            THROW_ERROR result
                << TErrorAttribute("token_hash", tokenHash);
        }

        YT_LOG_DEBUG("Blackbox authentication successful (TokenHash: %v, Login: %v, Realm: %v)",
            tokenHash,
            result.Value().Login,
            result.Value().Realm);
        return result.Value();
    }

    TErrorOr<TAuthenticationResult> OnCallResultImpl(const INodePtr& data)
    {
        // See https://doc.yandex-team.ru/blackbox/reference/method-oauth-response-json.xml for reference.
        auto statusId = GetByYPath<int>(data, "/status/id");
        if (!statusId.IsOK()) {
            InvalidBlackboxResponsesCounter_.Increment();
            return TError("Blackbox returned invalid response");
        }

        if (EBlackboxStatus(statusId.Value()) != EBlackboxStatus::Valid) {
            auto error = GetByYPath<TString>(data, "/error");
            auto reason = error.IsOK() ? error.Value() : "unknown";
            RejectedTokensCounter_.Increment();
            return TError(NRpc::EErrorCode::InvalidCredentials, "Blackbox rejected token")
                << TErrorAttribute("reason", reason);
        }

        auto login = BlackboxSevice_->GetLogin(data);
        auto oauthClientId = GetByYPath<TString>(data, "/oauth/client_id");
        auto oauthClientName = GetByYPath<TString>(data, "/oauth/client_name");
        auto oauthScope = GetByYPath<TString>(data, "/oauth/scope");

        // Sanity checks.
        if (!login.IsOK() || !oauthClientId.IsOK() || !oauthClientName.IsOK() || !oauthScope.IsOK()) {
            auto error = TError("Blackbox returned invalid response");
            if (!login.IsOK()) error.MutableInnerErrors()->push_back(login);
            if (!oauthClientId.IsOK()) error.MutableInnerErrors()->push_back(oauthClientId);
            if (!oauthClientName.IsOK()) error.MutableInnerErrors()->push_back(oauthClientName);
            if (!oauthScope.IsOK()) error.MutableInnerErrors()->push_back(oauthScope);

            InvalidBlackboxResponsesCounter_.Increment();
            return error;
        }

        // Check that token provides valid scope.
        // `oauthScope` is space-delimited list of provided scopes.
        if (Config_->EnableScopeCheck) {
            bool matchedScope = false;
            TStringBuf providedScopesStr(oauthScope.Value());
            TStringBuf providedScope;
            std::vector<TStringBuf> providedScopes;
            while (providedScopesStr.NextTok(' ', providedScope)) {
                providedScopes.push_back(providedScope);
                if (providedScope == Config_->Scope) {
                    matchedScope = true;
                }
            }
            if (!matchedScope) {
                TokenScopeCheckErrorsCounter_.Increment();
                return TError(NRpc::EErrorCode::InvalidCredentials, "Token does not provide a valid scope")
                    << TErrorAttribute("provided_scopes", providedScopes)
                    << TErrorAttribute("allowed_scope", Config_->Scope);
            }
        }

        // Check that token was issued by a known application.
        TAuthenticationResult result;
        result.Login = login.Value();
        result.Realm = "blackbox:token:" + oauthClientId.Value() + ":" + oauthClientName.Value();
        auto userTicket = GetByYPath<TString>(data, "/user_ticket");
        if (userTicket.IsOK()) {
            result.UserTicket = userTicket.Value();
        } else if (Config_->GetUserTicket) {
            return TError("Failed to retrieve user ticket");
        }
        return result;
    }
};

ITokenAuthenticatorPtr CreateBlackboxTokenAuthenticator(
    TBlackboxTokenAuthenticatorConfigPtr config,
    IBlackboxServicePtr blackboxService,
    NProfiling::TProfiler profiler)
{
    return New<TBlackboxTokenAuthenticator>(
        std::move(config),
        std::move(blackboxService),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

class TLegacyCypressTokenAuthenticator
    : public ITokenAuthenticator
{
public:
    TLegacyCypressTokenAuthenticator(
        TCypressTokenAuthenticatorConfigPtr config,
        IClientPtr client)
        : Config_(std::move(config))
        , Client_(std::move(client))
    { }

    TFuture<TAuthenticationResult> Authenticate(
        const TTokenCredentials& credentials) override
    {
        const auto& token = credentials.Token;
        const auto& userIP = credentials.UserIP;
        auto tokenHash = GetCryptoHash(token);
        YT_LOG_DEBUG("Authenticating user with token via Cypress (TokenHash: %v, UserIP: %v)",
            tokenHash,
            userIP);

        auto path = Format("%v/%v",
            Config_->RootPath ? Config_->RootPath : "//sys/tokens",
            ToYPathLiteral(Config_->Secure ? tokenHash : token));
        return Client_->GetNode(path)
            .Apply(BIND(
                &TLegacyCypressTokenAuthenticator::OnCallResult,
                MakeStrong(this),
                std::move(token),
                std::move(tokenHash)));
    }

private:
    const TCypressTokenAuthenticatorConfigPtr Config_;
    const IClientPtr Client_;

private:
    static void SanitizeToken(TError* error, const TString& token)
    {
        auto message = TString(error->GetMessage());
        SubstGlobal(message, token, "<redacted>");
        error->SetMessage(message);
        for (auto& innerError : *error->MutableInnerErrors()) {
            SanitizeToken(&innerError, token);
        }
    }

    TAuthenticationResult OnCallResult(const TString& token, const TString& tokenHash, const TErrorOr<TYsonString>& callResult)
    {
        if (!callResult.IsOK()) {
            TError error = callResult;
            if (!Config_->Secure) {
                SanitizeToken(&error, token);
            }
            if (callResult.FindMatching(NYTree::EErrorCode::ResolveError)) {
                YT_LOG_DEBUG(callResult, "Token is missing in Cypress (TokenHash: %v)",
                    tokenHash);
                THROW_ERROR_EXCEPTION(NRpc::EErrorCode::InvalidCredentials,
                    "Token is missing in Cypress")
                    << TErrorAttribute("token_hash", tokenHash)
                    << callResult;
            } else {
                YT_LOG_DEBUG(callResult, "Cypress authentication failed (TokenHash: %v)",
                    tokenHash);
                THROW_ERROR_EXCEPTION("Cypress authentication failed")
                    << TErrorAttribute("token_hash", tokenHash)
                    << callResult;
            }
        }

        const auto& ysonString = callResult.Value();
        try {
            TAuthenticationResult authResult;
            authResult.Login = ConvertTo<TString>(ysonString);
            authResult.Realm = Config_->Realm;
            YT_LOG_DEBUG("Cypress authentication successful (TokenHash: %v, Login: %v)",
                tokenHash,
                authResult.Login);
            return authResult;
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(callResult, "Cypress contains malformed authentication entry (TokenHash: %v)",
                tokenHash);
            THROW_ERROR_EXCEPTION("Malformed Cypress authentication entry")
                << TErrorAttribute("token_hash", tokenHash);
        }
    }
};

ITokenAuthenticatorPtr CreateLegacyCypressTokenAuthenticator(
    TCypressTokenAuthenticatorConfigPtr config,
    IClientPtr client)
{
    return New<TLegacyCypressTokenAuthenticator>(std::move(config), std::move(client));
}

////////////////////////////////////////////////////////////////////////////////

struct TTokenAuthenticatorCacheKey
{
    TString Token;
    TString UserIPFactor;

    operator size_t() const
    {
        return MultiHash(Token, UserIPFactor);
    }

    bool operator == (const TTokenAuthenticatorCacheKey&) const = default;
};

class TCachingTokenAuthenticator
    : public ITokenAuthenticator
    , public TAuthCache<TTokenAuthenticatorCacheKey, TAuthenticationResult, TNetworkAddress>
{
public:
    TCachingTokenAuthenticator(
        TCachingTokenAuthenticatorConfigPtr config,
        ITokenAuthenticatorPtr tokenAuthenticator,
        NProfiling::TProfiler profiler)
        : TAuthCache(config->Cache, std::move(profiler))
        , Config_(std::move(config))
        , TokenAuthenticator_(std::move(tokenAuthenticator))
    { }

    TFuture<TAuthenticationResult> Authenticate(const TTokenCredentials& credentials) override
    {
        return Get(
            TTokenAuthenticatorCacheKey{credentials.Token, GetBlackboxCacheKeyFactorFromUserIP(Config_->CacheKeyMode, credentials.UserIP)},
            credentials.UserIP);
    }

private:
    const TCachingTokenAuthenticatorConfigPtr Config_;
    const ITokenAuthenticatorPtr TokenAuthenticator_;

    TFuture<TAuthenticationResult> DoGet(
        const TTokenAuthenticatorCacheKey& key,
        const TNetworkAddress& userIP) noexcept override
    {
        return TokenAuthenticator_->Authenticate(TTokenCredentials{key.Token, userIP});
    }
};

ITokenAuthenticatorPtr CreateCachingTokenAuthenticator(
    TCachingTokenAuthenticatorConfigPtr config,
    ITokenAuthenticatorPtr authenticator,
    NProfiling::TProfiler profiler)
{
    return New<TCachingTokenAuthenticator>(
        std::move(config),
        std::move(authenticator),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

class TCompositeTokenAuthenticator
    : public ITokenAuthenticator
{
public:
    explicit TCompositeTokenAuthenticator(std::vector<ITokenAuthenticatorPtr> authenticators)
        : Authenticators_(std::move(authenticators))
    { }

    TFuture<TAuthenticationResult> Authenticate(
        const TTokenCredentials& credentials) override
    {
        return New<TAuthenticationSession>(this, credentials)->GetResult();
    }

private:
    const std::vector<ITokenAuthenticatorPtr> Authenticators_;

    class TAuthenticationSession
        : public TRefCounted
    {
    public:
        TAuthenticationSession(
            TIntrusivePtr<TCompositeTokenAuthenticator> owner,
            const TTokenCredentials& credentials)
            : Owner_(std::move(owner))
            , Credentials_(credentials)
        {
            InvokeNext();
        }

        TFuture<TAuthenticationResult> GetResult()
        {
            return Promise_;
        }

    private:
        const TIntrusivePtr<TCompositeTokenAuthenticator> Owner_;
        const TTokenCredentials Credentials_;

        TPromise<TAuthenticationResult> Promise_ = NewPromise<TAuthenticationResult>();
        std::vector<TError> Errors_;
        size_t CurrentIndex_ = 0;

    private:
        void InvokeNext()
        {
            if (CurrentIndex_ >= Owner_->Authenticators_.size()) {
                Promise_.Set(TError(NSecurityClient::EErrorCode::AuthenticationError, "Authentication failed")
                    << Errors_);
                return;
            }

            const auto& authenticator = Owner_->Authenticators_[CurrentIndex_++];
            authenticator->Authenticate(Credentials_).Subscribe(
                BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<TAuthenticationResult>& result) {
                    if (result.IsOK()) {
                        Promise_.Set(result.Value());
                    } else {
                        Errors_.push_back(result);
                        InvokeNext();
                    }
                }));
        }
    };
};

ITokenAuthenticatorPtr CreateCompositeTokenAuthenticator(
    std::vector<ITokenAuthenticatorPtr> authenticators)
{
    return New<TCompositeTokenAuthenticator>(std::move(authenticators));
}

////////////////////////////////////////////////////////////////////////////////

class TNoopTokenAuthenticator
    : public ITokenAuthenticator
{
public:
    TFuture<TAuthenticationResult> Authenticate(const TTokenCredentials& /*credentials*/) override
    {
        static const auto Realm = TString("noop");
        static const auto UserTicket = TString("");
        TAuthenticationResult result{
            .Login = NRpc::RootUserName,
            .Realm = Realm,
            .UserTicket = UserTicket,
        };
        return MakeFuture<TAuthenticationResult>(result);
    }
};

ITokenAuthenticatorPtr CreateNoopTokenAuthenticator()
{
    return New<TNoopTokenAuthenticator>();
}

////////////////////////////////////////////////////////////////////////////////

class TTokenAuthenticatorWrapper
    : public NRpc::IAuthenticator
{
public:
    explicit TTokenAuthenticatorWrapper(ITokenAuthenticatorPtr underlying)
        : Underlying_(std::move(underlying))
    { }

    bool CanAuthenticate(const NRpc::TAuthenticationContext& context) override
    {
        if (!context.Header->HasExtension(NRpc::NProto::TCredentialsExt::credentials_ext)) {
            return false;
        }
        const auto& ext = context.Header->GetExtension(NRpc::NProto::TCredentialsExt::credentials_ext);
        return ext.has_token();
    }

    TFuture<NRpc::TAuthenticationResult> AsyncAuthenticate(
        const NRpc::TAuthenticationContext& context) override
    {
        YT_ASSERT(CanAuthenticate(context));
        const auto& ext = context.Header->GetExtension(NRpc::NProto::TCredentialsExt::credentials_ext);
        TTokenCredentials credentials;
        credentials.UserIP = context.UserIP;
        credentials.Token = ext.token();
        return Underlying_->Authenticate(credentials).Apply(
            BIND([=] (const TAuthenticationResult& authResult) {
                NRpc::TAuthenticationResult rpcResult;
                rpcResult.User = authResult.Login;
                rpcResult.Realm = authResult.Realm;
                rpcResult.UserTicket = authResult.UserTicket;
                return rpcResult;
            }));
    }
private:
    const ITokenAuthenticatorPtr Underlying_;
};

NRpc::IAuthenticatorPtr CreateTokenAuthenticatorWrapper(ITokenAuthenticatorPtr underlying)
{
    return New<TTokenAuthenticatorWrapper>(std::move(underlying));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
