#include "token_authenticator.h"
#include "blackbox_service.h"
#include "helpers.h"
#include "config.h"
#include "private.h"

#include <yt/client/api/client.h>

#include <yt/core/misc/async_expiring_cache.h>

#include <yt/core/rpc/authenticator.h>

namespace NYT {
namespace NAuth {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NApi;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = AuthLogger;
static const TString LocalUserIP = "127.0.0.1";

////////////////////////////////////////////////////////////////////////////////

class TBlackboxTokenAuthenticator
    : public ITokenAuthenticator
{
public:
    TBlackboxTokenAuthenticator(
        TBlackboxTokenAuthenticatorConfigPtr config,
        IBlackboxServicePtr blackboxService)
        : Config_(std::move(config))
        , Blackbox_(std::move(blackboxService))
    { }

    virtual TFuture<TAuthenticationResult> Authenticate(
        const TTokenCredentials& credentials) override
    {
        const auto& token = credentials.Token;
        TString userIP;
        if (credentials.UserIP.IsIP6() || credentials.UserIP.IsIP4()) {
            userIP = credentials.UserIP.FormatIP();
        } else {
            // UserIP is not required for token authentication, but
            // blackbox requires that parameter anyway.
            userIP = LocalUserIP;
        }

        auto tokenHash = GetCryptoHash(token);
        LOG_DEBUG("Authenticating user with token via Blackbox (TokenHash: %v, UserIP: %v)",
            tokenHash,
            userIP);
        return Blackbox_->Call("oauth", {{"oauth_token", token}, {"userip", userIP}})
            .Apply(BIND(
                &TBlackboxTokenAuthenticator::OnCallResult,
                MakeStrong(this),
                std::move(tokenHash)));
    }

private:
    const TBlackboxTokenAuthenticatorConfigPtr Config_;
    const IBlackboxServicePtr Blackbox_;

    TMonotonicCounter RejectedTokens_{"/blackbox_token_authenticator/rejected_tokens"};
    TMonotonicCounter InvalidBlackboxResponces_{"/blackbox_token_authenticator/invalid_responces"};
    TMonotonicCounter TokenScopeCheckErrors_{"/blackbox_token_authenticator/scope_check_errors"};

private:
    TAuthenticationResult OnCallResult(const TString& tokenHash, const INodePtr& data)
    {
        auto result = OnCallResultImpl(data);
        if (!result.IsOK()) {
            LOG_DEBUG(result, "Blackbox authentication failed (TokenHash: %v)",
                tokenHash);
            THROW_ERROR result
                << TErrorAttribute("token_hash", tokenHash);
        }

        LOG_DEBUG("Blackbox authentication successful (TokenHash: %v, Login: %v, Realm: %v)",
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
            AuthProfiler.Increment(InvalidBlackboxResponces_);
            return TError("Blackbox returned invalid response");
        }

        if (EBlackboxStatus(statusId.Value()) != EBlackboxStatus::Valid) {
            auto error = GetByYPath<TString>(data, "/error");
            auto reason = error.IsOK() ? error.Value() : "unknown";
            AuthProfiler.Increment(RejectedTokens_);
            return TError(NRpc::EErrorCode::InvalidCredentials, "Blackbox rejected token")
                << TErrorAttribute("reason", reason);
        }

        auto login = Blackbox_->GetLogin(data);
        auto oauthClientId = GetByYPath<TString>(data, "/oauth/client_id");
        auto oauthClientName = GetByYPath<TString>(data, "/oauth/client_name");
        auto oauthScope = GetByYPath<TString>(data, "/oauth/scope");

        // Sanity checks.
        if (!login.IsOK() || !oauthClientId.IsOK() || !oauthClientName.IsOK() || !oauthScope.IsOK()) {
            auto error = TError("Blackbox returned invalid response");
            if (!login.IsOK()) error.InnerErrors().push_back(login);
            if (!oauthClientId.IsOK()) error.InnerErrors().push_back(oauthClientId);
            if (!oauthClientName.IsOK()) error.InnerErrors().push_back(oauthClientName);
            if (!oauthScope.IsOK()) error.InnerErrors().push_back(oauthScope);

            AuthProfiler.Increment(InvalidBlackboxResponces_);
            return error;
        }

        // Check that token provides valid scope.
        // `oauthScope` is space-delimited list of provided scopes.
        if (Config_->EnableScopeCheck) {
            bool matchedScope = false;
            TStringBuf providedScopes(oauthScope.Value());
            TStringBuf providedScope;
            while (providedScopes.NextTok(' ', providedScope)) {
                if (providedScope == Config_->Scope) {
                    matchedScope = true;
                }
            }
            if (!matchedScope) {
                AuthProfiler.Increment(TokenScopeCheckErrors_);
                return TError(NRpc::EErrorCode::InvalidCredentials, "Token does not provide a valid scope")
                    << TErrorAttribute("scope", oauthScope.Value());
            }
        }

        // Check that token was issued by a known application.
        TAuthenticationResult result;
        result.Login = login.Value();
        result.Realm = "blackbox:token:" + oauthClientId.Value() + ":" + oauthClientName.Value();
        return result;
    }
};

ITokenAuthenticatorPtr CreateBlackboxTokenAuthenticator(
    TBlackboxTokenAuthenticatorConfigPtr config,
    IBlackboxServicePtr blackboxService)
{
    return New<TBlackboxTokenAuthenticator>(std::move(config), std::move(blackboxService));
}

////////////////////////////////////////////////////////////////////////////////

class TCypressTokenAuthenticator
    : public ITokenAuthenticator
{
public:
    TCypressTokenAuthenticator(
        TCypressTokenAuthenticatorConfigPtr config,
        IClientPtr client)
        : Config_(std::move(config))
        , Client_(std::move(client))
    { }

    virtual TFuture<TAuthenticationResult> Authenticate(
        const TTokenCredentials& credentials) override
    {
        const auto& token = credentials.Token;
        const auto& userIP = credentials.UserIP;
        auto tokenHash = GetCryptoHash(token);
        LOG_DEBUG("Authenticating user with token via Cypress (TokenHash: %v, UserIP: %v)",
            tokenHash,
            userIP);

        TString path;
        if (!Config_->Secure) {
            path = Config_->RootPath + "/" + ToYPathLiteral(credentials.Token);
        } else {
            path = Config_->RootPath + "/" + GetCryptoHash(credentials.Token);
        }
        return Client_->GetNode(path)
            .Apply(BIND(
                &TCypressTokenAuthenticator::OnCallResult,
                MakeStrong(this),
                std::move(tokenHash)));
    }

private:
    const TCypressTokenAuthenticatorConfigPtr Config_;
    const IClientPtr Client_;

private:
    TAuthenticationResult OnCallResult(const TString& tokenHash, const TErrorOr<TYsonString>& callResult)
    {
        if (!callResult.IsOK()) {
            if (callResult.FindMatching(NYTree::EErrorCode::ResolveError)) {
                LOG_DEBUG(callResult, "Token is missing in Cypress (TokenHash: %v)",
                    tokenHash);
                THROW_ERROR_EXCEPTION("Token is missing in Cypress");
            } else {
                LOG_DEBUG(callResult, "Cypress authentication failed (TokenHash: %v)",
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
            LOG_DEBUG("Cypress authentication successful (TokenHash: %v, Login: %v)",
                tokenHash,
                authResult.Login);
            return authResult;
        } catch (const std::exception& ex) {
            LOG_DEBUG(callResult, "Cypress contains malformed authentication entry (TokenHash: %v)",
                tokenHash);
            THROW_ERROR_EXCEPTION("Malformed Cypress authentication entry")
                << TErrorAttribute("token_hash", tokenHash);
        }
    }
};

ITokenAuthenticatorPtr CreateCypressTokenAuthenticator(
    TCypressTokenAuthenticatorConfigPtr config,
    IClientPtr client)
{
    return New<TCypressTokenAuthenticator>(std::move(config), std::move(client));
}

////////////////////////////////////////////////////////////////////////////////

class TCachingTokenAuthenticator
    : public ITokenAuthenticator
    , public TAsyncExpiringCache<TTokenCredentials, TAuthenticationResult>
{
public:
    TCachingTokenAuthenticator(TAsyncExpiringCacheConfigPtr config, ITokenAuthenticatorPtr tokenAuthenticator)
        : TAsyncExpiringCache(std::move(config))
        , TokenAuthenticator_(std::move(tokenAuthenticator))
    { }

    virtual TFuture<TAuthenticationResult> Authenticate(const TTokenCredentials& credentials) override
    {
        return Get(credentials);
    }

private:
    virtual TFuture<TAuthenticationResult> DoGet(const TTokenCredentials& credentials) override
    {
        return TokenAuthenticator_->Authenticate(credentials);
    }

    const ITokenAuthenticatorPtr TokenAuthenticator_;
};

ITokenAuthenticatorPtr CreateCachingTokenAuthenticator(
    TAsyncExpiringCacheConfigPtr config,
    ITokenAuthenticatorPtr authenticator)
{
    return New<TCachingTokenAuthenticator>(std::move(config), std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

class TCompositeTokenAuthenticator
    : public ITokenAuthenticator
{
public:
    explicit TCompositeTokenAuthenticator(std::vector<ITokenAuthenticatorPtr> authenticators)
        : Authenticators_(std::move(authenticators))
    { }

    virtual TFuture<TAuthenticationResult> Authenticate(
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
                Promise_.Set(TError("Authentication failed")
                    << Errors_);
                return;
            }

            const auto& authenticator = Owner_->Authenticators_[CurrentIndex_++];
            authenticator->Authenticate(Credentials_).Subscribe(
                BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TAuthenticationResult>& result) {
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
    virtual TFuture<TAuthenticationResult> Authenticate(const TTokenCredentials& credentials) override
    {
        static const auto Realm = TString("noop");
        TAuthenticationResult result{NRpc::RootUserName, Realm};
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

    virtual TFuture<NRpc::TAuthenticationResult> Authenticate(
        const NRpc::TAuthenticationContext& context) override
    {
        if (!context.Header->HasExtension(NRpc::NProto::TCredentialsExt::credentials_ext)) {
            return Null;
        }

        const auto& ext = context.Header->GetExtension(NRpc::NProto::TCredentialsExt::credentials_ext);
        if (!ext.has_token()) {
            return Null;
        }

        TTokenCredentials credentials;
        credentials.UserIP = context.UserIP;
        credentials.Token = ext.token();
        return Underlying_->Authenticate(credentials).Apply(
            BIND([=] (const TAuthenticationResult& authResult) {
                NRpc::TAuthenticationResult rpcResult;
                rpcResult.User = authResult.Login;
                rpcResult.Realm = authResult.Realm;
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

} // namespace NAuth
} // namespace NYT
