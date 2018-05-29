#include "token_authenticator.h"
#include "blackbox_service.h"
#include "helpers.h"
#include "config.h"
#include "private.h"

#include <yt/ytlib/api/client.h>

#include <yt/core/misc/async_expiring_cache.h>

#include <yt/core/crypto/crypto.h>

#include <util/string/split.h>

namespace NYT {
namespace NAuth {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NCrypto;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

class TBlackboxTokenAuthenticator
    : public ITokenAuthenticator
{
public:
    TBlackboxTokenAuthenticator(
        TBlackboxTokenAuthenticatorConfigPtr config,
        IBlackboxServicePtr blackbox)
        : Config_(std::move(config))
        , Blackbox_(std::move(blackbox))
    { }

    virtual TFuture<TAuthenticationResult> Authenticate(
        const TTokenCredentials& credentials) override
    {
        const auto& token = credentials.Token;
        const auto& userIP = credentials.UserIP;
        auto tokenMD5 = TMD5Hasher().Append(token).GetHexDigestUpper();
        LOG_DEBUG("Authenticating user with token via Blackbox (TokenMD5: %v, UserIP: %v)",
            tokenMD5,
            userIP);
        return Blackbox_->Call("oauth", {{"oauth_token", token}, {"userip", userIP}})
            .Apply(BIND(
                &TBlackboxTokenAuthenticator::OnCallResult,
                MakeStrong(this),
                std::move(tokenMD5)));
    }

private:
    const TBlackboxTokenAuthenticatorConfigPtr Config_;
    const IBlackboxServicePtr Blackbox_;

private:
    TAuthenticationResult OnCallResult(const TString& tokenMD5, const INodePtr& data)
    {
        auto result = OnCallResultImpl(data);
        if (!result.IsOK()) {
            LOG_DEBUG(result, "Blackbox authentication failed (TokenMD5: %v)",
                tokenMD5);
            THROW_ERROR result
                << TErrorAttribute("token_md5", tokenMD5);
        }

        LOG_DEBUG("Blackbox authentication successful (TokenMD5: %v, Login: %v, Realm: %v)",
            tokenMD5,
            result.Value().Login,
            result.Value().Realm);
        return result.Value();
    }

    TErrorOr<TAuthenticationResult> OnCallResultImpl(const INodePtr& data)
    {
        // See https://doc.yandex-team.ru/blackbox/reference/method-oauth-response-json.xml for reference.
        auto statusId = GetByYPath<int>(data, "/status/id");
        if (!statusId.IsOK()) {
            return TError("Blackbox returned invalid response");
        }

        if (EBlackboxStatus(statusId.Value()) != EBlackboxStatus::Valid) {
            auto error = GetByYPath<TString>(data, "/error");
            auto reason = error.IsOK() ? error.Value() : "unknown";
            return TError("Blackbox rejected token")
                << TErrorAttribute("reason", reason);
        }

        auto login = GetByYPath<TString>(data, "/login");
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
                return TError("Token does not provide a valid scope")
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
    IBlackboxServicePtr blackbox)
{
    return New<TBlackboxTokenAuthenticator>(std::move(config), std::move(blackbox));
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
        auto tokenMD5 = TMD5Hasher().Append(token).GetHexDigestUpper();
        LOG_DEBUG("Authenticating user with token via Cypress (TokenMD5: %v, UserIP: %v)",
            tokenMD5,
            userIP);

        auto path = Config_->RootPath + "/" + ToYPathLiteral(credentials.Token);
        return Client_->GetNode(path)
            .Apply(BIND(
                &TCypressTokenAuthenticator::OnCallResult,
                MakeStrong(this),
                std::move(tokenMD5)));
    }

private:
    const TCypressTokenAuthenticatorConfigPtr Config_;
    const IClientPtr Client_;

private:
    TAuthenticationResult OnCallResult(const TString& tokenMD5, const TErrorOr<TYsonString>& callResult)
    {
        if (!callResult.IsOK()) {
            if (callResult.FindMatching(NYTree::EErrorCode::ResolveError)) {
                LOG_DEBUG(callResult, "Token is missing in Cypress (TokenMD5: %v)",
                    tokenMD5);
                THROW_ERROR_EXCEPTION("Token is missing in Cypress");
            } else {
                LOG_DEBUG(callResult, "Cypress authentication failed (TokenMD5: %v)",
                    tokenMD5);
                THROW_ERROR_EXCEPTION("Cypress authentication failed")
                    << TErrorAttribute("token_md5", tokenMD5)
                    << callResult;
            }
        }

        const auto& ysonString = callResult.Value();
        try {
            TAuthenticationResult authResult;
            authResult.Login = ConvertTo<TString>(ysonString);
            authResult.Realm = Config_->Realm;
            LOG_DEBUG("Cypress authentication successful (TokenMD5: %v, Login: %v)",
                tokenMD5,
                authResult.Login);
            return authResult;
        } catch (const std::exception& ex) {
            LOG_DEBUG(callResult, "Cypress contains malformed authentication entry (TokenMD5: %v)",
                tokenMD5);
            THROW_ERROR_EXCEPTION("Malformed Cypress authentication entry")
                << TErrorAttribute("token_md5", tokenMD5);
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

        TPromise<TAuthenticationResult> Promise_;
        std::vector<TError> Errors_;
        size_t CurrentIndex_ = 0;

    private:
        void InvokeNext()
        {
            if (CurrentIndex_ >= Owner_->Authenticators_.size()) {
                THROW_ERROR_EXCEPTION("Authentication failed")
                    << Errors_;
            }

            const auto& authenticator = Owner_->Authenticators_[CurrentIndex_++];
            authenticator->Authenticate(Credentials_).Subscribe(
                BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TAuthenticationResult>& result) {
                    if (result.IsOK()) {
                        Promise_.Set(result.Value());
                    } else {
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

} // namespace NAuth
} // namespace NYT
