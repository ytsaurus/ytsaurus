#include "cypress_cookie_login.h"

#include "config.h"
#include "cypress_cookie_store.h"
#include "cypress_login_authenticator.h"
#include "login_authenticator.h"
#include "private.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/crypto/crypto.h>

#include <yt/yt/core/http/helpers.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/helpers.h>

#include <library/cpp/string_utils/base64/base64.h>

namespace NYT::NAuth {

using namespace NConcurrency;
using namespace NCrypto;
using namespace NHttp;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

class TCypressCookieLoginHandler
    : public IHttpHandler
{
public:
    TCypressCookieLoginHandler(
        TCypressCookieGeneratorConfigPtr config,
        NApi::IClientPtr client,
        ICypressCookieStorePtr cookieStore,
        ILoginAuthenticatorPtr authenticator)
        : Config_(std::move(config))
        , Client_(std::move(client))
        , CookieStore_(std::move(cookieStore))
        , Authenticator_(std::move(authenticator))
    { }

    void HandleRequest(
        const IRequestPtr& req,
        const IResponseWriterPtr& rsp) override
    {
        if (auto header = req->GetHeaders()->Find(AuthorizationHeader)) {
            HandleLoginRequest(*header, req, rsp);
        } else {
            HandleRegularRequest(rsp);
        }

        WaitFor(rsp->Close())
            .ThrowOnError();
    }

private:
    const TCypressCookieGeneratorConfigPtr Config_;
    const NApi::IClientPtr Client_;
    const ICypressCookieStorePtr CookieStore_;
    const ILoginAuthenticatorPtr Authenticator_;

    static constexpr TStringBuf AuthorizationHeader = "Authorization";
    static constexpr TStringBuf SetCookieHeader = "Set-Cookie";
    static constexpr TStringBuf BasicAuthorizationMethod = "Basic";

    void ReplyAndLogError(
        const IRequestPtr& req,
        const IResponseWriterPtr& rsp,
        const TError& error,
        bool maskError,
        const std::optional<std::string>& user = {})
    {
        if (maskError) {
            // Hide details about unsuccessful login attempts for security reasons.
            ReplyError(rsp, TError("Incorrect login or password"));
        } else {
            ReplyError(rsp, error);
        }

        YT_LOG_DEBUG(error, "Failed to login user (ConnectionId: %v, User: %v)",
            req->GetConnectionId(),
            user);
    }

    void HandleLoginRequest(
        TStringBuf authorizationHeader,
        const IRequestPtr& req,
        const IResponseWriterPtr& rsp)
    {
        TStringBuf authorizationMethod;
        TStringBuf encodedCredentials;
        if (!authorizationHeader.TrySplit(' ', authorizationMethod, encodedCredentials)) {
            rsp->SetStatus(EStatusCode::BadRequest);
            ReplyAndLogError(
                req,
                rsp,
                TError("Malformed \"Authorization\" header: failed to parse authorization method"),
                /*maskError*/ false);
            return;
        }

        if (authorizationMethod != BasicAuthorizationMethod) {
            rsp->SetStatus(EStatusCode::BadRequest);
            ReplyAndLogError(
                req,
                rsp,
                TError("Unsupported authorization method %Qlv", authorizationMethod),
                /*maskError*/ false);
            return;
        }

        auto credentials = Base64StrictDecode(encodedCredentials);
        TStringBuf user;
        TStringBuf password;
        if (!TStringBuf{credentials}.TrySplit(':', user, password)) {
            rsp->SetStatus(EStatusCode::BadRequest);
            ReplyAndLogError(
                req,
                rsp,
                TError("Failed to parse user credentials"),
                /*maskError*/ false);
            return;
        }

        TLoginResult loginResult;
        try {
            loginResult = WaitFor(Authenticator_->Authenticate(TLoginCredentials{TString{user}, TString{password}}))
                .ValueOrThrow();
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            if (error.FindMatching(NYTree::EErrorCode::ResolveError)) {
                // User not found in Cypress — present the login form.
                HandleRegularRequest(rsp);
                ReplyAndLogError(
                    req,
                    rsp,
                    TError("No such user %Qlv or user has no password set", user) << error,
                    /*maskError*/ true,
                    std::string(user));
                return;
            }
            if (error.FindMatching(NRpc::EErrorCode::InvalidCredentials)) {
                HandleRegularRequest(rsp);
                ReplyAndLogError(req, rsp, error, /*maskError*/ true, std::string(user));
                return;
            }

            // Unknown error, reply 500.
            ReplyAndLogError(
                req,
                rsp,
                TError("Failed to authenticate user %Qlv", user) << error,
                /*maskError*/ true,
                std::string(user));
            throw;
        }

        const auto& login = loginResult.Login;

        // Fetch the appropriate password revision to embed in the cookie.
        // For Cypress auth: "password_revision" — incremented automatically on password change.
        // For LDAP auth: "ldap_password_revision" — incremented manually to force-invalidate cookies.
        auto revisionAttribute = (loginResult.Source == EAuthSource::Ldap)
            ? LdapPasswordRevisionAttribute
            : PasswordRevisionAttribute;

        YT_LOG_DEBUG("Login succeeded, fetching password revision (User: %v, Source: %v, RevisionAttribute: %v)",
            login,
            loginResult.Source,
            revisionAttribute);

        ui64 passwordRevision = 0;
        try {
            passwordRevision = WaitFor(FetchPasswordRevision(login, revisionAttribute))
                .ValueOrThrow();
        } catch (const std::exception& ex) {
            auto error = TError("Failed to fetch password revision for user %Qv", login)
                << TError(ex);
            ReplyAndLogError(req, rsp, error, /*maskError*/ false, login);
            throw;
        }

        auto expirationTimeout = (loginResult.Source == EAuthSource::Ldap)
            ? Config_->LdapCookieExpirationTimeout
            : Config_->CookieExpirationTimeout;

        auto cookie = New<TCypressCookie>();
        cookie->Value = GenerateCookieValue();
        cookie->User = login;
        cookie->AuthSource = loginResult.Source;
        cookie->PasswordRevision = passwordRevision;
        cookie->ExpiresAt = TInstant::Now() + expirationTimeout;

        auto error = WaitFor(CookieStore_->RegisterCookie(cookie));
        if (!error.IsOK()) {
            error = TError("Failed to register cookie in cookie store") << error;
            ReplyAndLogError(req, rsp, error, /*maskError*/ false, login);
            // Will return 500.
            error.ThrowOnError();
        }

        YT_LOG_DEBUG("Issued new cookie for user (User: %v, CookieMD5: %v)",
            login,
            GetMD5HexDigestUpperCase(cookie->Value));

        if (const auto& redirectUrl = Config_->RedirectUrl) {
            rsp->SetStatus(EStatusCode::PermanentRedirect);
            rsp->GetHeaders()->Add("Location", *redirectUrl);
        } else {
            rsp->SetStatus(EStatusCode::OK);
        }

        rsp->GetHeaders()->Add(TString{SetCookieHeader}, cookie->ToHeader(Config_));
    }

    void HandleRegularRequest(const IResponseWriterPtr& rsp)
    {
        rsp->SetStatus(EStatusCode::Unauthorized);
        rsp->GetHeaders()->Add("WWW-Authenticate", "Basic");
    }

    TFuture<ui64> FetchPasswordRevision(const TString& user, TStringBuf attribute)
    {
        auto path = Format("//sys/users/%v", ToYPathLiteral(user));

        NApi::TGetNodeOptions options;
        options.Attributes = {attribute};

        return Client_->GetNode(path, options)
            .Apply(BIND([attribute] (const NYson::TYsonString& rsp) {
                // For ldap_password_revision the attribute may not exist yet — default to 0.
                return ConvertToNode(rsp)->Attributes().Get<ui64>(attribute, /*default*/ 0);
            }));
    }
};

////////////////////////////////////////////////////////////////////////////////

IHttpHandlerPtr CreateCypressCookieLoginHandler(
    TCypressCookieGeneratorConfigPtr config,
    NApi::IClientPtr client,
    ICypressCookieStorePtr cookieStore,
    std::vector<ILoginAuthenticatorPtr> authenticators)
{
    auto compositeAuth = CreateCompositeLoginAuthenticator(std::move(authenticators));

    return New<TCypressCookieLoginHandler>(
        std::move(config),
        std::move(client),
        std::move(cookieStore),
        std::move(compositeAuth));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
