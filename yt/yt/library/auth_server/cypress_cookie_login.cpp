#include "cypress_cookie_login.h"

#include "config.h"
#include "cypress_cookie_store.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/library/auth_server/private.h>

#include <yt/yt/core/crypto/crypto.h>

#include <yt/yt/core/http/helpers.h>

#include <library/cpp/string_utils/base64/base64.h>

namespace NYT::NAuth {

using namespace NApi;
using namespace NConcurrency;
using namespace NCrypto;
using namespace NHttp;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

class TCypressCookieLoginHandler
    : public IHttpHandler
{
public:
    TCypressCookieLoginHandler(
        TCypressCookieGeneratorConfigPtr config,
        NApi::IClientPtr client,
        ICypressCookieStorePtr cookieStore)
        : Config_(std::move(config))
        , Client_(std::move(client))
        , CookieStore_(std::move(cookieStore))
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

    static constexpr TStringBuf AuthorizationHeader = "Authorization";
    static constexpr TStringBuf SetCookieHedaer = "Set-Cookie";
    static constexpr TStringBuf BasicAuthorizationMethod = "Basic";

    struct TUserInfo
    {
        TString HashedPassword;
        TString PasswordSalt;
        ui64 PasswordRevision;
    };

    void HandleLoginRequest(
        TStringBuf authorizationHeader,
        const IRequestPtr& req,
        const IResponseWriterPtr& rsp)
    {
        auto replyAndLogError = [&] (
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

            YT_LOG_DEBUG(error, "Failed to login user using password (ConnectionId: %v, User: %v)",
                req->GetConnectionId(),
                user);
        };

        TStringBuf authorizationMethod;
        TStringBuf encodedCredentials;
        if (!authorizationHeader.TrySplit(' ', authorizationMethod, encodedCredentials)) {
            rsp->SetStatus(EStatusCode::BadRequest);

            auto error = TError("Malformed \"Authorization\" header: failed to parse authorization method");
            replyAndLogError(error, /*maskError*/ false);
            return;
        }

        if (authorizationMethod != BasicAuthorizationMethod) {
            rsp->SetStatus(EStatusCode::BadRequest);

            auto error = TError("Unsupported authorization method %Qlv", authorizationMethod);
            replyAndLogError(error, /*maskError*/ false);
            return;
        }

        auto credentials = Base64StrictDecode(encodedCredentials);
        TStringBuf user;
        TStringBuf password;
        if (!TStringBuf{credentials}.TrySplit(':', user, password)) {
            rsp->SetStatus(EStatusCode::BadRequest);

            auto error = TError("Failed to parse user credentials");
            replyAndLogError(error, /*maskError*/ false);
            return;
        }

        TUserInfo userInfo;
        try {
            userInfo = FetchUserInfo(TString{user});
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            if (error.FindMatching(NYTree::EErrorCode::ResolveError)) {
                HandleRegularRequest(rsp);

                error = TError("No such user %Qlv or user has no password set", user) << error;
                replyAndLogError(error, /*maskError*/ true, TString{user});
                return;
            }

            // Unknown error, reply 500.
            error = TError("Failed to fetch info for user %Qlv during logging", user) << error;
            replyAndLogError(error, /*maskError*/ false, TString{user});
            throw;
        }

        if (HashPassword(TString{password}, userInfo.PasswordSalt) != userInfo.HashedPassword) {
            HandleRegularRequest(rsp);

            auto error = TError("Invalid password");
            replyAndLogError(error, /*maskError*/ true, TString{user});
            return;
        }

        auto cookie = New<TCypressCookie>();
        cookie->Value = GenerateCookieValue();
        cookie->User = user;
        cookie->PasswordRevision = userInfo.PasswordRevision;
        cookie->ExpiresAt = TInstant::Now() + Config_->CookieExpirationTimeout;

        auto error = WaitFor(CookieStore_->RegisterCookie(cookie));
        if (!error.IsOK()) {
            error = TError("Failed to register cookie in cookie store");
            replyAndLogError(error, /*maskError*/ false, TString{user});
            // Will return 500.
            error.ThrowOnError();
        }

        YT_LOG_DEBUG("Issued new cookie for user (User: %v, CookieMD5: %v)",
            user,
            GetMD5HexDigestUpperCase(cookie->Value));

        if (const auto& redirectUrl = Config_->RedirectUrl) {
            rsp->SetStatus(EStatusCode::PermanentRedirect);
            rsp->GetHeaders()->Add("Location", *redirectUrl);
        } else {
            rsp->SetStatus(EStatusCode::OK);
        }

        rsp->GetHeaders()->Add(TString{SetCookieHedaer}, cookie->ToHeader(Config_));
    }

    void HandleRegularRequest(const IResponseWriterPtr& rsp)
    {
        rsp->SetStatus(EStatusCode::Unauthorized);

        rsp->GetHeaders()->Add("WWW-Authenticate", "Basic");
    }

    TUserInfo FetchUserInfo(const std::string& user)
    {
        auto path = Format("//sys/users/%v", ToYPathLiteral(user));

        static const std::string HashedPasswordAttribute = "hashed_password";
        static const std::string PasswordSaltAttribute = "password_salt";
        static const std::string PasswordRevisionAttribute = "password_revision";

        TGetNodeOptions options;
        options.Attributes = {
            HashedPasswordAttribute,
            PasswordSaltAttribute,
            PasswordRevisionAttribute,
        };

        auto rsp = WaitFor(Client_->GetNode(path, options))
            .ValueOrThrow();
        auto rspNode = ConvertToNode(rsp);
        const auto& attributes = rspNode->Attributes();

        return TUserInfo{
            .HashedPassword = attributes.Get<TString>(HashedPasswordAttribute),
            .PasswordSalt = attributes.Get<TString>(PasswordSaltAttribute),
            .PasswordRevision = attributes.Get<ui64>(PasswordRevisionAttribute),
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

IHttpHandlerPtr CreateCypressCookieLoginHandler(
    TCypressCookieGeneratorConfigPtr config,
    NApi::IClientPtr client,
    ICypressCookieStorePtr cookieStore)
{
    return New<TCypressCookieLoginHandler>(
        std::move(config),
        std::move(client),
        std::move(cookieStore));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
