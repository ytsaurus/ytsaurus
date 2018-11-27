#include "http_authenticator.h"

#include <yt/ytlib/auth/config.h>
#include <yt/ytlib/auth/token_authenticator.h>
#include <yt/ytlib/auth/cookie_authenticator.h>

#include <yt/core/http/http.h>
#include <yt/core/http/helpers.h>

#include <yt/core/ytree/fluent.h>

#include <util/string/strip.h>

namespace NYT {
namespace NHttpProxy {

using namespace NAuth;
using namespace NHttp;
using namespace NYTree;
using namespace NConcurrency;

DEFINE_REFCOUNTED_TYPE(THttpAuthenticator)

////////////////////////////////////////////////////////////////////////////////

THttpAuthenticator::THttpAuthenticator(
    TAuthenticationManagerConfigPtr config,
    ITokenAuthenticatorPtr tokenAuthenticator,
    ICookieAuthenticatorPtr cookieAuthenticator)
    : Config_(config)
    , TokenAuthenticator_(tokenAuthenticator)
    , CookieAuthenticator_(cookieAuthenticator)
{
    YCHECK(TokenAuthenticator_);
    YCHECK(CookieAuthenticator_);
}

void THttpAuthenticator::HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp)
{
    if (MaybeHandleCors(req, rsp)) {
        return;
    }

    auto result = Authenticate(req, true);
    if (result.IsOK()) {
        rsp->SetStatus(EStatusCode::OK);
        ProtectCsrfToken(rsp);

        TString csrfSecret = Config_->GetCsrfSecret();
        auto csrfToken = SignCsrfToken(result.Value().Login, csrfSecret, TInstant::Now());

        ReplyJson(rsp, [&] (NYson::IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("login").Value(result.Value().Login)
                    .Item("realm").Value(result.Value().Realm)
                    .Item("csrf_token").Value(csrfToken)
                .EndMap();
        });
    } else {
        rsp->SetStatus(EStatusCode::InternalServerError);
        ReplyJson(rsp, [&] (auto consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("error").Value(TError(result))
                .EndMap();
        });
    }
}

TErrorOr<TAuthenticationResult> THttpAuthenticator::Authenticate(
    const IRequestPtr& request,
    bool disableCsrfTokenCheck)
{
    if (!Config_->RequireAuthentication) {
        return TAuthenticationResult{"root", "YT"};
    }

    auto authorizationHeader = request->GetHeaders()->Find("Authorization");
    if (authorizationHeader) {
        TStringBuf prefix = "OAuth ";
        if (!authorizationHeader->StartsWith(prefix)) {
            return TError(NRpc::EErrorCode::InvalidCredentials, "Invalid value of Authorization header");
        }

        TTokenCredentials credentials;
        credentials.Token = authorizationHeader->substr(prefix.Size());
        return WaitFor(TokenAuthenticator_->Authenticate(credentials));
    }

    auto cookieHeader = request->GetHeaders()->Find("Cookie");
    if (cookieHeader) {
        auto cookies = ParseCookies(*cookieHeader);

        TCookieCredentials credentials;
        credentials.UserIP = request->GetRemoteAddress();
        if (cookies.find("Session_id") == cookies.end()) {
            return TError(NRpc::EErrorCode::InvalidCredentials, "Request is missing \"Session_id\" cookie");
        }
        if (cookies.find("sessionid2") == cookies.end()) {
            return TError(NRpc::EErrorCode::InvalidCredentials, "Request is missing \"sessionid2\" cookie");
        }
        credentials.SessionId = cookies["Session_id"];
        credentials.SslSessionId = cookies["sessionid2"];

        auto authResult = WaitFor(CookieAuthenticator_->Authenticate(credentials));
        if (!authResult.IsOK()) {
            return authResult;
        }

        if (request->GetMethod() != EMethod::Get && !disableCsrfTokenCheck) {
            auto csrfTokenHeader = request->GetHeaders()->Find("X-Csrf-Token");
            if (!csrfTokenHeader) {
                return TError(NRpc::EErrorCode::InvalidCredentials, "CSRF token is missing");
            }

            auto error = CheckCsrfToken(
                Strip(*csrfTokenHeader),
                authResult.Value().Login,
                Config_->GetCsrfSecret(),
                Config_->GetCsrfTokenExpirationTime());

            if (!error.IsOK()) {
                return error;
            }
        }

        return authResult;
    }

    return TError(NRpc::EErrorCode::InvalidCredentials, "Client is missing credentials");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttpProxy
} // namespace NYT
