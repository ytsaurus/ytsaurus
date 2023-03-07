#include "http_authenticator.h"

#include "coordinator.h"

#include <yt/ytlib/auth/config.h>
#include <yt/ytlib/auth/token_authenticator.h>
#include <yt/ytlib/auth/cookie_authenticator.h>
#include <yt/ytlib/auth/helpers.h>

#include <yt/core/http/http.h>
#include <yt/core/http/helpers.h>

#include <yt/core/ytree/fluent.h>

#include <util/string/strip.h>

namespace NYT::NHttpProxy {

using namespace NAuth;
using namespace NHttp;
using namespace NYTree;
using namespace NConcurrency;

DEFINE_REFCOUNTED_TYPE(THttpAuthenticator)

////////////////////////////////////////////////////////////////////////////////

void SetStatusFromAuthError(const NHttp::IResponseWriterPtr& rsp, const TError& error)
{
    if (error.FindMatching(NRpc::EErrorCode::InvalidCredentials)) {
        rsp->SetStatus(EStatusCode::Unauthorized);
    } else if (error.FindMatching(NRpc::EErrorCode::InvalidCsrfToken)) {
        rsp->SetStatus(EStatusCode::Unauthorized);
    } else {
        rsp->SetStatus(EStatusCode::ServiceUnavailable);
    }
}

////////////////////////////////////////////////////////////////////////////////

THttpAuthenticator::THttpAuthenticator(
    TAuthenticationManagerConfigPtr config,
    ITokenAuthenticatorPtr tokenAuthenticator,
    ICookieAuthenticatorPtr cookieAuthenticator,
    TCoordinatorPtr coordinator)
    : Config_(config)
    , TokenAuthenticator_(tokenAuthenticator)
    , CookieAuthenticator_(cookieAuthenticator)
    , Coordinator_(coordinator)
{
    YT_VERIFY(TokenAuthenticator_);
    YT_VERIFY(CookieAuthenticator_);
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

        auto csrfSecret = Config_->GetCsrfSecret();
        auto csrfToken = SignCsrfToken(result.Value().Result.Login, csrfSecret, TInstant::Now());

        ReplyJson(rsp, [&] (NYson::IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("login").Value(result.Value().Result.Login)
                    .Item("realm").Value(result.Value().Result.Realm)
                    .Item("csrf_token").Value(csrfToken)
                .EndMap();
        });
    } else {
        SetStatusFromAuthError(rsp, TError(result));
        ReplyJson(rsp, [&] (auto consumer) {
            BuildYsonFluently(consumer)
                .Value(TError(result));
        });
    }
}

TErrorOr<TAuthenticationResultAndToken> THttpAuthenticator::Authenticate(
    const IRequestPtr& request,
    bool disableCsrfTokenCheck)
{
    if (!Config_->RequireAuthentication) {
        return TAuthenticationResultAndToken{TAuthenticationResult{"root", "YT"}, TString()};
    }

    auto userIP = request->GetRemoteAddress();
    auto realIP = GetBalancerRealIP(request);
    if (realIP) {
        auto parsedRealIP = NNet::TNetworkAddress::TryParse(*realIP);
        if (parsedRealIP.IsOK()) {
            userIP = parsedRealIP.ValueOrThrow();
        }
    }

    NTracing::TChildTraceContextGuard authSpan("HttpProxy.Auth");

    auto authorizationHeader = request->GetHeaders()->Find("Authorization");
    auto tokenHash = TString();
    if (authorizationHeader) {
        TStringBuf prefix = "OAuth ";
        if (!authorizationHeader->StartsWith(prefix)) {
            return TError(NRpc::EErrorCode::InvalidCredentials, "Invalid value of Authorization header");
        }

        TTokenCredentials credentials;
        credentials.UserIP = userIP;
        credentials.Token = authorizationHeader->substr(prefix.size());
        tokenHash = GetCryptoHash(credentials.Token);
        if (!credentials.Token.empty()) {
            auto rsp = WaitFor(TokenAuthenticator_->Authenticate(credentials));
            if (!rsp.IsOK()) {
                return TError(rsp);
            } else {
                return TAuthenticationResultAndToken{rsp.Value(), tokenHash};
            }
        }
    }

    auto cookieHeader = request->GetHeaders()->Find("Cookie");
    if (cookieHeader) {
        auto cookies = ParseCookies(*cookieHeader);

        TCookieCredentials credentials;
        credentials.UserIP = userIP;
        if (cookies.find("Session_id") == cookies.end()) {
            return TError(NRpc::EErrorCode::InvalidCredentials, "Request is missing \"Session_id\" cookie");
        }
        credentials.SessionId = cookies["Session_id"];

        if (cookies.find("sessionid2") != cookies.end()) {
            credentials.SslSessionId = cookies["sessionid2"];
        }

        auto authResult = WaitFor(CookieAuthenticator_->Authenticate(credentials));
        if (!authResult.IsOK()) {
            return TError(authResult);
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

            auto dynamicConfig = Coordinator_->GetDynamicConfig();
            if (!error.IsOK() && !dynamicConfig->RelaxCsrfCheck) {
                return error;
            }
        }

        return TAuthenticationResultAndToken{authResult.Value(), tokenHash};
    }

    return TError(NRpc::EErrorCode::InvalidCredentials, "Client is missing credentials");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
