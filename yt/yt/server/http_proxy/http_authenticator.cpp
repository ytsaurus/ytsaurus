#include "http_authenticator.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/ytlib/auth/config.h>
#include <yt/yt/ytlib/auth/token_authenticator.h>
#include <yt/yt/ytlib/auth/ticket_authenticator.h>
#include <yt/yt/ytlib/auth/cookie_authenticator.h>
#include <yt/yt/ytlib/auth/helpers.h>
#include <yt/yt/ytlib/auth/tvm_service.h>
#include <yt/yt/ytlib/auth/authentication_manager.h>

#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/helpers.h>

#include <yt/yt/core/ytree/fluent.h>

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

THttpAuthenticator::THttpAuthenticator(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , Config_(Bootstrap_->GetConfig()->Auth)
    , AuthenticationManager_(Bootstrap_->GetAuthenticationManager())
    , TokenAuthenticator_(Bootstrap_->GetTokenAuthenticator())
    , CookieAuthenticator_(Bootstrap_->GetCookieAuthenticator())
{
    YT_VERIFY(Config_);
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
        TString user = "root";
        if (auto userNameHeader = request->GetHeaders()->Find("X-YT-Testing-User-Name")) {
            user = *userNameHeader;
        }
        static const auto UserTicket = TString();
        return TAuthenticationResultAndToken{TAuthenticationResult{user, "YT", UserTicket}, TString()};
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

    static const TString AuthorizationHeaderName("Authorization");
    if (auto authorizationHeader = request->GetHeaders()->Find(AuthorizationHeaderName)) {
        static const TStringBuf Prefix = "OAuth ";
        if (!authorizationHeader->StartsWith(Prefix)) {
            return TError(
                NRpc::EErrorCode::InvalidCredentials,
                "Malformed Authorization header");
        }

        TTokenCredentials credentials{
            .Token = authorizationHeader->substr(Prefix.size()),
            .UserIP = userIP
        };

        if (!credentials.Token.empty()) {
            if (!TokenAuthenticator_) {
                return TError(
                    NRpc::EErrorCode::InvalidCredentials,
                    "Client has provided a token but no token authenticator is configured");
            }

            auto rsp = WaitFor(TokenAuthenticator_->Authenticate(credentials));
            if (!rsp.IsOK()) {
                return TError(rsp);
            }

            auto tokenHash = GetCryptoHash(credentials.Token);
            return TAuthenticationResultAndToken{rsp.Value(), tokenHash};
        }
    }

    static const TString CookieHeaderName("Cookie");
    if (auto cookieHeader = request->GetHeaders()->Find(CookieHeaderName)) {
        auto cookies = ParseCookies(*cookieHeader);

        TCookieCredentials credentials;
        credentials.UserIP = userIP;
        static const TString SessionIdCookieName("Session_id");
        auto sessionIdIt = cookies.find(SessionIdCookieName);
        if (sessionIdIt == cookies.end()) {
            return TError(
                NRpc::EErrorCode::InvalidCredentials,
                "Request is missing %Qv cookie",
                SessionIdCookieName);
        }
        credentials.SessionId = sessionIdIt->second;

        static const TString SessionId2CookieName("sessionid2");
        auto sessionId2It = cookies.find(SessionId2CookieName);
        if (sessionId2It != cookies.end()) {
            credentials.SslSessionId = sessionId2It->second;
        }

        if (!CookieAuthenticator_) {
            return TError(
                NRpc::EErrorCode::InvalidCredentials,
                "Client has provided a cookie but no cookie authenticator is configured");
        }

        auto authResult = WaitFor(CookieAuthenticator_->Authenticate(credentials));
        if (!authResult.IsOK()) {
            return TError(authResult);
        }

        if (request->GetMethod() != EMethod::Get && !disableCsrfTokenCheck) {
            static const TString CrfTokenHeaderName("X-Csrf-Token");
            auto csrfTokenHeader = request->GetHeaders()->Find(CrfTokenHeaderName);
            if (!csrfTokenHeader) {
                return TError(
                    NRpc::EErrorCode::InvalidCredentials,
                    "CSRF token is missing");
            }

            auto error = CheckCsrfToken(
                Strip(*csrfTokenHeader),
                authResult.Value().Login,
                Config_->GetCsrfSecret(),
                Config_->GetCsrfTokenExpirationTime());

            auto dynamicConfig = Bootstrap_->GetDynamicConfig();
            if (!error.IsOK() && !dynamicConfig->RelaxCsrfCheck) {
                return error;
            }
        }

        return TAuthenticationResultAndToken{authResult.Value(), TString()};
    }

    static const TString UserTicketHeaderName("X-Ya-User-Ticket");
    if (auto userTicketHeader = request->GetHeaders()->Find(UserTicketHeaderName)) {
        const auto& ticketAuthenticator = AuthenticationManager_->GetTicketAuthenticator();

        TTicketCredentials credentials;
        credentials.Ticket = *userTicketHeader;

        if (!ticketAuthenticator) {
            return TError(
                NRpc::EErrorCode::InvalidCredentials,
                "Client has provided a user ticket, but no ticket authenticator is configured");
        }

        auto authResult = WaitFor(ticketAuthenticator->Authenticate(credentials));
        if (!authResult.IsOK()) {
            return TError(authResult);
        }

        return TAuthenticationResultAndToken{authResult.Value(), {}};
    }

    static const TString ServiceTicketHeaderName("X-Ya-Service-Ticket");
    if (auto serviceTicketHeader = request->GetHeaders()->Find(ServiceTicketHeaderName)) {
        const auto& ticketAuthenticator = AuthenticationManager_->GetTicketAuthenticator();

        TServiceTicketCredentials credentials;
        credentials.Ticket = *serviceTicketHeader;

        if (!ticketAuthenticator) {
            return TError(
                NRpc::EErrorCode::InvalidCredentials,
                "Client has provided a service ticket, but no ticket authenticator is configured");
        }

        auto authResult = WaitFor(ticketAuthenticator->Authenticate(credentials));
        if (!authResult.IsOK()) {
            return TError(authResult);
        }

        return TAuthenticationResultAndToken{authResult.Value(), {}};
    }

    return TError(
        NRpc::EErrorCode::InvalidCredentials,
        "Client is missing credentials");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
