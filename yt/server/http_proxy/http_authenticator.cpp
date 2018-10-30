#include "http_authenticator.h"

#include <yt/ytlib/auth/config.h>
#include <yt/ytlib/auth/token_authenticator.h>
#include <yt/ytlib/auth/cookie_authenticator.h>

#include <yt/core/http/http.h>
#include <yt/core/http/helpers.h>

#include <yt/core/ytree/fluent.h>

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

    auto result = WaitFor(Authenticate(req))
        .ValueOrThrow();

    rsp->SetStatus(EStatusCode::OK);
    if (result.CsrfToken) {
        ProtectCsrfToken(rsp);
    }
    
    ReplyJson(rsp, [&] (NYson::IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("login").Value(result.Login)
                .Item("realm").Value(result.Realm)
                .DoIf(result.CsrfToken.HasValue(), [&] (auto fluent) {
                    fluent.Item("csrf_token").Value(result.CsrfToken);
                })
            .EndMap();
    });
}

TFuture<TAuthenticationResult> THttpAuthenticator::Authenticate(
    const IRequestPtr& request)
{
    if (!Config_->RequireAuthentication) {
        return MakeFuture(TAuthenticationResult{"root", "YT"});
    }

    auto authorizationHeader = request->GetHeaders()->Find("Authorization");
    if (authorizationHeader) {
        TStringBuf prefix = "OAuth ";
        if (!authorizationHeader->StartsWith(prefix)) {
            return MakeFuture<TAuthenticationResult>(TError(
                NRpc::EErrorCode::InvalidCredentials,
                "Invalid value of Authorization header"));
        }

        TTokenCredentials credentials;
        credentials.Token = authorizationHeader->substr(prefix.Size());
        return TokenAuthenticator_->Authenticate(credentials);
    }

    auto cookieHeader = request->GetHeaders()->Find("Cookie");
    if (cookieHeader) {
        auto cookies = ParseCookies(*cookieHeader);

        TCookieCredentials credentials;
        credentials.UserIP = request->GetRemoteAddress();
        if (cookies.find("Session_id") == cookies.end()) {
            return MakeFuture<TAuthenticationResult>(TError(
                NRpc::EErrorCode::InvalidCredentials,
                "Request is missing \"Session_id\" cookie"));
        }
        if (cookies.find("sessionid2") == cookies.end()) {
            return MakeFuture<TAuthenticationResult>(TError(
                NRpc::EErrorCode::InvalidCredentials,
                "Request is missing \"sessionid2\" cookie"));
        }
        credentials.SessionId = cookies["Session_id"];
        credentials.SslSessionId = cookies["sessionid2"];
        return CookieAuthenticator_->Authenticate(credentials);
    }

    return MakeFuture<TAuthenticationResult>(TError(
        NRpc::EErrorCode::InvalidCredentials,
        "Client is missing credentials"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttpProxy
} // namespace NYT
