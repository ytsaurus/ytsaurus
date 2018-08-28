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
    auto auth = WaitFor(Authenticate(req))
        .ValueOrThrow();

    rsp->SetStatus(EStatusCode::OK);
    if (auth.CsrfToken) {
        ProtectCsrfToken(rsp);
    }
    
    ReplyJson(rsp, [&] (NYson::IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("login").Value(auth.Login)
                .Item("realm").Value(auth.Realm)
                .DoIf(auth.CsrfToken.HasValue(), [&] (auto fluent) {
                    fluent.Item("csrf_token").Value(auth.CsrfToken);
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
            THROW_ERROR_EXCEPTION("Invalid value of Authorization header");
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
            THROW_ERROR_EXCEPTION("Request is missing \"Session_id\" cookie");
        }
        if (cookies.find("sessionid2") == cookies.end()) {
            THROW_ERROR_EXCEPTION("Request is missing \"sessionid2\" cookie");
        }
        credentials.SessionId = cookies["Session_id"];
        credentials.SslSessionId = cookies["sessionid2"];
        credentials.Domain = "yt.yandex-team.ru";
        return CookieAuthenticator_->Authenticate(credentials);
    }

    THROW_ERROR_EXCEPTION("Client is missing credentials");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttpProxy
} // namespace NYT
