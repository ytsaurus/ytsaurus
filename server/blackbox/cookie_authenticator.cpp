#include "cookie_authenticator.h"
#include "helpers.h"
#include "private.h"

#include <util/string/split.h>

namespace NYT {
namespace NBlackbox {

using namespace NYTree;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = BlackboxLogger;

////////////////////////////////////////////////////////////////////////////////

// TODO(sandello): Indicate to end-used that cookie must be resigned.
class TCookieAuthenticator
    : public ICookieAuthenticator
{
public:
    TCookieAuthenticator(TCookieAuthenticatorConfigPtr config, IBlackboxServicePtr blackbox)
        : Config_(std::move(config))
        , Blackbox_(std::move(blackbox))
    { }

    virtual TFuture<TAuthenticationResult> Authenticate(
        const TString& sessionId,
        const TString& sslSessionId,
        const TString& host,
        const TString& userIP) override
    {
        auto sessionIdMD5 = ComputeMD5(sessionId);
        auto sslSessionIdMD5 = ComputeMD5(sslSessionId);
        LOG_DEBUG(
            "Authenticating user via session cookie (SessionIdMD5: %v, SslSessionIdMD5: %v)",
            sessionIdMD5,
            sslSessionIdMD5);
        return Blackbox_->Call("sessionid", {
                {"sessionid", sessionId}, {"sslsessionid", sslSessionId},
                {"host", host}, {"userip", userIP}})
            .Apply(BIND(
                &TCookieAuthenticator::OnCallResult,
                MakeStrong(this),
                std::move(sessionIdMD5),
                std::move(sslSessionIdMD5)));
    }

private:
    TFuture<TAuthenticationResult> OnCallResult(const TString& sessionIdMD5, const TString& sslSessionIdMD5, const INodePtr& data)
    {
        auto result = OnCallResultImpl(data);
        if (!result.IsOK()) {
            LOG_DEBUG(result, "Authentication failed (SessionIdMD5: %v, SslSessionIdMD5: %v)", sessionIdMD5, sslSessionIdMD5);
            result.Attributes().Set("sessionid_md5", sessionIdMD5);
            result.Attributes().Set("sslsessionid_md5", sslSessionIdMD5);
        } else {
            LOG_DEBUG(
                "Authentication successful (SessionIdMD5: %v, SslSessionIdMD5: %v, Login: %v, Realm: %v)",
                sessionIdMD5,
                sslSessionIdMD5,
                result.Value().Login,
                result.Value().Realm);
        }
        return MakeFuture(result);
    }

    TErrorOr<TAuthenticationResult> OnCallResultImpl(const INodePtr& data)
    {
        // See https://doc.yandex-team.ru/blackbox/reference/method-sessionid-response-json.xml for reference.
        auto statusId = GetByYPath<int>(data, "/status/id");
        if (!statusId.IsOK()) {
            return TError("Blackbox returned invalid response");
        }

        if (statusId.Value() != EBlackboxStatusId::Valid && statusId.Value() != EBlackboxStatusId::NeedReset) {
            auto error = GetByYPath<TString>(data, "/error");
            auto reason = error.IsOK() ? error.Value() : "unknown";
            return TError("Blackbox rejected session cookie")
                << TErrorAttribute("reason", reason);
        }

        auto login = GetByYPath<TString>(data, "/login");

        // Sanity checks.
        if (!login.IsOK()) {
            return TError("Blackbox returned invalid response") << login;
        }

        TAuthenticationResult result;
        result.Login = login.Value();
        result.Realm = "blackbox:cookie";
        return result;
    }

    const TCookieAuthenticatorConfigPtr Config_;
    const IBlackboxServicePtr Blackbox_;
};

ICookieAuthenticatorPtr CreateCookieAuthenticator(
    TCookieAuthenticatorConfigPtr config,
    IBlackboxServicePtr blackbox)
{
    return New<TCookieAuthenticator>(std::move(config), std::move(blackbox));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBlackbox
} // namespace NYT
