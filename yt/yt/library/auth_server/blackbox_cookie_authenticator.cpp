#include "blackbox_cookie_authenticator.h"

#include "blackbox_service.h"
#include "config.h"
#include "cookie_authenticator.h"
#include "credentials.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/core/crypto/crypto.h>

#include <util/string/split.h>

namespace NYT::NAuth {

using namespace NYTree;
using namespace NYPath;
using namespace NCrypto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

// TODO(sandello): Indicate to end-used that cookie must be resigned.
class TBlackboxCookieAuthenticator
    : public ICookieAuthenticator
{
public:
    TBlackboxCookieAuthenticator(
        TBlackboxCookieAuthenticatorConfigPtr config,
        IBlackboxServicePtr blackboxService)
        : Config_(std::move(config))
        , BlackboxService_(std::move(blackboxService))
    { }

    const std::vector<TStringBuf>& GetCookieNames() const override
    {
        static const std::vector<TStringBuf> cookieNames{
            BlackboxSessionIdCookieName,
            BlackboxSslSessionIdCookieName,
            BlackboxSessguardCookieName,
        };
        return cookieNames;
    }

    bool CanAuthenticate(const TCookieCredentials& credentials) const override
    {
        return credentials.Cookies.contains(BlackboxSessionIdCookieName);
    }

    TFuture<TAuthenticationResult> Authenticate(
        const TCookieCredentials& credentials) override
    {
        const auto& cookies = credentials.Cookies;
        auto sessionId = GetOrCrash(cookies, BlackboxSessionIdCookieName);

        std::vector<TErrorAttribute> errorAttributes;
        std::optional<TString> sslSessionId;
        auto cookieIt = cookies.find(BlackboxSslSessionIdCookieName);
        if (cookieIt != cookies.end()) {
            sslSessionId = cookieIt->second;
        }

        auto sessionIdMD5 = GetMD5HexDigestUpperCase(sessionId);
        errorAttributes.emplace_back("sessionid_md5", sessionIdMD5);
        TString sslSessionIdMD5 = "<empty>";
        if (sslSessionId) {
            sslSessionIdMD5 = GetMD5HexDigestUpperCase(*sslSessionId);
            errorAttributes.emplace_back("sslsessionid_md5", sslSessionIdMD5);
        }
        auto userIP = FormatUserIP(credentials.UserIP);

        std::optional<TString> sessguard;
        TString sessguardMD5 = "<empty>";
        std::string origin = Config_->Domain;
        auto sessguardIt = cookies.find(BlackboxSessguardCookieName);
        if (Config_->EnableSessguard && sessguardIt != cookies.end()) {
            sessguard = cookieIt->second;
            sessguardMD5 = GetMD5HexDigestUpperCase(*sessguard);;
            errorAttributes.emplace_back("sessguard_md5", sessguardMD5);
        }

        auto authArgs = Format("SessionIdMD5: %v, SslSessionIdMD5: %v, SessguardMD5: %v, UserIP: %v",
            sessionIdMD5,
            sslSessionIdMD5,
            sessguardMD5,
            userIP);

        YT_LOG_DEBUG("Authenticating user via session cookie (%v)",
            authArgs);

        THashMap<TString, TString> params{
            {"sessionid", sessionId},
            {"host", Config_->Domain},
            {"userip", userIP},
        };

        if (Config_->GetUserTicket) {
            params["get_user_ticket"] = "yes";
        }

        if (sslSessionId) {
            params["sslsessionid"] = *sslSessionId;
        }

        if (sessguard) {
            params["sessguard"] = *sessguard;
        }

        return BlackboxService_->Call("sessionid", params)
            .Apply(BIND(
                &TBlackboxCookieAuthenticator::OnCallResult,
                MakeStrong(this),
                std::move(authArgs),
                std::move(errorAttributes)));
    }

private:
    const TBlackboxCookieAuthenticatorConfigPtr Config_;
    const IBlackboxServicePtr BlackboxService_;

private:
    TFuture<TAuthenticationResult> OnCallResult(
        const TString& authArgs,
        const std::vector<TErrorAttribute>& errorAttributes,
        const INodePtr& data)
    {
        auto result = OnCallResultImpl(data);
        if (!result.IsOK()) {
            YT_LOG_DEBUG(result, "Authentication failed (%v)",
                authArgs);
            result <<= errorAttributes;
        } else {
            YT_LOG_DEBUG(
                "Authentication successful (%v, Login: %v, Realm: %v)",
                authArgs,
                result.Value().Login,
                result.Value().Realm);
        }
        return MakeFuture(result);
    }

    TErrorOr<TAuthenticationResult> OnCallResultImpl(const INodePtr& data)
    {
        auto statusId = GetByYPath<i64>(data, "/status/id");
        if (!statusId.IsOK()) {
            return TError("Blackbox returned invalid response");
        }

        auto status = TryCheckedEnumCast<EBlackboxStatus>(statusId.Value());
        if (status != EBlackboxStatus::Valid && status != EBlackboxStatus::NeedReset) {
            auto error = GetByYPath<TString>(data, "/error");
            auto reason = error.IsOK() ? error.Value() : "unknown";
            auto statusString = status.has_value() ? Format("%lv", *status) : "unknown";
            return TError(NRpc::EErrorCode::InvalidCredentials, "Blackbox rejected session cookie")
                << TErrorAttribute("reason", reason)
                << TErrorAttribute("blackbox_status_string", statusString);
        }

        auto login = BlackboxService_->GetLogin(data);

        // Sanity checks.
        if (!login.IsOK()) {
            return TError("Blackbox returned invalid response")
                << login;
        }

        TAuthenticationResult result;
        result.Login = login.Value();
        result.Realm = "blackbox:cookie";
        auto userTicket = GetByYPath<TString>(data, "/user_ticket");
        if (userTicket.IsOK()) {
            result.UserTicket = userTicket.Value();
        } else if (Config_->GetUserTicket) {
            return TError("Failed to retrieve user ticket");
        }
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

ICookieAuthenticatorPtr CreateBlackboxCookieAuthenticator(
    TBlackboxCookieAuthenticatorConfigPtr config,
    IBlackboxServicePtr blackboxService)
{
    return New<TBlackboxCookieAuthenticator>(std::move(config), std::move(blackboxService));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
