#include "cypress_cookie_authenticator.h"

#include "config.h"
#include "credentials.h"
#include "cypress_cookie_store.h"
#include "private.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/library/auth_server/cookie_authenticator.h>
#include <yt/yt/library/auth_server/helpers.h>
#include <yt/yt/library/auth_server/private.h>

#include <yt/yt/core/crypto/crypto.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NAuth {

using namespace NApi;
using namespace NConcurrency;
using namespace NCrypto;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

class TCypressCookieAuthenticator
    : public ICookieAuthenticator
{
public:
    TCypressCookieAuthenticator(
        TCypressCookieGeneratorConfigPtr config,
        ICypressCookieStorePtr cookieStore,
        IClientPtr client)
        : Config_(std::move(config))
        , CookieStore_(std::move(cookieStore))
        , Client_(std::move(client))
    { }

    const std::vector<TStringBuf>& GetCookieNames() const override
    {
        static const std::vector<TStringBuf> cookieNames{
            CypressCookieName,
        };
        return cookieNames;
    }

    bool CanAuthenticate(const TCookieCredentials& credentials) const override
    {
        return credentials.Cookies.contains(CypressCookieName);
    }

    TFuture<TAuthenticationResult> Authenticate(
        const TCookieCredentials& credentials) override
    {
        const auto& cookieValue = GetOrCrash(credentials.Cookies, CypressCookieName);

        YT_TLOG_DEBUG("Authenticating user via native cookie")
            .With("CookieMD5", GetMD5HexDigestUpperCase(cookieValue))
            .With("UserIP", FormatUserIP(credentials.UserIP));

        return CookieStore_->GetCookie(cookieValue)
            .Apply(BIND(&TCypressCookieAuthenticator::OnGotCookie, MakeStrong(this)))
            .Apply(BIND([] (const TErrorOr<TAuthenticationResult>& resultOrError) -> TErrorOr<TAuthenticationResult> {
                if (resultOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                    return TError(
                        NRpc::EErrorCode::InvalidCredentials,
                        "Unknown credentials")
                        .With(resultOrError);
                }

                return resultOrError;
            }));
    }

private:
    const TCypressCookieGeneratorConfigPtr Config_;

    const ICypressCookieStorePtr CookieStore_;

    const IClientPtr Client_;

    TFuture<ui64> GetUserPasswordRevision(const std::string& user, TStringBuf attribute)
    {
        auto path = Format("//sys/users/%v", ToYPathLiteral(user));

        TGetNodeOptions options{
            .Attributes = {attribute},
        };

        return Client_->GetNode(path, options)
            .Apply(BIND([attribute] (const TYsonString& rsp) {
                // ldap_password_revision may not exist on the user node — default to 0.
                return ConvertToNode(rsp)->Attributes().Get<ui64>(attribute, /*default*/ 0);
            }));
    }

    TFuture<TAuthenticationResult> OnGotCookie(const TCypressCookiePtr& cookie)
    {
        const auto& attribute = (cookie->AuthSource == EAuthSource::Ldap)
            ? LdapPasswordRevisionAttribute
            : PasswordRevisionAttribute;

        return GetUserPasswordRevision(cookie->User, attribute)
            .Apply(BIND(&TCypressCookieAuthenticator::OnGotPasswordRevision, MakeStrong(this), cookie)
                .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker()));
    }

    TAuthenticationResult OnGotPasswordRevision(
        const TCypressCookiePtr& cookie,
        ui64 passwordRevision)
    {
        if (cookie->PasswordRevision != passwordRevision) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::InvalidCredentials,
                "Native cookie was issued for previous password revision")
                .With("cookie_password_revision", cookie->PasswordRevision)
                .With("password_revision", passwordRevision);
        }

        auto now = TInstant::Now();
        if (cookie->ExpiresAt < now) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::InvalidCredentials,
                "Native cookie expired")
                .With("cookie_expiration_time", cookie->ExpiresAt);
        }

        const auto& user = cookie->User;
        TAuthenticationResult result{
            .Login = user,
        };

        if (cookie->AuthSource == EAuthSource::Cypress &&
            cookie->ExpiresAt < now + Config_->CookieRenewalPeriod)
        {
            auto latestCookie = CookieStore_->GetLastCookieForUser(user);

            // Very unlikely, but might happen during cookie duration reconfiguration.
            if (latestCookie && latestCookie->PasswordRevision != passwordRevision) {
                CookieStore_->RemoveLastCookieForUser(user);
                latestCookie.Reset();
            }

            if (latestCookie && latestCookie->ExpiresAt > now + Config_->CookieRenewalPeriod) {
                result.SetCookie = latestCookie->ToHeader(Config_);
            } else {
                auto expirationTimeout = (cookie->AuthSource == EAuthSource::Ldap)
                    ? Config_->LdapCookieExpirationTimeout
                    : Config_->CookieExpirationTimeout;

                auto newCookie = New<TCypressCookie>();
                newCookie->Value = GenerateCookieValue();
                newCookie->User = user;
                newCookie->AuthSource = cookie->AuthSource;
                newCookie->PasswordRevision = passwordRevision;
                newCookie->ExpiresAt = TInstant::Now() + expirationTimeout;

                YT_TLOG_DEBUG("Issuing new cookie for renewal")
                    .With("User", user)
                    .With("CookieMD5", GetMD5HexDigestUpperCase(newCookie->Value))
                    .With("PasswordRevision", passwordRevision)
                    .With("ExpiresAt", newCookie->ExpiresAt);

                auto error = WaitFor(CookieStore_->RegisterCookie(newCookie));
                if (error.IsOK()) {
                    YT_TLOG_DEBUG("Issued new cookie for renewal")
                        .With("User", user)
                        .With("CookieMD5", GetMD5HexDigestUpperCase(newCookie->Value));
                    result.SetCookie = newCookie->ToHeader(Config_);
                } else {
                    // NB: Cookie creation failure should not lead to authentication error.
                    YT_TLOG_DEBUG("Failed to issue new cookie for renewal")
                        .With("User", user)
                        .With("CookieMD5", GetMD5HexDigestUpperCase(newCookie->Value))
                        .With(error);
                }
            }
        }

        std::optional<std::string> setCookieMD5;
        if (auto setCookie = result.SetCookie) {
            setCookieMD5 = GetMD5HexDigestUpperCase(*setCookie);
        }

        YT_TLOG_DEBUG("User authenticated")
            .With("User", user)
            .With("CookieMD5", GetMD5HexDigestUpperCase(cookie->Value))
            .With("SetCookieMD5", setCookieMD5);

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

ICookieAuthenticatorPtr CreateCypressCookieAuthenticator(
    TCypressCookieGeneratorConfigPtr config,
    ICypressCookieStorePtr cookieStore,
    IClientPtr client)
{
    return New<TCypressCookieAuthenticator>(
        std::move(config),
        std::move(cookieStore),
        std::move(client));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
