#include "cypress_token_authenticator.h"

#include "config.h"
#include "credentials.h"
#include "token_authenticator.h"
#include "private.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/crypto/crypto.h>

namespace NYT::NAuth {

using namespace NApi;
using namespace NConcurrency;
using namespace NCrypto;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

class TCypressTokenAuthenticator
    : public ITokenAuthenticator
{
public:
    TCypressTokenAuthenticator(TCypressTokenAuthenticatorConfigPtr config, IClientPtr client)
        : Config_(std::move(config))
        , Client_(std::move(client))
    { }

    TFuture<TAuthenticationResult> Authenticate(
        const TTokenCredentials& credentials) override
    {
        const auto& token = credentials.Token;
        const auto& userIP = credentials.UserIP;
        auto tokenHash = GetSha256HexDigestLowerCase(token);
        YT_LOG_DEBUG("Authenticating user with Cypress token (TokenHash: %v, UserIP: %v)",
            tokenHash,
            userIP);

        // Firstly, try to authenticate the user using the newer @user_id attribute.
        auto path = Format("%v/%v/@user_id",
            Config_->RootPath ? Config_->RootPath : "//sys/cypress_tokens",
            ToYPathLiteral(tokenHash));
        return Client_->GetNode(path, /*options*/ {})
            .Apply(BIND(
                &TCypressTokenAuthenticator::OnCallLoginResult,
                MakeStrong(this),
                tokenHash,
                true));
    }

private:
    const TCypressTokenAuthenticatorConfigPtr Config_;
    const IClientPtr Client_;

    TFuture<TAuthenticationResult> OnCallLoginResult(
        const TString& tokenHash,
        bool withUserIdAttribute,
        const TErrorOr<TYsonString>& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            if (withUserIdAttribute) {
                // Fallback to the old token schema using the @user attribute.
                auto path = Format("%v/%v/@user",
                    Config_->RootPath ? Config_->RootPath : "//sys/cypress_tokens",
                    ToYPathLiteral(tokenHash));
                return Client_->GetNode(path, /*options*/ {})
                    .Apply(BIND(
                        &TCypressTokenAuthenticator::OnCallLoginResult,
                        MakeStrong(this),
                        tokenHash,
                        false));
            }

            if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                YT_LOG_DEBUG(rspOrError, "Token is missing in Cypress (TokenHash: %v)",
                    tokenHash);
                THROW_ERROR_EXCEPTION(NRpc::EErrorCode::InvalidCredentials,
                    "Token is missing in Cypress")
                    << TErrorAttribute("token_hash", tokenHash)
                    << rspOrError;
            } else {
                YT_LOG_DEBUG(rspOrError, "Cypress authentication failed (TokenHash: %v)",
                    tokenHash);
                THROW_ERROR_EXCEPTION("Cypress authentication failed")
                    << TErrorAttribute("token_hash", tokenHash)
                    << rspOrError;
            }
        }

        auto login = ConvertTo<TString>(rspOrError.Value());
        if (withUserIdAttribute) {
            // We need to get the username given the user ID which we received.
            auto path = Format("#%v/@name", ToYPathLiteral(login));
            return Client_->GetNode(path, /*options*/ {})
                .Apply(BIND(
                    &TCypressTokenAuthenticator::OnCallUsernameResult,
                    MakeStrong(this),
                    std::move(tokenHash),
                    std::move(login)));
        }

        YT_LOG_DEBUG("Cypress authentication succeeded (TokenHash: %v, Login: %v)",
            tokenHash,
            login);
        return MakeFuture(TAuthenticationResult{
            .Login = login,
        });
    }

    TAuthenticationResult OnCallUsernameResult(
        const TString& tokenHash,
        const TString& userId,
        const TErrorOr<TYsonString>& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                YT_LOG_DEBUG(rspOrError, "Could not get username by user ID (User ID: %v)",
                    userId);
                THROW_ERROR_EXCEPTION("Could not get username by user ID")
                    << TErrorAttribute("token_hash", userId)
                    << rspOrError;
            } else {
                YT_LOG_DEBUG(rspOrError, "Cypress authentication failed (User ID: %v)",
                    userId);
                THROW_ERROR_EXCEPTION("Cypress authentication failed")
                    << TErrorAttribute("user_id", userId)
                    << rspOrError;
            }
        }

        auto login = ConvertTo<TString>(rspOrError.Value());
        YT_LOG_DEBUG("Cypress authentication succeeded (TokenHash: %v, Login: %v)",
            tokenHash,
            login);
        return TAuthenticationResult{
            .Login = login,
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

ITokenAuthenticatorPtr CreateCypressTokenAuthenticator(
    TCypressTokenAuthenticatorConfigPtr config,
    IClientPtr client)
{
    return New<TCypressTokenAuthenticator>(std::move(config), std::move(client));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
