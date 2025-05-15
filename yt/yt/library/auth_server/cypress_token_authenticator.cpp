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

        // Try to retrieve both old (@user) and new (@user_id) token attributes
        // at the same time to speed up the process.
        auto path = Format("%v/%v",
            Config_->RootPath ? Config_->RootPath : "//sys/cypress_tokens",
            ToYPathLiteral(tokenHash));
        auto options = TGetNodeOptions{
            .Attributes = TAttributeFilter({"user", "user_id"}),
        };
        options.ReadFrom = EMasterChannelKind::Cache;
        return Client_->GetNode(path, options)
            .Apply(BIND(
                &TCypressTokenAuthenticator::OnCallTokenResult,
                MakeStrong(this),
                tokenHash));
    }

private:
    const TCypressTokenAuthenticatorConfigPtr Config_;
    const IClientPtr Client_;

    TFuture<TAuthenticationResult> OnCallTokenResult(
        const TString& tokenHash,
        const TErrorOr<TYsonString>& rspOrError)
    {
        if (!rspOrError.IsOK()) {
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
        auto tokenNode = ConvertTo<INodePtr>(rspOrError.Value());
        const auto& tokenAttributes = tokenNode->Attributes();

        auto userIdAttribute = tokenAttributes.Find<TString>("user_id");
        if (userIdAttribute) {
            // New authentication schema: now we need to get the username given the user ID which we received.
            auto path = Format("#%v/@name", ToYPathLiteral(*userIdAttribute));
            TGetNodeOptions options{};
            options.ReadFrom = EMasterChannelKind::Cache;
            return Client_->GetNode(path, options)
                .Apply(BIND(
                    &TCypressTokenAuthenticator::OnCallUsernameResult,
                    MakeStrong(this),
                    std::move(tokenHash),
                    std::move(*userIdAttribute)));
        }

        auto userAttribute = tokenAttributes.Find<TString>("user");
        if (userAttribute) {
            // Old authentication schema: we already retrieved the username.
            YT_LOG_DEBUG("Cypress authentication succeeded (TokenHash: %v, Login: %v)",
                tokenHash,
                *userAttribute);
            return MakeFuture(TAuthenticationResult{
                .Login = *userAttribute,
            });
        }

        YT_LOG_DEBUG(rspOrError, "Cypress authentication failed (TokenHash: %v)",
            tokenHash);
        THROW_ERROR_EXCEPTION("Cypress authentication failed")
            << TErrorAttribute("token_hash", tokenHash);
    }

    TAuthenticationResult OnCallUsernameResult(
        const TString& tokenHash,
        const TString& userId,
        const TErrorOr<TYsonString>& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            YT_LOG_DEBUG(rspOrError, "Cypress authentication failed (User ID: %v)",
                    userId);
            if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                THROW_ERROR_EXCEPTION("Could not get username by user ID")
                    << TErrorAttribute("token_hash", userId)
                    << rspOrError;
            } else {
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
