#include "cypress_token_authenticator.h"

#include "config.h"
#include "credentials.h"
#include "token_authenticator.h"
#include "private.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/crypto/crypto.h>

namespace NYT::NAuth {

using namespace NApi;
using namespace NConcurrency;
using namespace NCrypto;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = AuthLogger;

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
            .ApplyUnique(BIND(
                &TCypressTokenAuthenticator::OnCallTokenResult,
                MakeStrong(this)))
            .ApplyUnique(BIND(
                &TCypressTokenAuthenticator::WrapError,
                tokenHash));
    }

private:
    const TCypressTokenAuthenticatorConfigPtr Config_;
    const IClientPtr Client_;

    static TAuthenticationResult WrapError(const std::string& tokenHash, TErrorOr<TAuthenticationResult>&& rspOrError)
    {
        try {
            auto& result = rspOrError.ValueOrThrow();
            YT_LOG_DEBUG("Cypress authentication succeeded (TokenHash: %v, Login: %v)",
                tokenHash,
                result.Login);
            return std::move(result);
        } catch (const std::exception& ex) {
            auto error = TError("Cypress authentication failed")
                << TErrorAttribute("token_hash", tokenHash)
                << ex;
            YT_LOG_DEBUG(error, "Cypress authentication failed");
            THROW_ERROR(error);
        }
    }

    TFuture<TAuthenticationResult> OnCallTokenResult(TErrorOr<TYsonString>&& rspOrError)
    {
        if (!rspOrError.IsOK() && rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::InvalidCredentials,
                "Token is missing in Cypress")
                << rspOrError;
        }
        auto tokenNode = ConvertTo<INodePtr>(rspOrError.ValueOrThrow());
        const auto& tokenAttributes = tokenNode->Attributes();

        auto userIdAttribute = tokenAttributes.Find<TObjectId>("user_id");
        if (userIdAttribute) {
            // New authentication schema: now we need to get the username given the user ID which we received.
            auto path = Format("%v/@name", FromObjectId(*userIdAttribute));
            TGetNodeOptions options;
            options.ReadFrom = EMasterChannelKind::Cache;
            return Client_->GetNode(path, options)
                .ApplyUnique(BIND(
                    &TCypressTokenAuthenticator::OnCallUsernameResult,
                    MakeStrong(this),
                    std::move(*userIdAttribute)));
        }

        auto userAttribute = tokenAttributes.Find<std::string>("user");
        if (userAttribute) {
            // Old authentication schema: we already retrieved the username.
            return MakeFuture(TAuthenticationResult{
                .Login = std::move(*userAttribute),
            });
        }

        THROW_ERROR_EXCEPTION("Missing attributes \"user_id\" and \"user\" on token Cypress node");
    }

    TAuthenticationResult OnCallUsernameResult(
        const TObjectId& userId,
        TErrorOr<TYsonString>&& rspOrError)
    {
        try {
            auto login = ConvertTo<std::string>(rspOrError.ValueOrThrow());
            return TAuthenticationResult{
                .Login = std::move(login),
            };
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error resolving user_id")
                << TErrorAttribute("user_id", userId)
                << ex;
        }
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
