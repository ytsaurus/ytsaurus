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
        if (!credentials.Token && !credentials.TokenSha256) {
            return MakeFuture<TAuthenticationResult>(TError("Token or token sha256 must be provided to authenticate"));
        }

        const auto& userIP = credentials.UserIP;
        auto tokenHash = credentials.Token
            ? GetSha256HexDigestLowerCase(*credentials.Token)
            : std::move(*credentials.TokenSha256);
        YT_LOG_DEBUG("Authenticating user with Cypress token (TokenHash: %v, UserIP: %v)",
            tokenHash,
            userIP);

        // Try to retrieve both old (@user) and new (@user_id) token attributes
        // at the same time to speed up the process.
        auto path = Format("%v/%v",
            Config_->RootPath ? Config_->RootPath : "//sys/cypress_tokens",
            ToYPathLiteral(tokenHash));

        auto userAttributePath = Format("%v/@user", path);
        auto userIdAttributePath = Format("%v/@user_id", path);

        TGetNodeOptions options;
        options.ReadFrom = EMasterChannelKind::Cache;

        // For compatability with cypress tokens created as document
        // we request attributes explicitly.
        auto getAttributeFutures = std::vector{
            Client_->GetNode(userAttributePath, options),
            Client_->GetNode(userIdAttributePath, options),
        };

        return AllSet(getAttributeFutures)
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

    TFuture<TAuthenticationResult> OnCallTokenResult(std::vector<TErrorOr<TYsonString>>&& responsesOrErrors)
    {
        YT_VERIFY(responsesOrErrors.size() == 2);
        auto userAttributeRspOrError = responsesOrErrors[0];
        auto userIdAttributeRspOrError = responsesOrErrors[1];

        auto isMissingAttribute = [] (const auto& rspOrError) {
            return !rspOrError.IsOK() && rspOrError.FindMatching(NYTree::EErrorCode::ResolveError);
        };

        auto isMissingUserAttribute = isMissingAttribute(userAttributeRspOrError);
        auto isMissingUserIdAttribute = isMissingAttribute(userIdAttributeRspOrError);

        if (isMissingUserAttribute && isMissingUserIdAttribute) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::InvalidCredentials,
                "Token is missing in Cypress or missing attributes \"user_id\" and \"user\" on token Cypress node")
                << userAttributeRspOrError
                << userIdAttributeRspOrError;
        }

        if (!userAttributeRspOrError.IsOK() && !isMissingUserAttribute) {
            THROW_ERROR_EXCEPTION(userAttributeRspOrError);
        }

        if (!userIdAttributeRspOrError.IsOK() && !isMissingUserIdAttribute) {
            THROW_ERROR_EXCEPTION(userIdAttributeRspOrError);
        }

        if (userIdAttributeRspOrError.IsOK()) {
            auto userIdAttribute = ConvertTo<TObjectId>(userIdAttributeRspOrError.Value());
            // New authentication schema: now we need to get the username given the user ID which we received.
            auto path = Format("%v/@name", FromObjectId(userIdAttribute));
            TGetNodeOptions options;
            options.ReadFrom = EMasterChannelKind::Cache;
            return Client_->GetNode(path, options)
                .ApplyUnique(BIND(
                    &TCypressTokenAuthenticator::OnCallUsernameResult,
                    MakeStrong(this),
                    std::move(userIdAttribute)));
        }

        // Old authentication schema: we already retrieved the username.
        YT_VERIFY(userAttributeRspOrError.IsOK());
        auto userAttribute = ConvertTo<std::string>(userAttributeRspOrError.Value());
        return MakeFuture(TAuthenticationResult{
            .Login = std::move(userAttribute),
        });
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
