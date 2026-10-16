#include "client_impl.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/library/auth_server/config.h>
#include <yt/yt/library/auth_server/credentials.h>
#include <yt/yt/library/auth_server/cypress_token_authenticator.h>
#include <yt/yt/library/auth_server/token_authenticator.h>

#include <yt/yt/library/re2/re2.h>

#include <yt/yt/core/crypto/crypto.h>

#include <library/cpp/yt/string/string.h>

#include <util/string/hex.h>

namespace NYT::NApi::NNative {

using namespace NAuth;
using namespace NConcurrency;
using namespace NCrypto;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

/*
 * User-created cypress tokens follow the format "ytct-{}-{}".
 * First {} is 4 hexadecimal characters and may be saved and revealed to the user.
 * Second {} is 32 hexadecimal characters, private, and should not be saved in the system.
 */
constexpr TStringBuf CypressTokenPrefixRegex = "ytct-[0-9a-f]{4}-";
constexpr int CypressTokenPrefixLength = 10; // "ytct-abcd-"

static std::string GenerateToken()
{
    constexpr int TokenBodyBytesLength = 16;
    constexpr int TokenPrefixBytesLength = 2;
    auto tokenBodyBytes = GenerateCryptoStrongRandomString(TokenBodyBytesLength);
    auto tokenBody = AsciiStringToLower(HexEncode(tokenBodyBytes.data(), tokenBodyBytes.size()));
    auto tokenPrefixBytes = GenerateCryptoStrongRandomString(TokenPrefixBytesLength);
    auto tokenPrefix = Format("ytct-%v-", AsciiStringToLower(HexEncode(tokenPrefixBytes.data(), tokenPrefixBytes.size())));
    return tokenPrefix + tokenBody;
}

////////////////////////////////////////////////////////////////////////////////

void TClient::DoSetUserPassword(
    const std::string& user,
    const std::string& currentPasswordSha256,
    const std::string& newPasswordSha256,
    const TSetUserPasswordOptions& options)
{
    ValidateAuthenticationCommandPermissions(
        "Password change",
        user,
        currentPasswordSha256,
        options);

    constexpr int PasswordSaltLength = 16;
    auto newPasswordSaltBytes = GenerateCryptoStrongRandomString(PasswordSaltLength);
    auto newPasswordSalt = HexEncode(newPasswordSaltBytes.data(), newPasswordSaltBytes.size());

    auto hashedNewPassword = HashPasswordSha256(newPasswordSha256, newPasswordSalt);

    TMultisetAttributesNodeOptions multisetAttributesOptions;
    static_cast<TTimeoutOptions&>(multisetAttributesOptions) = options;

    auto rootClient = CreateRootClient();
    auto path = Format("//sys/users/%v/@", ToYPathLiteral(user));
    auto nodeFactory = GetEphemeralNodeFactory();
    auto attributes = nodeFactory->CreateMap();
    attributes->AddChild("hashed_password", ConvertToNode(hashedNewPassword));
    attributes->AddChild("password_salt", ConvertToNode(newPasswordSalt));
    attributes->AddChild("password_is_temporary", ConvertToNode(options.PasswordIsTemporary));
    WaitFor(rootClient->MultisetAttributesNode(
        path,
        attributes,
        multisetAttributesOptions))
        .ThrowOnError();

    YT_TLOG_DEBUG("User password updated")
        .With("User", user)
        .With("NewPasswordSha256", newPasswordSha256)
        .With("HashedNewPassword", hashedNewPassword);
}

TIssueTokenResult TClient::DoIssueToken(
    const std::string& user,
    const std::string& passwordSha256,
    const TIssueTokenOptions& options)
{
    ValidateAuthenticationCommandPermissions(
        "Token issuance",
        user,
        passwordSha256,
        options);

    YT_TLOG_DEBUG("Issuing new token for user")
        .With("User", user);

    auto attributes = CreateEphemeralAttributes();
    attributes->Set("description", options.Description);
    return DoIssueTokenImpl(user, GenerateToken(), attributes, options);
}

TIssueTokenResult TClient::DoIssueSpecificTemporaryToken(
    const std::string& user,
    const std::string& token,
    const IAttributeDictionaryPtr& attributes,
    const TIssueTemporaryTokenOptions& options)
{
    YT_TLOG_DEBUG("Issuing specific temporary token for user")
        .With("User", user);

    auto attributesCopy = attributes->Clone();
    attributesCopy->Set("expiration_timeout", options.ExpirationTimeout.MilliSeconds());
    attributesCopy->Set("description", options.Description);
    return DoIssueTokenImpl(user, token, attributesCopy, options);
}

TIssueTokenResult TClient::DoIssueTemporaryToken(
    const std::string& user,
    const IAttributeDictionaryPtr& attributes,
    const TIssueTemporaryTokenOptions& options)
{
    YT_TLOG_DEBUG("Issuing new temporary token for user")
        .With("User", user);

    auto attributesCopy = attributes->Clone();
    attributesCopy->Set("expiration_timeout", options.ExpirationTimeout.MilliSeconds());
    attributesCopy->Set("description", options.Description);
    return DoIssueTokenImpl(user, GenerateToken(), attributesCopy, options);
}

TIssueTokenResult TClient::DoIssueTokenImpl(
    const std::string& user,
    const std::string& token,
    const IAttributeDictionaryPtr& attributes,
    const TIssueTokenOptions& options)
{
    auto tokenHash = GetSha256HexDigestLowerCase(token);
    auto tokenPrefix = token.substr(0, CypressTokenPrefixLength);
    if (!NRe2::TRe2::FullMatch(tokenPrefix.data(), CypressTokenPrefixRegex.data())) {
        tokenPrefix = "";
    }

    TCreateNodeOptions createOptions;
    static_cast<TTimeoutOptions&>(createOptions) = options;

    auto rootClient = CreateRootClient();
    auto userIdRspOrError = WaitFor(rootClient->GetNode(
        Format("//sys/users/%v/@id", ToYPathLiteral(user)),
        /*options*/ {}));
    if (!userIdRspOrError.IsOK()) {
        YT_TLOG_DEBUG("Failed to issue new token for user: could not get user ID by username")
            .With("User", user)
            .With("TokenPrefix", tokenPrefix)
            .With("TokenHash", tokenHash)
            .With(userIdRspOrError);
        if (userIdRspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            THROW_ERROR_EXCEPTION(NSecurityClient::EErrorCode::NoSuchUser, "No such user %Qv",
                user)
                .With(userIdRspOrError);
        } else {
            THROW_ERROR_EXCEPTION("Failed to issue new token for user")
                .With(userIdRspOrError);
        }
    }

    attributes->Set("user", user);
    attributes->Set("user_id", ConvertTo<std::string>(userIdRspOrError.Value()));
    attributes->Set("token_prefix", tokenPrefix);

    createOptions.Attributes = attributes;

    YT_TLOG_DEBUG("Issuing new token for user")
        .With("User", user)
        .With("TokenPrefix", tokenPrefix)
        .With("TokenHash", tokenHash);

    auto path = Format("//sys/cypress_tokens/%v", ToYPathLiteral(tokenHash));
    auto rspOrError = WaitFor(rootClient->CreateNode(
        path,
        EObjectType::MapNode,
        createOptions));

    if (!rspOrError.IsOK()) {
        YT_TLOG_DEBUG("Failed to issue new token for user")
            .With("User", user)
            .With("TokenPrefix", tokenPrefix)
            .With("TokenHash", tokenHash)
            .With(rspOrError);
        THROW_ERROR_EXCEPTION("Failed to issue new token for user")
            .With(rspOrError);
    }

    YT_TLOG_DEBUG("Issued new token for user")
        .With("User", user)
        .With("TokenPrefix", tokenPrefix)
        .With("TokenHash", tokenHash);

    return TIssueTokenResult{
        .Token = token,
        .NodeId = rspOrError.Value(),
    };
}

void TClient::DoRefreshTemporaryToken(
    const std::string& user,
    const std::string& token,
    const TRefreshTemporaryTokenOptions& options)
{
    auto tokenHash = GetSha256HexDigestLowerCase(token);

    TGetNodeOptions getOptions;
    static_cast<TTimeoutOptions&>(getOptions) = options;

    YT_TLOG_DEBUG("Refresh temporary token for user")
        .With("User", user)
        .With("TokenHash", tokenHash);

    auto rootClient = CreateRootClient();
    auto path = Format("//sys/cypress_tokens/%v", ToYPathLiteral(tokenHash));
    auto rspOrError = WaitFor(rootClient->GetNode(
        path,
        getOptions));

    if (!rspOrError.IsOK()) {
        YT_TLOG_WARNING("Failed to refresh token for user")
            .With("User", user)
            .With("TokenHash", tokenHash)
            .With(rspOrError);
        THROW_ERROR_EXCEPTION("Failed to refresh token for user")
            .With(rspOrError);
    }

    YT_TLOG_DEBUG("Successfully refreshed token for user")
        .With("User", user)
        .With("TokenHash", tokenHash);
}

void TClient::DoRevokeToken(
    const std::string& user,
    const std::string& passwordSha256,
    const std::string& tokenSha256,
    const TRevokeTokenOptions& options)
{
    auto rootClient = CreateRootClient();

    auto config = New<TCypressTokenAuthenticatorConfig>();
    auto cypressTokenAuthenticator = CreateCypressTokenAuthenticator(std::move(config), rootClient);

    auto tokenCredentials = TTokenCredentials{
        .TokenSha256 = tokenSha256,
    };
    auto tokenUser = WaitFor(cypressTokenAuthenticator->Authenticate(std::move(tokenCredentials)))
        .ValueOrThrow()
        .Login;

    if (tokenUser != user) {
        THROW_ERROR_EXCEPTION("Provided token is not recognized as a valid token for user %Qv", user);
    }

    ValidateAuthenticationCommandPermissions(
        "Token revocation",
        tokenUser,
        passwordSha256,
        options);

    TRemoveNodeOptions removeOptions;
    static_cast<TTimeoutOptions&>(removeOptions) = options;

    auto path = Format("//sys/cypress_tokens/%v", ToYPathLiteral(tokenSha256));
    auto error = WaitFor(rootClient->RemoveNode(path, removeOptions));
    if (!error.IsOK()) {
        YT_TLOG_DEBUG("Failed to remove token")
            .With("User", tokenUser)
            .With("TokenHash", tokenSha256)
            .With(error);
        THROW_ERROR_EXCEPTION("Failed to remove token")
            .With(error);
    }

    YT_TLOG_DEBUG("Token removed successfully")
        .With("User", tokenUser)
        .With("TokenHash", tokenSha256);
}

TListUserTokensResult TClient::DoListUserTokens(
    const std::string& user,
    const std::string& passwordSha256,
    const TListUserTokensOptions& options)
{
    ValidateAuthenticationCommandPermissions(
        "Tokens listing",
        user,
        passwordSha256,
        options);

    YT_TLOG_DEBUG("Listing tokens for user")
        .With("User", user)
        .With("WithMetadata", options.WithMetadata);

    TListNodeOptions listOptions;
    static_cast<TTimeoutOptions&>(listOptions) = options;

    std::vector<IAttributeDictionary::TKey> keys = {"user", "user_id"};
    if (options.WithMetadata) {
        keys.push_back("description");
        keys.push_back("token_prefix");
        keys.push_back("creation_time");
        keys.push_back("effective_expiration");
    }
    listOptions.Attributes = TAttributeFilter(std::move(keys));

    auto rootClient = CreateRootClient();
    auto rspOrError = WaitFor(rootClient->ListNode("//sys/cypress_tokens", listOptions));
    if (!rspOrError.IsOK()) {
        YT_TLOG_DEBUG("Failed to list tokens")
            .With(rspOrError);
        THROW_ERROR_EXCEPTION("Failed to list tokens")
            .With(rspOrError);
    }

    auto userIdRspOrError = WaitFor(rootClient->GetNode(
        Format("//sys/users/%v/@id", ToYPathLiteral(user)),
        /*options*/ {}));
    if (!userIdRspOrError.IsOK()) {
        YT_TLOG_DEBUG("Failed to list tokens: could not get user ID by username")
            .With("User", user)
            .With(userIdRspOrError);
        THROW_ERROR_EXCEPTION("Failed to list tokens")
            .With(userIdRspOrError);
    }
    auto userId = ConvertTo<std::string>(userIdRspOrError.Value());

    std::vector<std::string> userTokens;
    THashMap<std::string, NYson::TYsonString> tokenMetadata;

    auto tokens = ConvertTo<IListNodePtr>(rspOrError.Value());
    for (const auto& tokenNode : tokens->GetChildren()) {
        const auto& attributes = tokenNode->Attributes();
        auto userIdAttribute = attributes.Find<std::string>("user_id");
        auto userAttribute = attributes.Find<std::string>("user");
        if (userIdAttribute == userId || userAttribute == user) {
            userTokens.push_back(ConvertTo<std::string>(tokenNode));
            if (options.WithMetadata) {
                auto metadata = BuildYsonStringFluently()
                    .BeginMap()
                        .Item("description").Value(attributes.Find<std::string>("description"))
                        .Item("token_prefix").Value(attributes.Find<std::string>("token_prefix"))
                        .Item("creation_time").Value(attributes.Find<std::string>("creation_time"))
                        .Item("effective_expiration").Value(attributes.GetYson("effective_expiration"))
                    .EndMap();
                tokenMetadata[ConvertTo<std::string>(tokenNode)] = ConvertToYsonString(metadata);
            }
        }
    }

    return TListUserTokensResult{
        .Tokens = std::move(userTokens),
        .Metadata = std::move(tokenMetadata),
    };
}

void TClient::ValidateAuthenticationCommandPermissions(
    TStringBuf action,
    const std::string& user,
    const std::string& passwordSha256,
    const TTimeoutOptions& options)
{
    static const std::string HashedPasswordAttribute = "hashed_password";
    static const std::string PasswordSaltAttribute = "password_salt";
    static const std::string PasswordRevisionAttribute = "password_revision";

    bool canAdminister = false;
    if (Options_.User) {
        TCheckPermissionOptions checkPermissionOptions;
        static_cast<TTimeoutOptions&>(checkPermissionOptions) = options;

        auto rspOrError = WaitFor(CheckPermission(
            *Options_.User,
            Format("//sys/users/%v", ToYPathLiteral(user)),
            EPermission::Administer,
            checkPermissionOptions));

        if (!rspOrError.IsOK()) {
            if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                THROW_ERROR_EXCEPTION(NSecurityClient::EErrorCode::NoSuchUser, "No such user %Qv",
                    user)
                    .With(rspOrError);
            } else {
                THROW_ERROR_EXCEPTION("Failed to check %Qlv permission to administer user %Qv for user %Qv",
                    EPermission::Administer,
                    user,
                    *Options_.User)
                    .With(rspOrError);
            }
        }

        canAdminister = (rspOrError.Value().Action == ESecurityAction::Allow);
    }

    if (!canAdminister) {
        if (Options_.User != user) {
            THROW_ERROR_EXCEPTION(
                "%v can be performed either by user themselves "
                "or by a user having %Qlv permission on the user",
                action,
                EPermission::Administer)
                .With("user", user)
                .With("authenticated_user", Options_.User);
        }

        if (Options_.RequirePasswordInAuthenticationCommands) {
            TGetNodeOptions getOptions;
            static_cast<TTimeoutOptions&>(getOptions) = options;
            getOptions.Attributes = {
                HashedPasswordAttribute,
                PasswordSaltAttribute,
                PasswordRevisionAttribute,
            };

            auto path = Format("//sys/users/%v", ToYPathLiteral(user));
            auto rsp = WaitFor(GetNode(path, getOptions))
                .ValueOrThrow();
            auto rspNode = ConvertToNode(rsp);
            const auto& attributes = rspNode->Attributes();

            auto hashedPassword = attributes.Get<std::string>(HashedPasswordAttribute);
            auto passwordSalt = attributes.Get<std::string>(PasswordSaltAttribute);
            auto passwordRevision = attributes.Get<ui64>(PasswordRevisionAttribute);

            if (HashPasswordSha256(passwordSha256, passwordSalt) != hashedPassword) {
                THROW_ERROR_EXCEPTION("User provided invalid password")
                    .With("password_revision", passwordRevision);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
