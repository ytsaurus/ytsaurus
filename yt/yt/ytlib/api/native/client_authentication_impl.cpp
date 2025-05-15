#include "client_impl.h"

#include <yt/yt/library/re2/re2.h>
#include <yt/yt/core/crypto/crypto.h>

#include <util/string/hex.h>

namespace NYT::NApi::NNative {

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

static TString GenerateToken()
{
    constexpr int TokenBodyBytesLength = 16;
    constexpr int TokenPrefixBytesLength = 2;
    auto tokenBodyBytes = GenerateCryptoStrongRandomString(TokenBodyBytesLength);
    auto tokenBody = to_lower(HexEncode(tokenBodyBytes.data(), tokenBodyBytes.size()));
    auto tokenPrefixBytes = GenerateCryptoStrongRandomString(TokenPrefixBytesLength);
    auto tokenPrefix = Format("ytct-%v-", to_lower(HexEncode(tokenPrefixBytes.data(), tokenPrefixBytes.size())));
    return tokenPrefix + tokenBody;
}

////////////////////////////////////////////////////////////////////////////////

void TClient::DoSetUserPassword(
    const std::string& user,
    const TString& currentPasswordSha256,
    const TString& newPasswordSha256,
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

    YT_LOG_DEBUG("User password updated "
        "(User: %v, NewPasswordSha256: %v, HashedNewPassword: %v)",
        user,
        newPasswordSha256,
        hashedNewPassword);
}

TIssueTokenResult TClient::DoIssueToken(
    const std::string& user,
    const TString& passwordSha256,
    const TIssueTokenOptions& options)
{
    ValidateAuthenticationCommandPermissions(
        "Token issuance",
        user,
        passwordSha256,
        options);

    YT_LOG_DEBUG("Issuing new token for user (User: %v)",
        user);

    auto attributes = CreateEphemeralAttributes();
    attributes->Set("description", options.Description);
    return DoIssueTokenImpl(user, GenerateToken(), attributes, options);
}

TIssueTokenResult TClient::DoIssueSpecificTemporaryToken(
    const std::string& user,
    const TString& token,
    const IAttributeDictionaryPtr& attributes,
    const TIssueTemporaryTokenOptions& options)
{
    YT_LOG_DEBUG("Issuing specific temporary token for user (User: %v)",
        user);

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
    YT_LOG_DEBUG("Issuing new temporary token for user (User: %v)",
        user);

    auto attributesCopy = attributes->Clone();
    attributesCopy->Set("expiration_timeout", options.ExpirationTimeout.MilliSeconds());
    attributesCopy->Set("description", options.Description);
    return DoIssueTokenImpl(user, GenerateToken(), attributesCopy, options);
}

TIssueTokenResult TClient::DoIssueTokenImpl(
    const std::string& user,
    const TString& token,
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
        Format("//sys/users/%v/@id", user),
        /*options*/ {}));
    if (!userIdRspOrError.IsOK()) {
        YT_LOG_DEBUG(userIdRspOrError, "Failed to issue new token for user: "
            "could not get user ID by username "
            "(User: %v, TokenPrefix: %v, TokenHash: %v)",
            user,
            tokenPrefix,
            tokenHash);
        THROW_ERROR_EXCEPTION("Failed to issue new token for user")
            << userIdRspOrError;
    }

    attributes->Set("user_id", ConvertTo<std::string>(userIdRspOrError.Value()));
    attributes->Set("token_prefix", tokenPrefix);

    createOptions.Attributes = attributes;

    YT_LOG_DEBUG("Issuing new token for user (User: %v, TokenPrefix: %v, TokenHash: %v)",
        user,
        tokenPrefix,
        tokenHash);

    auto path = Format("//sys/cypress_tokens/%v", ToYPathLiteral(tokenHash));
    auto rspOrError = WaitFor(rootClient->CreateNode(
        path,
        EObjectType::MapNode,
        createOptions));

    if (!rspOrError.IsOK()) {
        YT_LOG_DEBUG(rspOrError, "Failed to issue new token for user "
            "(User: %v, TokenPrefix: %v, TokenHash: %v)",
            user,
            tokenPrefix,
            tokenHash);
        THROW_ERROR_EXCEPTION("Failed to issue new token for user")
            << rspOrError;
    }

    YT_LOG_DEBUG("Issued new token for user (User: %v, TokenPrefix: %v, TokenHash: %v)",
        user,
        tokenPrefix,
        tokenHash);

    return TIssueTokenResult{
        .Token = token,
        .NodeId = rspOrError.Value(),
    };
}

void TClient::DoRefreshTemporaryToken(
    const std::string& user,
    const TString& token,
    const TRefreshTemporaryTokenOptions& options)
{
    auto tokenHash = GetSha256HexDigestLowerCase(token);

    TGetNodeOptions getOptions;
    static_cast<TTimeoutOptions&>(getOptions) = options;

    YT_LOG_DEBUG("Refresh temporary token for user (User: %v, TokenHash: %v)",
        user,
        tokenHash);

    auto rootClient = CreateRootClient();
    auto path = Format("//sys/cypress_tokens/%v", ToYPathLiteral(tokenHash));
    auto rspOrError = WaitFor(rootClient->GetNode(
        path,
        getOptions));

    if (!rspOrError.IsOK()) {
        YT_LOG_WARNING(rspOrError, "Failed to refresh token for user "
            "(User: %v, TokenHash: %v)",
            user,
            tokenHash);
        THROW_ERROR_EXCEPTION("Failed to refresh token for user")
            << rspOrError;
    }

    YT_LOG_DEBUG("Successfully refreshed token for user (User: %v, TokenHash: %v)",
        user,
        tokenHash);
}

void TClient::DoRevokeToken(
    const std::string& user,
    const TString& passwordSha256,
    const TString& tokenSha256,
    const TRevokeTokenOptions& options)
{
    auto rootClient = CreateRootClient();

    auto path = Format("//sys/cypress_tokens/%v", ToYPathLiteral(tokenSha256));

    TGetNodeOptions getOptions;
    static_cast<TTimeoutOptions&>(getOptions) = options;
    getOptions.Attributes = TAttributeFilter({"user", "user_id"});

    auto tokenNodeOrError = WaitFor(rootClient->GetNode(path, getOptions));
    if (!tokenNodeOrError.IsOK()) {
        YT_LOG_DEBUG(tokenNodeOrError, "Failed to get token (TokenHash: %v)",
            tokenSha256);
        THROW_ERROR_EXCEPTION("Failed to get token")
            << tokenNodeOrError;
    }
    auto tokenNode = ConvertTo<INodePtr>(tokenNodeOrError.Value());
    const auto& tokenAttributes = tokenNode->Attributes();

    getOptions.Attributes = {};
    TString tokenUser;
    auto userIdAttribute = tokenAttributes.Find<TString>("user_id");
    if (userIdAttribute) {
        // Resolve the username with the retrieved user_id.
        auto tokenUsernameOrError = WaitFor(rootClient->GetNode(
            Format("#%v/@name", ToYPathLiteral(*userIdAttribute)),
            getOptions));
        if (!tokenUsernameOrError.IsOK()) {
            YT_LOG_DEBUG(tokenUsernameOrError, "Failed to get user for token (TokenHash: %v)",
                tokenSha256);
            THROW_ERROR_EXCEPTION("Failed to get user for token")
                << tokenUsernameOrError;
        }
        tokenUser = ConvertTo<TString>(tokenUsernameOrError.Value());
    } else {
        // This case means that the token was issued using the old schema (with @user attribute), and we already have the name.
        auto userAttribute = tokenAttributes.Find<TString>("user");
        if (userAttribute) {
            tokenUser = *userAttribute;
        } else {
            YT_LOG_DEBUG("Failed to get both attributes of the token (TokenHash: %v)",
                tokenSha256);
            THROW_ERROR_EXCEPTION("Failed to get both attributes of the token");
        }
    }

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

    auto error = WaitFor(rootClient->RemoveNode(path, removeOptions));
    if (!error.IsOK()) {
        YT_LOG_DEBUG(error, "Failed to remove token (User: %v, TokenHash: %v)",
            tokenUser,
            tokenSha256);
        THROW_ERROR_EXCEPTION("Failed to remove token")
            << error;
    }

    YT_LOG_DEBUG("Token removed successfully (User: %v, TokenHash: %v)",
        tokenUser,
        tokenSha256);
}

TListUserTokensResult TClient::DoListUserTokens(
    const std::string& user,
    const TString& passwordSha256,
    const TListUserTokensOptions& options)
{
    ValidateAuthenticationCommandPermissions(
        "Tokens listing",
        user,
        passwordSha256,
        options);

    YT_LOG_DEBUG("Listing tokens for user (User: %v, WithMetadata: %v)",
        user,
        options.WithMetadata);

    TListNodeOptions listOptions;
    static_cast<TTimeoutOptions&>(listOptions) = options;

    listOptions.Attributes = TAttributeFilter({"user", "user_id"});
    if (options.WithMetadata) {
        listOptions.Attributes.AddKey("description");
        listOptions.Attributes.AddKey("token_prefix");
        listOptions.Attributes.AddKey("creation_time");
        listOptions.Attributes.AddKey("effective_expiration");
    }

    auto rootClient = CreateRootClient();
    auto rspOrError = WaitFor(rootClient->ListNode("//sys/cypress_tokens", listOptions));
    if (!rspOrError.IsOK()) {
        YT_LOG_DEBUG(rspOrError, "Failed to list tokens");
        THROW_ERROR_EXCEPTION("Failed to list tokens")
            << rspOrError;
    }

    auto userIdRspOrError = WaitFor(rootClient->GetNode(
        Format("//sys/users/%v/@id", user),
        /*options*/ {}));
    if (!userIdRspOrError.IsOK()) {
        YT_LOG_DEBUG(userIdRspOrError, "Failed to list tokens: could not get user ID by username (User: %v)",
            user);
        THROW_ERROR_EXCEPTION("Failed to list tokens")
            << userIdRspOrError;
    }
    auto userId = ConvertTo<TString>(userIdRspOrError.Value());

    std::vector<TString> userTokens;
    THashMap<TString, NYson::TYsonString> tokenMetadata;

    auto tokens = ConvertTo<IListNodePtr>(rspOrError.Value());
    for (const auto& tokenNode : tokens->GetChildren()) {
        const auto& attributes = tokenNode->Attributes();
        auto userIdAttribute = attributes.Find<TString>("user_id");
        auto userAttribute = attributes.Find<TString>("user");
        if (userIdAttribute == userId || userAttribute == user) {
            userTokens.push_back(ConvertTo<TString>(tokenNode));
            if (options.WithMetadata) {
                auto metadata = BuildYsonStringFluently()
                    .BeginMap()
                        .Item("description").Value(attributes.Find<TString>("description"))
                        .Item("token_prefix").Value(attributes.Find<TString>("token_prefix"))
                        .Item("creation_time").Value(attributes.Find<TString>("creation_time"))
                        .Item("effective_expiration").Value(attributes.GetYson("effective_expiration"))
                    .EndMap();
                tokenMetadata[ConvertTo<TString>(tokenNode)] = ConvertToYsonString(metadata);
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
    const TString& passwordSha256,
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
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Failed to check %Qlv permission for user", EPermission::Administer);

        canAdminister = (rspOrError.Value().Action == ESecurityAction::Allow);
    }

    if (!canAdminister) {
        if (Options_.User != user) {
            THROW_ERROR_EXCEPTION(
                "%v can be performed either by user themselves "
                "or by a user having %Qlv permission on the user",
                action,
                EPermission::Administer)
                << TErrorAttribute("user", user)
                << TErrorAttribute("authenticated_user", Options_.User);
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

            auto hashedPassword = attributes.Get<TString>(HashedPasswordAttribute);
            auto passwordSalt = attributes.Get<TString>(PasswordSaltAttribute);
            auto passwordRevision = attributes.Get<ui64>(PasswordRevisionAttribute);

            if (HashPasswordSha256(passwordSha256, passwordSalt) != hashedPassword) {
                THROW_ERROR_EXCEPTION("User provided invalid password")
                    << TErrorAttribute("password_revision", passwordRevision);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
