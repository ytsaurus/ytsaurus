#include "client_impl.h"

#include <yt/yt/core/crypto/crypto.h>

#include <util/string/hex.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NCrypto;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TClient::DoSetUserPassword(
    const TString& user,
    const TString& currentPasswordSha256,
    const TString& newPasswordSha256,
    const TSetUserPasswordOptions& options)
{
    auto path = Format("//sys/users/%v", ToYPathLiteral(user));

    constexpr TStringBuf HashedPasswordAttribute = "hashed_password";
    constexpr TStringBuf PasswordSaltAttribute = "password_salt";
    constexpr TStringBuf PasswordRevisionAttribute = "password_revision";

    bool isSuperuser = false;
    if (Options_.User) {
        TGetNodeOptions getOptions;
        static_cast<TTimeoutOptions&>(getOptions) = options;

        auto path = Format("//sys/users/%v/@member_of_closure", ToYPathLiteral(*Options_.User));
        auto rsp = WaitFor(GetNode(path, /*options*/ {}))
            .ValueOrThrow();
        auto groups = ConvertTo<THashSet<TString>>(rsp);
        isSuperuser =
            groups.contains(NSecurityClient::SuperusersGroupName) ||
            Options_.User == NSecurityClient::RootUserName;
    }

    // Password can be changed either by user themselves
    // or by superuser. In first case, current password is checked.
    if (!isSuperuser) {
        if (Options_.User != user) {
            THROW_ERROR_EXCEPTION(
                "Password can be changed either by user theirselves "
                "or by superuser")
                << TErrorAttribute("user", user)
                << TErrorAttribute("authenticated_user", Options_.User);
        }

        TGetNodeOptions getOptions;
        static_cast<TTimeoutOptions&>(getOptions) = options;

        getOptions.Attributes = std::vector<TString>({
            TString{HashedPasswordAttribute},
            TString{PasswordSaltAttribute},
            TString{PasswordRevisionAttribute},
        });

        auto rsp = WaitFor(GetNode(path, getOptions))
            .ValueOrThrow();
        auto rspNode = ConvertToNode(rsp);
        const auto& attributes = rspNode->Attributes();

        auto hashedPassword = attributes.Get<TString>(HashedPasswordAttribute);
        auto passwordSalt = attributes.Get<TString>(PasswordSaltAttribute);
        auto passwordRevision = attributes.Get<ui64>(PasswordRevisionAttribute);

        if (HashPasswordSha256(currentPasswordSha256, passwordSalt) != hashedPassword) {
            THROW_ERROR_EXCEPTION("User provided invalid password")
                << TErrorAttribute("password_revision", passwordRevision);
        }
    }

    constexpr int PasswordSaltLength = 16;
    auto newPasswordSaltRaw = GenerateCryptoStrongRandomString(PasswordSaltLength);
    auto newPasswordSalt = HexEncode(newPasswordSaltRaw.data(), newPasswordSaltRaw.size());

    auto hashedNewPassword = HashPasswordSha256(newPasswordSha256, newPasswordSalt);

    TSetNodeOptions setOptions;
    static_cast<TTimeoutOptions&>(setOptions) = options;

    // TODO(gritukan): This is not atomic; support MulticellAttributes for users.
    WaitFor(SetNode(
        Format("%v/@hashed_password", path),
        ConvertToYsonString(hashedNewPassword),
        setOptions))
        .ThrowOnError();
    WaitFor(SetNode(
        Format("%v/@password_salt", path),
        ConvertToYsonString(newPasswordSalt),
        setOptions))
        .ThrowOnError();

    YT_LOG_DEBUG("User password updated "
        "(User: %v, AuthenticatedUser: %v, NewPasswordSha256: %v, HashedNewPassword: %v)",
        user,
        Options_.User,
        newPasswordSha256,
        hashedNewPassword);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
