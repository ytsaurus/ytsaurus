#include "connection.h"

#include <yt/yt/core/rpc/authentication_identity.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

TClientOptions TClientOptions::FromUser(const TString& user, const std::optional<TString>& userTag)
{
    return {
        .User = user,
        .UserTag = userTag ? *userTag : user
    };
}

TClientOptions TClientOptions::FromAuthenticationIdentity(const NRpc::TAuthenticationIdentity& identity)
{
    return FromUser(identity.User, identity.UserTag);
}

TClientOptions TClientOptions::FromToken(const TString& token)
{
    return {
        .Token = token
    };
}

const TString& TClientOptions::GetAuthenticatedUser() const
{
    static const TString UnknownUser("<unknown>");
    return User ? *User : UnknownUser;
}

NRpc::TAuthenticationIdentity TClientOptions::GetAuthenticationIdentity() const
{
    if (!User) {
        THROW_ERROR_EXCEPTION("Authenticated user is not specified in client options");
    }
    return NRpc::TAuthenticationIdentity(*User, UserTag.value_or(*User));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

