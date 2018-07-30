#include "auth_token.h"

#include <Interpreters/Context.h>

namespace NYT {
namespace NClickHouse {

using namespace DB;

namespace {

////////////////////////////////////////////////////////////////////////////////

std::string MapUserName(const std::string& user)
{
    if (user.empty() || user == "default") {
        return "guest";
    }
    return user;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NInterop::IAuthorizationTokenPtr CreateAuthToken(
    NInterop::IAuthorizationTokenService& auth,
    const std::string& user)
{
    NInterop::TStringMap attrs;
    attrs["user"] = MapUserName(user);

    return auth.CreateToken(attrs);
}

NInterop::IAuthorizationTokenPtr CreateAuthToken(
    NInterop::IAuthorizationTokenService& auth,
    const Context& context)
{
    const auto& clientInfo = context.getClientInfo();

    NInterop::TStringMap attrs;
    attrs["user"] = MapUserName(clientInfo.initial_user);

    return auth.CreateToken(attrs);
}

////////////////////////////////////////////////////////////////////////////////

NInterop::IAuthorizationTokenPtr CreateAuthToken(
    NInterop::IStorage& storage,
    const DB::Context& context)
{
    return CreateAuthToken(*storage.AuthTokenService(), context);
}

}   // namespace NClickHouse
}   // namespace NYT
