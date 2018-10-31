#include "auth_token.h"

#include <yt/server/clickhouse_server/native/auth_token.h>
#include <yt/server/clickhouse_server/native/storage.h>

#include <Interpreters/Context.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

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

NNative::IAuthorizationTokenPtr CreateAuthToken(
    NNative::IAuthorizationTokenService& auth,
    const std::string& user)
{
    THashMap<TString, TString> attrs;
    attrs["user"] = MapUserName(user);

    return auth.CreateToken(attrs);
}

NNative::IAuthorizationTokenPtr CreateAuthToken(
    NNative::IAuthorizationTokenService& auth,
    const Context& context)
{
    const auto& clientInfo = context.getClientInfo();

    THashMap<TString, TString> attrs;
    attrs["user"] = MapUserName(clientInfo.initial_user);

    return auth.CreateToken(attrs);
}

////////////////////////////////////////////////////////////////////////////////

NNative::IAuthorizationTokenPtr CreateAuthToken(
    NNative::IStorage& storage,
    const DB::Context& context)
{
    return CreateAuthToken(*storage.AuthTokenService(), context);
}

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT

