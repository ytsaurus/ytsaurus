#pragma once

#include <yt/server/clickhouse_server/public.h>

#include <yt/client/api/public.h>

#include <string>

namespace DB {
    class Context;
}

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct IAuthorizationToken
{
    virtual ~IAuthorizationToken() = default;

    /// Opaque token used to represent authenticated client session
};

////////////////////////////////////////////////////////////////////////////////

struct IAuthorizationTokenService
{
    virtual ~IAuthorizationTokenService() = default;

    virtual IAuthorizationTokenPtr CreateToken(const THashMap<TString, TString>& attrs) = 0;
};

////////////////////////////////////////////////////////////////////////////////

const NApi::TClientOptions& UnwrapAuthToken(const IAuthorizationToken& token);

////////////////////////////////////////////////////////////////////////////////

IAuthorizationTokenService* GetAuthTokenService();

////////////////////////////////////////////////////////////////////////////////

IAuthorizationTokenPtr CreateAuthToken(
    IAuthorizationTokenService& auth,
    const std::string& user = {});

IAuthorizationTokenPtr CreateAuthToken(
    IAuthorizationTokenService& auth,
    const DB::Context& context);

////////////////////////////////////////////////////////////////////////////////

// Storage helpers

IAuthorizationTokenPtr CreateAuthToken(
    IStorage& storage,
    const DB::Context& context);

} // namespace NYT::NClickHouseServer
