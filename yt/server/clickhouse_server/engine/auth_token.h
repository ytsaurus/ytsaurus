#pragma once

#include <yt/server/clickhouse_server/native/public.h>

#include <string>

namespace DB {
    class Context;
}

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

NNative::IAuthorizationTokenPtr CreateAuthToken(
    NNative::IAuthorizationTokenService& auth,
    const std::string& user = {});

NNative::IAuthorizationTokenPtr CreateAuthToken(
    NNative::IAuthorizationTokenService& auth,
    const DB::Context& context);

////////////////////////////////////////////////////////////////////////////////

// Storage helpers

NNative::IAuthorizationTokenPtr CreateAuthToken(
    NNative::IStorage& storage,
    const DB::Context& context);

} // namespace NYT::NClickHouseServer::NEngine
