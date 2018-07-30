#pragma once

#include <yt/server/clickhouse/interop/api.h>

#include <string>

namespace DB {
    class Context;
}

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

NInterop::IAuthorizationTokenPtr CreateAuthToken(
    NInterop::IAuthorizationTokenService& auth,
    const std::string& user = {});

NInterop::IAuthorizationTokenPtr CreateAuthToken(
    NInterop::IAuthorizationTokenService& auth,
    const DB::Context& context);

////////////////////////////////////////////////////////////////////////////////

// Storage helpers

NInterop::IAuthorizationTokenPtr CreateAuthToken(
    NInterop::IStorage& storage,
    const DB::Context& context);

}   // namespace NClickHouse
}   // namespace NYT
