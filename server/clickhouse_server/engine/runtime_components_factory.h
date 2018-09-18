#pragma once

#include <yt/server/clickhouse_server/interop/api.h>

#include <Interpreters/IRuntimeComponentsFactory.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IRuntimeComponentsFactory> CreateRuntimeComponentsFactory(
    NInterop::IStoragePtr storage,
    std::string cliqueId,
    NInterop::IAuthorizationTokenPtr authToken,
    std::string homePath,
    NInterop::ICliqueAuthorizationManagerPtr cliqueAuthorizationManager);

} // namespace NClickHouse
} // namespace NYT
