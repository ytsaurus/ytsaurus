#pragma once

#include <yt/server/clickhouse_server/public.h>

#include <Interpreters/IRuntimeComponentsFactory.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IRuntimeComponentsFactory> CreateRuntimeComponentsFactory(
    IStoragePtr storage,
    std::string cliqueId,
    IAuthorizationTokenPtr authToken,
    std::string homePath,
    ICliqueAuthorizationManagerPtr cliqueAuthorizationManager);

} // namespace NYT::NClickHouseServer
