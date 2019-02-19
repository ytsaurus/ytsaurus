#pragma once

#include <yt/server/clickhouse_server/public.h>

#include <Interpreters/IRuntimeComponentsFactory.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IRuntimeComponentsFactory> CreateRuntimeComponentsFactory(
    std::string cliqueId,
    std::string homePath,
    ICliqueAuthorizationManagerPtr cliqueAuthorizationManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
