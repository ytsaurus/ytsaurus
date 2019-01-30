#pragma once

#include <yt/server/clickhouse_server/public.h>

#include <Interpreters/ISecurityManager.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::ISecurityManager> CreateSecurityManager(
    std::string cliqueId,
    ICliqueAuthorizationManagerPtr cliqueAuthorizationManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
