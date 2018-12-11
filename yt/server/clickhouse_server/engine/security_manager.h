#pragma once

#include "clickhouse.h"

#include <yt/server/clickhouse_server/native/public.h>

//#include <Interpreters/ISecurityManager.h>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::ISecurityManager> CreateSecurityManager(
    std::string cliqueId,
    NNative::ICliqueAuthorizationManagerPtr cliqueAuthorizationManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
