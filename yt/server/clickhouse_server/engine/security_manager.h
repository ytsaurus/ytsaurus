#pragma once

#include <yt/server/clickhouse_server/native/public.h>

#include <Interpreters/ISecurityManager.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::ISecurityManager> CreateSecurityManager(
    std::string cliqueId,
    NNative::ICliqueAuthorizationManagerPtr cliqueAuthorizationManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
