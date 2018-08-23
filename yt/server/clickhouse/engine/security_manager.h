#pragma once

#include <yt/server/clickhouse/interop/auth_clique.h>

#include <Interpreters/ISecurityManager.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::ISecurityManager> CreateSecurityManager(
    std::string cliqueId,
    NInterop::ICliqueAuthorizationManagerPtr cliqueAuthorizationManager);

} // namespace NClickHouse
} // namespace NYT
