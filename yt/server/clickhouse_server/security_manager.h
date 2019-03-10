#pragma once

#include "private.h"

#include <Interpreters/ISecurityManager.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::ISecurityManager> CreateSecurityManager(
    TBootstrap* bootstrap,
    TString cliqueId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
