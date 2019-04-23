#pragma once

#include "private.h"

#include <Interpreters/IUsersManager.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IUsersManager> CreateSecurityManager(
    TBootstrap* bootstrap,
    TString cliqueId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
