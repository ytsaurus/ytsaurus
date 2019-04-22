#pragma once

#include "private.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IRuntimeComponentsFactory> CreateRuntimeComponentsFactory(
    std::unique_ptr<DB::IUsersManager> securityManager,
    std::unique_ptr<DB::IExternalLoaderConfigRepository> dictionariesConfigRepository,
    std::unique_ptr<IGeoDictionariesLoader> geoDictionariesLoader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
