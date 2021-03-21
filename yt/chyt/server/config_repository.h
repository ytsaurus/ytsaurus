#pragma once

#include "private.h"

#include <Interpreters/IExternalLoaderConfigRepository.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IExternalLoaderConfigRepository> CreateDictionaryConfigRepository(
    const std::vector<TDictionaryConfigPtr>& dictionaries);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
