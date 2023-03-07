#pragma once

#include "private.h"

#include <Interpreters/IExternalLoaderConfigRepository.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

//! Config repository used for external models which are not supported in CHYT. Represents an empty set of configs.
std::unique_ptr<DB::IExternalLoaderConfigRepository> CreateDummyConfigRepository();

//! Config repository representing loaded dictionaries.
std::unique_ptr<DB::IExternalLoaderConfigRepository> CreateDictionaryConfigRepository(
    const std::vector<TDictionaryConfigPtr>& dictionaries);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
