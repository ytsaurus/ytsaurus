#pragma once

#include "config_repository.h"

#include <functional>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

using TUpdateConfigHook = std::function<void(IConfigPtr config)>;

////////////////////////////////////////////////////////////////////////////////

class IConfigReloader
{
public:
    virtual ~IConfigReloader() = default;

    virtual void Start() = 0;
};

using IConfigReloaderPtr = std::unique_ptr<IConfigReloader>;

////////////////////////////////////////////////////////////////////////////////

IConfigReloaderPtr CreateConfigReloader(
    IConfigRepositoryPtr repository,
    const std::string& name,
    TUpdateConfigHook updateConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
