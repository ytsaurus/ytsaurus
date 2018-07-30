#pragma once

#include "config_reloader.h"
#include "config_repository.h"

#include <Common/config.h>
#include <Interpreters/Context.h>

#include <Poco/Util/LayeredConfiguration.h>

#include <memory>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

using TLayeredConfigPtr = Poco::AutoPtr<Poco::Util::LayeredConfiguration>;

////////////////////////////////////////////////////////////////////////////////

class IConfigManager
{
public:
    virtual ~IConfigManager() = default;

    virtual TLayeredConfigPtr LoadServerConfig() const = 0;
    virtual TLayeredConfigPtr LoadUsersConfig() const = 0;
    virtual TLayeredConfigPtr LoadClustersConfig() const = 0;

    virtual void SubscribeToUpdates(DB::Context* context) = 0;
};

using IConfigManagerPtr = std::unique_ptr<IConfigManager>;

////////////////////////////////////////////////////////////////////////////////

IConfigManagerPtr CreateConfigManager(
    IConfigPtr staticBootstrapConfig,
    NInterop::IStoragePtr storage,
    NInterop::IAuthorizationTokenPtr authToken);

////////////////////////////////////////////////////////////////////////////////

} // namespace NClickHouse
} // namespace NYT
