#pragma once

#include "clickhouse.h"

#include "config_repository.h"

#include <yt/server/clickhouse_server/native/public.h>

//#include <Common/config.h>
//#include <Interpreters/Context.h>

//#include <Poco/Util/LayeredConfiguration.h>

#include <memory>

namespace NYT::NClickHouseServer::NEngine {

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
};

using IConfigManagerPtr = std::unique_ptr<IConfigManager>;

////////////////////////////////////////////////////////////////////////////////

IConfigManagerPtr CreateConfigManager(
    IConfigPtr staticBootstrapConfig,
    NNative::IStoragePtr storage,
    NNative::IAuthorizationTokenPtr authToken);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
