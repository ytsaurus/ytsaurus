#include "config_manager.h"

#include "auth_token.h"
#include "logging_helpers.h"
#include "type_helpers.h"

#include <Poco/Logger.h>
#include <Poco/Util/XMLConfiguration.h>

#include <yt/server/clickhouse_server/native/storage.h>
#include <yt/server/clickhouse_server/native/path.h>

#include <common/logger_useful.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

namespace {

////////////////////////////////////////////////////////////////////////////////

TLayeredConfigPtr BuildLayeredConfig(IConfigPtr staticConfig, IConfigPtr dynamicConfig)
{
    TLayeredConfigPtr config = new Poco::Util::LayeredConfiguration();
    if (staticConfig) {
        config->add(staticConfig);
    }
    if (dynamicConfig) {
        config->add(dynamicConfig);
    }
    return config;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TConfigManager
    : public IConfigManager
{
private:
    IConfigPtr StaticConfig;
    IConfigRepositoryPtr ConfigRepository;

    Poco::Logger* Logger;

public:
    TConfigManager(IConfigPtr staticConfig,
                   IConfigRepositoryPtr repository);

    TLayeredConfigPtr LoadServerConfig() const override;
    TLayeredConfigPtr LoadUsersConfig() const override;
    TLayeredConfigPtr LoadClustersConfig() const override;

private:
    TLayeredConfigPtr CombineWithStatic(IConfigPtr dynamicConfig) const;
    TLayeredConfigPtr LoadConfig(const std::string& name) const;
};

////////////////////////////////////////////////////////////////////////////////

TConfigManager::TConfigManager(IConfigPtr staticConfig,
                               IConfigRepositoryPtr repository)
    : StaticConfig(staticConfig)
    , ConfigRepository(std::move(repository))
    , Logger(&Poco::Logger::get("ConfigManager"))
{}

TLayeredConfigPtr TConfigManager::LoadServerConfig() const
{
    return LoadConfig("server");
}

TLayeredConfigPtr TConfigManager::LoadUsersConfig() const
{
    return LoadConfig("users");
}

TLayeredConfigPtr TConfigManager::LoadClustersConfig() const
{
    return LoadConfig("clusters");
}

TLayeredConfigPtr TConfigManager::CombineWithStatic(IConfigPtr dynamicConfig) const
{
    return BuildLayeredConfig(StaticConfig, dynamicConfig);
}

TLayeredConfigPtr TConfigManager::LoadConfig(const std::string& name) const
{
    return CombineWithStatic(ConfigRepository->Load(name));
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IConfigManager> CreateConfigManager(
    IConfigPtr staticBootstrapConfig,
    NNative::IStoragePtr storage,
    NNative::IAuthorizationTokenPtr authToken)
{
    auto storageHomePath = staticBootstrapConfig->getString("storage_home_path");

    auto configsPath = storage->PathService()->Build(
        ToString(storageHomePath),
        {"configuration"});

    auto repository = CreateConfigRepository(
        std::move(storage),
        std::move(authToken),
        ToStdString(configsPath));

    return std::make_unique<TConfigManager>(
        std::move(staticBootstrapConfig),
        std::move(repository));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
