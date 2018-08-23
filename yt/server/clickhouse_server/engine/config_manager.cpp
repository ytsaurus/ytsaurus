#include "config_manager.h"

#include "auth_token.h"
#include "logging_helpers.h"
#include "type_helpers.h"

#include <Poco/Logger.h>
#include <Poco/Util/XMLConfiguration.h>

#include <common/logger_useful.h>

namespace NYT {
namespace NClickHouse {

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

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TConfigManager
    : public IConfigManager
{
private:
    IConfigPtr StaticConfig;
    IConfigRepositoryPtr ConfigRepository;

    Poco::Logger* Logger;

    std::vector<IConfigReloaderPtr> Reloaders;

public:
    TConfigManager(IConfigPtr staticConfig,
                   IConfigRepositoryPtr repository);

    TLayeredConfigPtr LoadServerConfig() const override;
    TLayeredConfigPtr LoadUsersConfig() const override;
    TLayeredConfigPtr LoadClustersConfig() const override;

    void SubscribeToUpdates(DB::Context* context) override;

private:
    void SetupAutoReloader(const std::string& name, TUpdateConfigHook updateConfig);

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

void TConfigManager::SubscribeToUpdates(DB::Context* context)
{
    // Server config
    auto updateConfig = [context, this](const IConfigPtr config) {
        context->setConfig(CombineWithStatic(config));
        LOG_INFO(Logger, "Server config reloaded");
    };
    SetupAutoReloader("config", std::move(updateConfig));

    // Users config
    auto updateUsersConfig = [context, this](const IConfigPtr config) {
        context->setUsersConfig(CombineWithStatic(config));
        LOG_INFO(Logger, "Users config reloaded");
    };
    SetupAutoReloader("users", std::move(updateUsersConfig));

    // Clusters config
    auto updateClustersConfig = [context, this](const IConfigPtr config) {
        context->setClustersConfig(CombineWithStatic(config));
        LOG_INFO(Logger, "Clusters config reloaded");
    };
    SetupAutoReloader("clusters", std::move(updateClustersConfig));
}

void TConfigManager::SetupAutoReloader(const std::string& name, TUpdateConfigHook updateConfig)
{
    auto reloader = CreateConfigReloader(ConfigRepository, name, updateConfig);
    reloader->Start();
    Reloaders.push_back(std::move(reloader));
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
    NInterop::IStoragePtr storage,
    NInterop::IAuthorizationTokenPtr authToken)
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

} // namespace NClickHouse
} // namespace NYT
