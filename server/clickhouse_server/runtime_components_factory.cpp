#include "runtime_components_factory.h"

#include "external_loader_config_repository.h"
#include "geo_dictionaries_loader.h"
#include "security_manager.h"
#include "type_helpers.h"

#include <yt/server/clickhouse_server/storage.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TRuntimeComponentsFactory
    : public DB::IRuntimeComponentsFactory
{
private:
    IStoragePtr Storage;
    std::string CliqueId;
    IAuthorizationTokenPtr AuthToken;
    std::string HomePath;
    ICliqueAuthorizationManagerPtr CliqueAuthorizationManager_;

public:
    TRuntimeComponentsFactory(
        IStoragePtr storage,
        std::string cliqueId,
        IAuthorizationTokenPtr authToken,
        std::string homePath,
        ICliqueAuthorizationManagerPtr cliqueAuthorizationManager)
        : Storage(std::move(storage))
        , CliqueId(std::move(cliqueId))
        , AuthToken(std::move(authToken))
        , HomePath(std::move(homePath))
        , CliqueAuthorizationManager_(std::move(cliqueAuthorizationManager))
    {}

    std::unique_ptr<DB::ISecurityManager> createSecurityManager() override
    {
        return CreateSecurityManager(CliqueId, CliqueAuthorizationManager_);
    }

    std::unique_ptr<DB::IExternalLoaderConfigRepository> createExternalDictionariesConfigRepository() override
    {
        auto repositoryPath = Storage->PathService()->Build(
            ToString(HomePath),
            {"data", "external", "dictionaries"});

        return CreateExternalLoaderConfigRepository(
            Storage,
            AuthToken,
            ToStdString(repositoryPath));
    }

    std::unique_ptr<DB::IExternalLoaderConfigRepository> createExternalModelsConfigRepository() override
    {
        auto repositoryPath = Storage->PathService()->Build(
            ToString(HomePath),
            {"data", "external", "models"});

        return CreateExternalLoaderConfigRepository(
            Storage,
            AuthToken,
            ToStdString(repositoryPath));
    }

    std::unique_ptr<IGeoDictionariesLoader> createGeoDictionariesLoader() override
    {
        auto geodataPath = Storage->PathService()->Build(
            ToString(HomePath),
            {"data", "embedded", "geodata"});

        return CreateGeoDictionariesLoader(
            Storage,
            AuthToken,
            ToStdString(geodataPath));
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IRuntimeComponentsFactory> CreateRuntimeComponentsFactory(
    IStoragePtr storage,
    std::string cliqueId,
    IAuthorizationTokenPtr authToken,
    std::string homePath,
    ICliqueAuthorizationManagerPtr cliqueAuthorizationManager)
{
    return std::make_unique<TRuntimeComponentsFactory>(
        std::move(storage),
        std::move(cliqueId),
        std::move(authToken),
        std::move(homePath),
        std::move(cliqueAuthorizationManager));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
