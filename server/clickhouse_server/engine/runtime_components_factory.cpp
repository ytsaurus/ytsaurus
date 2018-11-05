#include "runtime_components_factory.h"

#include "external_loader_config_repository.h"
#include "geo_dictionaries_loader.h"
#include "security_manager.h"
#include "type_helpers.h"

#include <yt/server/clickhouse_server/native/storage.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

class TRuntimeComponentsFactory
    : public DB::IRuntimeComponentsFactory
{
private:
    NNative::IStoragePtr Storage;
    std::string CliqueId;
    NNative::IAuthorizationTokenPtr AuthToken;
    std::string HomePath;
    NNative::ICliqueAuthorizationManagerPtr CliqueAuthorizationManager_;

public:
    TRuntimeComponentsFactory(
        NNative::IStoragePtr storage,
        std::string cliqueId,
        NNative::IAuthorizationTokenPtr authToken,
        std::string homePath,
        NNative::ICliqueAuthorizationManagerPtr cliqueAuthorizationManager)
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
    NNative::IStoragePtr storage,
    std::string cliqueId,
    NNative::IAuthorizationTokenPtr authToken,
    std::string homePath,
    NNative::ICliqueAuthorizationManagerPtr cliqueAuthorizationManager)
{
    return std::make_unique<TRuntimeComponentsFactory>(
        std::move(storage),
        std::move(cliqueId),
        std::move(authToken),
        std::move(homePath),
        std::move(cliqueAuthorizationManager));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
