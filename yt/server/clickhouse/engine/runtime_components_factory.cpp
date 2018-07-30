#include "runtime_components_factory.h"

#include "external_loader_config_repository.h"
#include "geo_dictionaries_loader.h"
#include "security_manager.h"
#include "type_helpers.h"

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

class TRuntimeComponentsFactory
    : public DB::IRuntimeComponentsFactory
{
private:
    NInterop::IStoragePtr Storage;
    NInterop::IAuthorizationTokenPtr AuthToken;
    std::string HomePath;

public:
    TRuntimeComponentsFactory(
        NInterop::IStoragePtr storage,
        NInterop::IAuthorizationTokenPtr authToken,
        std::string homePath)
        : Storage(std::move(storage))
        , AuthToken(std::move(authToken))
        , HomePath(std::move(homePath))
    {}

    std::unique_ptr<DB::ISecurityManager> createSecurityManager() override
    {
        return CreateSecurityManager();
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
    NInterop::IStoragePtr storage,
    NInterop::IAuthorizationTokenPtr authToken,
    std::string homePath)
{
    return std::make_unique<TRuntimeComponentsFactory>(
        std::move(storage),
        std::move(authToken),
        std::move(homePath));
}

} // namespace NClickHouse
} // namespace NYT
