#include "runtime_components_factory.h"

#include "dummy_config_repository.h"
//#include "geo_dictionaries_loader.h"
#include "security_manager.h"
#include "type_helpers.h"

#include "query_context.h"

#include <Dictionaries/Embedded/GeoDictionariesLoader.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): put native client here.
class TRuntimeComponentsFactory
    : public DB::IRuntimeComponentsFactory
{
private:
    std::string CliqueId;
    std::string HomePath;
    ICliqueAuthorizationManagerPtr CliqueAuthorizationManager_;

public:
    TRuntimeComponentsFactory(
        std::string cliqueId,
        std::string homePath,
        ICliqueAuthorizationManagerPtr cliqueAuthorizationManager)
        : CliqueId(std::move(cliqueId))
        , HomePath(std::move(homePath))
        , CliqueAuthorizationManager_(std::move(cliqueAuthorizationManager))
    {}

    std::unique_ptr<DB::ISecurityManager> createSecurityManager() override
    {
        return CreateSecurityManager(CliqueId, CliqueAuthorizationManager_);
    }

    std::unique_ptr<DB::IExternalLoaderConfigRepository> createExternalDictionariesConfigRepository() override
    {
        return CreateDummyConfigRepository();
    }

    std::unique_ptr<DB::IExternalLoaderConfigRepository> createExternalModelsConfigRepository() override
    {
        return CreateDummyConfigRepository();
    }

    std::unique_ptr<IGeoDictionariesLoader> createGeoDictionariesLoader() override
    {
        return std::make_unique<GeoDictionariesLoader>();
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IRuntimeComponentsFactory> CreateRuntimeComponentsFactory(
    std::string cliqueId,
    std::string homePath,
    ICliqueAuthorizationManagerPtr cliqueAuthorizationManager)
{
    return std::make_unique<TRuntimeComponentsFactory>(
        std::move(cliqueId),
        std::move(homePath),
        std::move(cliqueAuthorizationManager));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
