#include "runtime_components_factory.h"

#include "config_repository.h"

#include <Interpreters/IRuntimeComponentsFactory.h>
#include <Dictionaries/Embedded/IGeoDictionariesLoader.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TRuntimeComponentsFactory
    : public DB::IRuntimeComponentsFactory
{
public:
    TRuntimeComponentsFactory(
        std::unique_ptr<DB::ISecurityManager> securityManager,
        std::unique_ptr<DB::IExternalLoaderConfigRepository> dictionariesConfigRepository,
        std::unique_ptr<IGeoDictionariesLoader> geoDictionariesLoader)
        : SecurityManager_(std::move(securityManager))
        , DictionariesConfigRepository_(std::move(dictionariesConfigRepository))
        , GeoDictionariesLoader_(std::move(geoDictionariesLoader))
    {}

    std::unique_ptr<DB::ISecurityManager> createSecurityManager() override
    {
        YCHECK(SecurityManager_);
        return std::move(SecurityManager_);
    }

    std::unique_ptr<DB::IExternalLoaderConfigRepository> createExternalDictionariesConfigRepository() override
    {
        YCHECK(DictionariesConfigRepository_);
        return std::move(DictionariesConfigRepository_);
    }

    std::unique_ptr<DB::IExternalLoaderConfigRepository> createExternalModelsConfigRepository() override
    {
        return CreateDummyConfigRepository();
    }

    std::unique_ptr<IGeoDictionariesLoader> createGeoDictionariesLoader() override
    {
        YCHECK(GeoDictionariesLoader_);
        return std::move(GeoDictionariesLoader_);
    }

private:
    std::unique_ptr<DB::ISecurityManager> SecurityManager_;
    std::unique_ptr<DB::IExternalLoaderConfigRepository> DictionariesConfigRepository_;
    std::unique_ptr<IGeoDictionariesLoader> GeoDictionariesLoader_;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IRuntimeComponentsFactory> CreateRuntimeComponentsFactory(
    std::unique_ptr<DB::ISecurityManager> securityManager,
    std::unique_ptr<DB::IExternalLoaderConfigRepository> dictionariesConfigRepository,
    std::unique_ptr<IGeoDictionariesLoader> geoDictionariesLoader)
{
    return std::make_unique<TRuntimeComponentsFactory>(
        std::move(securityManager),
        std::move(dictionariesConfigRepository),
        std::move(geoDictionariesLoader));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
