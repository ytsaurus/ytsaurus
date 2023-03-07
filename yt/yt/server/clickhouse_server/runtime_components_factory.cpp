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
        std::unique_ptr<DB::IUsersManager> usersManager,
        std::unique_ptr<DB::IExternalLoaderConfigRepository> dictionariesConfigRepository,
        std::unique_ptr<IGeoDictionariesLoader> geoDictionariesLoader)
        : UsersManager_(std::move(usersManager))
        , DictionariesConfigRepository_(std::move(dictionariesConfigRepository))
        , GeoDictionariesLoader_(std::move(geoDictionariesLoader))
    { }

    std::unique_ptr<DB::IUsersManager> createUsersManager() override
    {
        YT_VERIFY(UsersManager_);
        return std::move(UsersManager_);
    }

    std::unique_ptr<DB::IExternalLoaderConfigRepository> createExternalDictionariesConfigRepository() override
    {
        YT_VERIFY(DictionariesConfigRepository_);
        return std::move(DictionariesConfigRepository_);
    }

    std::unique_ptr<DB::IExternalLoaderConfigRepository> createExternalModelsConfigRepository() override
    {
        return CreateDummyConfigRepository();
    }

    std::unique_ptr<IGeoDictionariesLoader> createGeoDictionariesLoader() override
    {
        YT_VERIFY(GeoDictionariesLoader_);
        return std::move(GeoDictionariesLoader_);
    }

private:
    std::unique_ptr<DB::IUsersManager> UsersManager_;
    std::unique_ptr<DB::IExternalLoaderConfigRepository> DictionariesConfigRepository_;
    std::unique_ptr<IGeoDictionariesLoader> GeoDictionariesLoader_;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IRuntimeComponentsFactory> CreateRuntimeComponentsFactory(
    std::unique_ptr<DB::IUsersManager> usersManager,
    std::unique_ptr<DB::IExternalLoaderConfigRepository> dictionariesConfigRepository,
    std::unique_ptr<IGeoDictionariesLoader> geoDictionariesLoader)
{
    return std::make_unique<TRuntimeComponentsFactory>(
        std::move(usersManager),
        std::move(dictionariesConfigRepository),
        std::move(geoDictionariesLoader));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
