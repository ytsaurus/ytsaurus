#include "external_loader_config_repository.h"

#include "auth_token.h"
#include "config_repository.h"
#include "logging_helpers.h"
#include "type_helpers.h"

//#include <Poco/Logger.h>

//#include <common/logger_useful.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

class TExternalLoaderConfigRepository
    : public DB::IExternalLoaderConfigRepository
{
private:
    IConfigRepositoryPtr ConfigRepository;

    Poco::Logger* Logger;

public:
    TExternalLoaderConfigRepository(IConfigRepositoryPtr repository)
        : ConfigRepository(repository)
        , Logger(&Poco::Logger::get("ExternalLoaderConfigRepository"))
    {}

    std::set<std::string> list(
        const Poco::Util::AbstractConfiguration& config, const std::string& pathKey) const override;

    bool exists(const std::string& configFile) const override;

    Poco::Timestamp getLastModificationTime(
        const std::string& configFile) const override;

    Poco::AutoPtr<Poco::Util::AbstractConfiguration> load(
        const std::string& configFile) const override;
};

////////////////////////////////////////////////////////////////////////////////

std::set<std::string> TExternalLoaderConfigRepository::list(
    const Poco::Util::AbstractConfiguration& config,
    const std::string& pathKey) const
{
    Y_UNUSED(config);
    Y_UNUSED(pathKey);

    try {
        auto configs = ConfigRepository->List();
        return {configs.begin(), configs.end()};
    } catch (...) {
        // Workaround to prevent crash of background reloader thread in DB::ExternalReloader
        CH_LOG_WARNING(
           Logger,
           "Error occurred while listing repository with address " << ConfigRepository->GetAddress() <<
           ": " << CurrentExceptionText());
        return {};
    }
}

bool TExternalLoaderConfigRepository::exists(
    const std::string& configFile) const
{
    return ConfigRepository->Exists(configFile);
}

Poco::Timestamp TExternalLoaderConfigRepository::getLastModificationTime(
    const std::string& configFile) const
{
    auto meta = ConfigRepository->GetAttributes(configFile);
    return ToTimestamp(meta.LastModificationTime);
}

Poco::AutoPtr<Poco::Util::AbstractConfiguration> TExternalLoaderConfigRepository::load(
    const std::string& configFile) const
{
    return new Poco::Util::LayeredConfiguration();
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IExternalLoaderConfigRepository> CreateExternalLoaderConfigRepository(
    NNative::IStoragePtr storage,
    NNative::IAuthorizationTokenPtr authToken,
    const std::string& path)
{
    auto repository = CreateConfigRepository(std::move(storage), std::move(authToken), path);
    return std::make_unique<TExternalLoaderConfigRepository>(std::move(repository));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
