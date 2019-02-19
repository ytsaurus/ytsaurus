#include "dummy_config_repository.h"

#include <Poco/Util/LayeredConfiguration.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TDummyConfigRepository
    : public DB::IExternalLoaderConfigRepository
{
public:
    TDummyConfigRepository() = default;

    virtual std::set<std::string> list(const Poco::Util::AbstractConfiguration& config, const std::string& pathKey) const override;

    virtual bool exists(const std::string& configFile) const override;

    virtual Poco::Timestamp getLastModificationTime(const std::string& configFile) const override;

    virtual Poco::AutoPtr<Poco::Util::AbstractConfiguration> load(const std::string& configFile) const override;
};

////////////////////////////////////////////////////////////////////////////////

std::set<std::string> TDummyConfigRepository::list(
    const Poco::Util::AbstractConfiguration& /* config */,
    const std::string& /* pathKey */) const
{
    return {};
}

bool TDummyConfigRepository::exists(const std::string& /* configFile */) const
{
    return false;
}

Poco::Timestamp TDummyConfigRepository::getLastModificationTime(const std::string& /* configFile */) const
{
    return Poco::Timestamp::TIMEVAL_MAX;
}

Poco::AutoPtr<Poco::Util::AbstractConfiguration> TDummyConfigRepository::load(const std::string& /* configFile */) const
{
    return new Poco::Util::LayeredConfiguration();
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IExternalLoaderConfigRepository> CreateDummyConfigRepository()
{
    return std::make_unique<TDummyConfigRepository>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
