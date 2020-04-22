#include "config_repository.h"

#include "config.h"

#include "poco_config.h"

#include <yt/core/misc/common.h>

#include <Poco/Util/LayeredConfiguration.h>

namespace NYT::NClickHouseServer {

/////////////////////////////////////////////////////////////////////////////

class TDictionaryConfigRepository
    : public DB::IExternalLoaderConfigRepository
{
public:
    explicit TDictionaryConfigRepository(const std::vector<TDictionaryConfigPtr>& dictionaries)
    {
        Dictionaries_.reserve(dictionaries.size());
        for (const auto& dictionary : dictionaries) {
            if (!Dictionaries_.emplace(dictionary->Name, dictionary).second) {
                THROW_ERROR_EXCEPTION("Duplicating dictionary name %Qv", dictionary->Name);
            };
            Keys_.emplace(dictionary->Name);
        }
    }

    virtual std::set<std::string> list(const Poco::Util::AbstractConfiguration& /* config */, const std::string& /* pathKey */) const override
    {
        return Keys_;
    }

    virtual bool exists(const std::string& configFile) const override
    {
        return Dictionaries_.contains(configFile);
    }

    virtual Poco::Timestamp getLastModificationTime(const std::string& /* configFile */) const override
    {
        return Poco::Timestamp::TIMEVAL_MAX;
    }

    virtual Poco::AutoPtr<Poco::Util::AbstractConfiguration> load(
        const std::string& configFile,
        const std::string & /* preprocessed_dir = "" */) const override
    {
        return ConvertToPocoConfig(NYTree::BuildYsonNodeFluently()
            .BeginMap()
                .Item("dictionary").Value(GetOrCrash(Dictionaries_, configFile))
            .EndMap());
    }

private:
    THashMap<TString, TDictionaryConfigPtr> Dictionaries_;
    std::set<std::string> Keys_;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IExternalLoaderConfigRepository> CreateDictionaryConfigRepository(
    const std::vector<TDictionaryConfigPtr>& dictionaries)
{
    return std::make_unique<TDictionaryConfigRepository>(dictionaries);
}

////////////////////////////////////////////////////////////////////////////////

class TDummyConfigRepository
    : public DB::IExternalLoaderConfigRepository
{
public:
    TDummyConfigRepository() = default;

    virtual std::set<std::string> list(
        const Poco::Util::AbstractConfiguration& /* config */,
        const std::string& /* pathKey */) const override
    {
        return {};
    }

    virtual bool exists(const std::string& /* configFile */) const override
    {
        return false;
    }

    virtual Poco::Timestamp getLastModificationTime(const std::string& /* configFile */) const override
    {
        YT_UNIMPLEMENTED();
    }

    virtual Poco::AutoPtr<Poco::Util::AbstractConfiguration> load(
        const std::string& /* configFile */,
        const std::string & /* preprocessed_dir = "" */) const override
    {
        YT_UNIMPLEMENTED();
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IExternalLoaderConfigRepository> CreateDummyConfigRepository()
{
    return std::make_unique<TDummyConfigRepository>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
