#include "config_repository.h"

#include "clickhouse_config.h"
#include "poco_config.h"

#include <yt/yt/core/misc/common.h>
#include <yt/yt/core/ytree/fluent.h>

#include <Poco/Util/LayeredConfiguration.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;

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

    std::string getName() const override
    {
        return Name_;
    }

    std::set<std::string> getAllLoadablesDefinitionNames() override
    {
        return Keys_;
    }

    bool exists(const std::string& configFile) override
    {
        return Dictionaries_.contains(configFile);
    }

    Poco::Timestamp getUpdateTime(const std::string& /*configFile*/) override
    {
        return Poco::Timestamp::TIMEVAL_MAX;
    }

    Poco::AutoPtr<Poco::Util::AbstractConfiguration> load(const std::string& configFile) override
    {
        return ConvertToPocoConfig(BuildYsonNodeFluently()
            .BeginMap()
                .Item("dictionary").Value(GetOrCrash(Dictionaries_, configFile))
            .EndMap());
    }

private:
    THashMap<TString, TDictionaryConfigPtr> Dictionaries_;
    std::set<std::string> Keys_;

    static const std::string Name_;
};

const std::string TDictionaryConfigRepository::Name_ = "YT";

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IExternalLoaderConfigRepository> CreateDictionaryConfigRepository(
    const std::vector<TDictionaryConfigPtr>& dictionaries)
{
    return std::make_unique<TDictionaryConfigRepository>(dictionaries);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
