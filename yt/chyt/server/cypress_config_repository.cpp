#include "cypress_config_repository.h"

#include "host.h"
#include "query_context.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/api/cypress_client.h>

#include <DBPoco/Util/LayeredConfiguration.h>
#include <DBPoco/Util/XMLConfiguration.h>

#include <Interpreters/ExternalDictionariesLoader.h>

namespace NYT::NClickHouseServer {

using namespace NYTree;
using namespace NConcurrency;
using namespace NApi::NNative;

////////////////////////////////////////////////////////////////////////////////

class TExternalLoaderFromCypressConfigRepository
    : public DB::IExternalLoaderConfigRepository
{
public:
    explicit TExternalLoaderFromCypressConfigRepository(TCypressDictionaryConfigRepositoryPtr repository)
        : Handler_(repository)
    { }

    std::string getName() const override
    {
        return TCypressDictionaryConfigRepository::CypressConfigRepositoryName;
    }

    std::set<std::string> getAllLoadablesDefinitionNames() override
    {
        return Handler_->GetAllDictionaryNames();
    }

    bool exists(const std::string& dictionaryName) override
    {
        return Handler_->DictionaryExists(dictionaryName);
    }

    std::optional<DBPoco::Timestamp> getUpdateTime(const std::string& /*dictionaryName*/) override
    {
        return DBPoco::Timestamp::TIMEVAL_MAX;
    }

    DB::LoadablesConfigurationPtr load(const std::string& dictionaryName) override
    {
        return Handler_->LoadDictionary(dictionaryName);
    }

private:
    TCypressDictionaryConfigRepositoryPtr Handler_;
};

////////////////////////////////////////////////////////////////////////////////

TCypressDictionaryConfigRepository::TCypressDictionaryConfigRepository(
    IClientPtr client,
    TDictionaryRepositoryConfigPtr config)
    : Client_(client)
    , PathToDictionaries_(config->Path)
{ }

std::set<std::string> TCypressDictionaryConfigRepository::GetAllDictionaryNames()
{
    NApi::TListNodeOptions options;
    options.Attributes = {"key"};
    auto resultOrError = WaitFor(Client_->ListNode(PathToDictionaries_, options));
    if (!resultOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Error while loading dictionaries from cypress") << resultOrError;
    }
    auto node = ConvertTo<IListNodePtr>(resultOrError.Value());
    std::set<std::string> names;
    for (auto& child : node->GetChildren()) {
        names.insert(child->Attributes().Get<TString>("key"));
    }
    return names;
}

bool TCypressDictionaryConfigRepository::DictionaryExists(const std::string& dictionaryName)
{
    return WaitFor(Client_->NodeExists(GetPathToConfig(dictionaryName))).ValueOrDefault(false);
}

DB::LoadablesConfigurationPtr TCypressDictionaryConfigRepository::LoadDictionary(const std::string& dictionaryName)
{
    auto pathToConfig = GetPathToConfig(dictionaryName);
    NApi::TGetNodeOptions options;
    options.Attributes = {"value"};
    auto configNode = ConvertTo<IStringNodePtr>(WaitFor(Client_->GetNode(pathToConfig, options)).ValueOrThrow());
    std::stringstream configStream(configNode->GetValue());
    DBPoco::AutoPtr<DBPoco::Util::XMLConfiguration> config(new DBPoco::Util::XMLConfiguration);
    config->load(configStream);
    return config;
}

void TCypressDictionaryConfigRepository::WriteDictionary(
    const DB::ContextPtr& context,
    const std::string& name,
    const DB::LoadablesConfigurationPtr& config)
{
    const auto* queryContext = GetQueryContext(context);
    const auto& client = queryContext->Client();
    const auto* host = queryContext->Host;

    host->ValidateCliquePermission(TString(context->getClientInfo().initial_user), EPermission::Manage);

    if (name.starts_with("//")) {
        THROW_ERROR_EXCEPTION("Error while creating dictionary %Qv. Dictionary name cannot start with //.", name);
    }
    std::stringstream parsedConfigStream;
    config.cast<DBPoco::Util::XMLConfiguration>()->save(parsedConfigStream);
    auto path = GetPathToConfig(name);
    NApi::TCreateNodeOptions options;
    options.Attributes = CreateEphemeralAttributes();
    options.Attributes->Set("value", parsedConfigStream.str());

    auto resultOrError = WaitFor(client->CreateNode(path, NCypressClient::EObjectType::Document, options));
    if (resultOrError.IsOK()) {
        host->ReloadDictionaryGlobally(name);
    } else {
        THROW_ERROR_EXCEPTION("Error while writing dictionary %Qv", name) << resultOrError;
    }
}

bool TCypressDictionaryConfigRepository::DeleteDictionary(
    const DB::ContextPtr& context,
    const std::string& name,
    const std::string& databaseName)
{
    const auto* queryContext = GetQueryContext(context);
    const auto& client = queryContext->Client();
    const auto* host = queryContext->Host;
    host->ValidateCliquePermission(TString(context->getClientInfo().initial_user), EPermission::Manage);

    const auto& externalDictionariesLoader = context->getExternalDictionariesLoader();
    if (!externalDictionariesLoader.has(DB::StorageID(databaseName, name).getFullTableName())) {
        return false;
    }

    auto path = GetPathToConfig(name);
    auto resultOrError = WaitFor(client->RemoveNode(path));
    if (resultOrError.IsOK()) {
        host->ReloadDictionaryGlobally(name);
        return true;
    } else if (resultOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
        return false;
    } else {
        THROW_ERROR_EXCEPTION("Error while deleting dictionary %Qv", name) << resultOrError;
    }
}

TYPath TCypressDictionaryConfigRepository::GetPathToConfig(const std::string& dictionaryName)
{
    return TYPath(Format("%v/%v", PathToDictionaries_, dictionaryName));
}

const std::string TCypressDictionaryConfigRepository::CypressConfigRepositoryName = "YT_Cypress";

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IExternalLoaderConfigRepository> CreateExternalLoaderFromCypressConfigRepository(TCypressDictionaryConfigRepositoryPtr cypressDictionaryConfigRepository)
{
    return std::make_unique<TExternalLoaderFromCypressConfigRepository>(cypressDictionaryConfigRepository);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
