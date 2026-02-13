#include "cypress_config_repository.h"

#include "host.h"
#include "query_context.h"

#include <yt/yt/client/api/cypress_client.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <DBPoco/Util/LayeredConfiguration.h>
#include <DBPoco/Util/XMLConfiguration.h>

#include <Interpreters/ExternalDictionariesLoader.h>

namespace NYT::NClickHouseServer {

using namespace NApi;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYPath;

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

    std::optional<DBPoco::Timestamp> getUpdateTime(const std::string& dictionaryName) override
    {
        return Handler_->GetDictionaryUpdateTime(dictionaryName);
    }

    DB::LoadablesConfigurationPtr load(const std::string& dictionaryName) override
    {
        return Handler_->LoadDictionary(dictionaryName);
    }

private:
    const TCypressDictionaryConfigRepositoryPtr Handler_;
};

////////////////////////////////////////////////////////////////////////////////

TCypressDictionaryConfigRepository::TCypressDictionaryConfigRepository(
    NNative::IClientPtr client,
    TDictionaryRepositoryConfigPtr config)
    : Client_(client)
    , RootPath_(config->RootPath)
{ }

std::set<std::string> TCypressDictionaryConfigRepository::GetAllDictionaryNames()
{
    TListNodeOptions options;
    options.Attributes = {"key"};
    auto resultOrError = WaitFor(Client_->ListNode(RootPath_, options));
    if (!resultOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Error while loading dictionaries from Cypress") << resultOrError;
    }
    auto node = ConvertTo<IListNodePtr>(resultOrError.Value());
    std::set<std::string> names;
    for (const auto& child : node->GetChildren()) {
        names.insert(child->Attributes().Get<std::string>("key"));
    }
    return names;
}

bool TCypressDictionaryConfigRepository::DictionaryExists(const std::string& dictionaryName)
{
    return WaitFor(Client_->NodeExists(GetPathToConfig(dictionaryName))).ValueOrDefault(false);
}

std::optional<DBPoco::Timestamp> TCypressDictionaryConfigRepository::GetDictionaryUpdateTime(const std::string& dictionaryName)
{
    auto attrYson = WaitFor(Client_->GetNode(Format("%v/@modification_time", GetPathToConfig(dictionaryName))))
        .ValueOrThrow();
    auto modificationTime = ConvertTo<TInstant>(attrYson);

    return DBPoco::Timestamp::fromEpochTime(modificationTime.TimeT());
}

DB::LoadablesConfigurationPtr TCypressDictionaryConfigRepository::LoadDictionary(const std::string& dictionaryName)
{
    TGetNodeOptions options;
    options.Attributes = {"value"};

    auto configYson = WaitFor(Client_->GetNode(GetPathToConfig(dictionaryName), options))
        .ValueOrThrow();

    std::stringstream configStream(ConvertTo<IStringNodePtr>(configYson)->GetValue());

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

    std::stringstream parsedConfigStream;
    config.cast<DBPoco::Util::XMLConfiguration>()->save(parsedConfigStream);
    auto path = GetPathToConfig(name);
    NApi::TCreateNodeOptions options;
    options.Attributes = CreateEphemeralAttributes();
    options.Attributes->Set("value", parsedConfigStream.str());

    auto resultOrError = WaitFor(client->CreateNode(path, NCypressClient::EObjectType::Document, options));
    if (!resultOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Error while writing dictionary %Qv", name) << resultOrError;
    }

    host->ReloadDictionaryGlobally(name);
}

void TCypressDictionaryConfigRepository::DeleteDictionary(
    const DB::ContextPtr& context,
    const DB::StorageID& storageId)
{
    const auto* queryContext = GetQueryContext(context);
    const auto& client = queryContext->Client();
    const auto* host = queryContext->Host;
    host->ValidateCliquePermission(TString(context->getClientInfo().initial_user), EPermission::Manage);

    const auto& externalDictionariesLoader = context->getExternalDictionariesLoader();
    if (!externalDictionariesLoader.has(storageId.getInternalDictionaryName())) {
        return;
    }

    auto path = GetPathToConfig(storageId.table_name);
    auto resultOrError = WaitFor(client->RemoveNode(path));
    if (!resultOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Error while deleting dictionary %Qv", storageId.table_name) << resultOrError;
    }

    // Global reload may fail, but eventually all instances will notice that the dictionary has been deleted
    // due to periodic updates to ExternalLoader.
    host->ReloadDictionaryGlobally(storageId.table_name);
}

TYPath TCypressDictionaryConfigRepository::GetPathToConfig(const std::string& dictionaryName) const
{
    return TYPath(Format("%v/%v", RootPath_, ToYPathLiteral(dictionaryName)));
}

const std::string TCypressDictionaryConfigRepository::CypressConfigRepositoryName = "YT_Cypress";

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IExternalLoaderConfigRepository>
CreateExternalLoaderFromCypressConfigRepository(TCypressDictionaryConfigRepositoryPtr cypressDictionaryConfigRepository)
{
    return std::make_unique<TExternalLoaderFromCypressConfigRepository>(cypressDictionaryConfigRepository);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
