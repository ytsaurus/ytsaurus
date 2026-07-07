#include "cypress_config_repository.h"

#include "host.h"
#include "query_context.h"

#include <yt/yt/client/api/cypress_client.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <util/generic/hash.h>

#include <DBPoco/Util/LayeredConfiguration.h>
#include <DBPoco/Util/XMLConfiguration.h>

#include <Interpreters/ExternalDictionariesLoader.h>

namespace NYT::NClickHouseServer {

using namespace NApi;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYPath;

constinit const auto Logger = ClickHouseYtLogger;

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

struct TCypressDictionaryConfigRepository::TDictionaryConfigSnapshot
{
    struct TEntry
    {
        DBPoco::Timestamp UpdateTime;
        std::string ConfigXml;
    };

    THashMap<std::string, TEntry> Entries;
    std::set<std::string> Names;
};

////////////////////////////////////////////////////////////////////////////////

TCypressDictionaryConfigRepository::TCypressDictionaryConfigRepository(
    NNative::IClientPtr client,
    TDictionaryRepositoryConfigPtr config,
    IInvokerPtr invoker)
    : Client_(std::move(client))
    , RootPath_(config->RootPath)
    , SnapshotExecutor_(New<TPeriodicExecutor>(
        std::move(invoker),
        BIND(&TCypressDictionaryConfigRepository::RefreshSnapshot, MakeWeak(this)),
        config->UpdatePeriod))
{ }

void TCypressDictionaryConfigRepository::Start()
{
    SnapshotExecutor_->Start();
}

TCypressDictionaryConfigRepository::TDictionaryConfigSnapshotPtr TCypressDictionaryConfigRepository::GetSnapshot()
{
    auto guard = ReaderGuard(SnapshotLock_);
    return Snapshot_;
}

TCypressDictionaryConfigRepository::TDictionaryConfigSnapshotPtr TCypressDictionaryConfigRepository::BuildSnapshot()
{
    TListNodeOptions options;
    options.Attributes = {"key", "value", "modification_time"};

    auto listYson = WaitFor(Client_->ListNode(RootPath_, options))
        .ValueOrThrow();
    auto listNode = ConvertTo<IListNodePtr>(listYson);

    auto snapshot = std::make_shared<TDictionaryConfigSnapshot>();
    for (const auto& child : listNode->GetChildren()) {
        const auto& attributes = child->Attributes();
        auto name = attributes.Get<std::string>("key");

        auto configXml = attributes.Find<std::string>("value");
        if (!configXml) {
            YT_LOG_WARNING("Dictionary config node is missing \"value\" attribute, skipping (Name: %v)",
                name);
            continue;
        }

        TDictionaryConfigSnapshot::TEntry entry;
        entry.ConfigXml = std::move(*configXml);
        if (auto modificationTime = attributes.Find<TInstant>("modification_time")) {
            entry.UpdateTime = DBPoco::Timestamp::fromEpochTime(modificationTime->TimeT());
        }

        snapshot->Names.insert(name);
        snapshot->Entries.emplace(std::move(name), std::move(entry));
    }

    YT_LOG_DEBUG("Cypress dictionary config snapshot built (RootPath: %v, DictionaryCount: %v)",
        RootPath_,
        snapshot->Names.size());

    return snapshot;
}

void TCypressDictionaryConfigRepository::RefreshSnapshot()
{
    try {
        auto snapshot = BuildSnapshot();
        auto guard = WriterGuard(SnapshotLock_);
        Snapshot_ = std::move(snapshot);
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to refresh Cypress dictionary config snapshot (RootPath: %v)",
            RootPath_);
    }
}

std::set<std::string> TCypressDictionaryConfigRepository::GetAllDictionaryNames()
{
    if (auto snapshot = GetSnapshot()) {
        return snapshot->Names;
    }
    return {};
}

bool TCypressDictionaryConfigRepository::DictionaryExists(const std::string& dictionaryName)
{
    auto snapshot = GetSnapshot();
    return snapshot && snapshot->Entries.contains(dictionaryName);
}

std::optional<DBPoco::Timestamp> TCypressDictionaryConfigRepository::GetDictionaryUpdateTime(const std::string& dictionaryName)
{
    if (auto snapshot = GetSnapshot()) {
        if (auto it = snapshot->Entries.find(dictionaryName); it != snapshot->Entries.end()) {
            return it->second.UpdateTime;
        }
    }
    return std::nullopt;
}

DB::LoadablesConfigurationPtr TCypressDictionaryConfigRepository::LoadDictionary(const std::string& dictionaryName)
{
    auto snapshot = GetSnapshot();
    if (!snapshot) {
        THROW_ERROR_EXCEPTION("Cypress dictionary config snapshot is not ready yet");
    }
    auto it = snapshot->Entries.find(dictionaryName);
    if (it == snapshot->Entries.end()) {
        THROW_ERROR_EXCEPTION("Dictionary %Qv is not found in Cypress config repository", dictionaryName);
    }

    std::stringstream configStream(it->second.ConfigXml);

    DBPoco::AutoPtr<DBPoco::Util::XMLConfiguration> config(new DBPoco::Util::XMLConfiguration);
    config->load(configStream);
    return config;
}

void TCypressDictionaryConfigRepository::WriteDictionary(
    const DB::ContextPtr& context,
    const DB::StorageID& storageId,
    const DB::LoadablesConfigurationPtr& config)
{
    const auto* queryContext = GetQueryContext(context);
    const auto& client = queryContext->Client();
    const auto* host = queryContext->Host;
    host->ValidateCliquePermission(TString(context->getClientInfo().initial_user), EPermission::Manage);

    auto configName = storageId.table_name;

    std::stringstream parsedConfigStream;
    config.cast<DBPoco::Util::XMLConfiguration>()->save(parsedConfigStream);
    auto path = GetPathToConfig(configName);
    NApi::TCreateNodeOptions options;
    options.Attributes = CreateEphemeralAttributes();
    options.Attributes->Set("value", parsedConfigStream.str());

    auto resultOrError = WaitFor(client->CreateNode(path, NCypressClient::EObjectType::Document, options));
    if (!resultOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Error while writing dictionary %Qv", configName) << resultOrError;
    }

    RefreshSnapshot();

    host->ReloadDictionaryGlobally(configName);
}

void TCypressDictionaryConfigRepository::DeleteDictionary(
    const DB::ContextPtr& context,
    const DB::StorageID& storageId)
{
    const auto* queryContext = GetQueryContext(context);
    const auto& client = queryContext->Client();
    const auto* host = queryContext->Host;
    host->ValidateCliquePermission(context->getClientInfo().initial_user, EPermission::Manage);

    const auto& externalDictionariesLoader = context->getExternalDictionariesLoader();
    if (!externalDictionariesLoader.has(storageId.getInternalDictionaryName())) {
        return;
    }

    auto path = GetPathToConfig(storageId.table_name);
    auto resultOrError = WaitFor(client->RemoveNode(path));
    if (!resultOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Error while deleting dictionary %Qv", storageId.table_name) << resultOrError;
    }

    RefreshSnapshot();

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
