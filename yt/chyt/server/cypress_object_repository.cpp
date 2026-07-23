#include "cypress_object_repository.h"

#include "config.h"
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

class TExternalLoaderFromCypressObjectRepository
    : public DB::IExternalLoaderConfigRepository
{
public:
    explicit TExternalLoaderFromCypressObjectRepository(TCypressObjectRepositoryPtr repository)
        : Handler_(std::move(repository))
    { }

    std::string getName() const override
    {
        return TCypressObjectRepository::CypressConfigRepositoryName;
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
    const TCypressObjectRepositoryPtr Handler_;
};

////////////////////////////////////////////////////////////////////////////////

struct TCypressObjectRepository::TObjectSnapshot
{
    struct TEntry
    {
        std::string Type;
        std::string Value;
        DBPoco::Timestamp UpdateTime;
    };

    THashMap<std::string, TEntry> Entries;
    std::set<std::string> DictionaryNames;
};

////////////////////////////////////////////////////////////////////////////////

TCypressObjectRepository::TCypressObjectRepository(
    NNative::IClientPtr client,
    TCypressObjectRepositoryConfigPtr config,
    IInvokerPtr invoker)
    : Client_(std::move(client))
    , RootPath_(config->RootPath)
    , SnapshotExecutor_(New<TPeriodicExecutor>(
        std::move(invoker),
        BIND(&TCypressObjectRepository::RefreshSnapshot, MakeWeak(this)),
        config->UpdatePeriod))
{ }

void TCypressObjectRepository::Start()
{
    SnapshotExecutor_->Start();
}

TCypressObjectRepository::TObjectSnapshotPtr TCypressObjectRepository::GetSnapshot()
{
    auto guard = ReaderGuard(SnapshotLock_);
    return Snapshot_;
}

TCypressObjectRepository::TObjectSnapshotPtr TCypressObjectRepository::BuildSnapshot()
{
    TListNodeOptions options;
    options.Attributes = {"key", "value", "modification_time", "chyt_object_type"};

    auto listYson = WaitFor(Client_->ListNode(RootPath_, options))
        .ValueOrThrow();
    auto listNode = ConvertTo<IListNodePtr>(listYson);

    auto snapshot = std::make_shared<TObjectSnapshot>();
    for (const auto& child : listNode->GetChildren()) {
        const auto& attributes = child->Attributes();
        auto name = attributes.Get<std::string>("key");

        auto value = attributes.Find<std::string>("value");
        if (!value) {
            YT_LOG_WARNING("Clique object node is missing \"value\" attribute, skipping (Name: %v)",
                name);
            continue;
        }

        TObjectSnapshot::TEntry entry;
        // COMPAT(buyval01): Documents without the type attribute are dictionaries.
        entry.Type = attributes.Find<std::string>("chyt_object_type").value_or(DictionaryObjectType);
        entry.Value = std::move(*value);
        if (auto modificationTime = attributes.Find<TInstant>("modification_time")) {
            entry.UpdateTime = DBPoco::Timestamp::fromEpochTime(modificationTime->TimeT());
        }

        if (entry.Type == DictionaryObjectType) {
            snapshot->DictionaryNames.insert(name);
        }
        snapshot->Entries.emplace(std::move(name), std::move(entry));
    }

    YT_LOG_DEBUG("Cypress object snapshot built (RootPath: %v, ObjectCount: %v)",
        RootPath_,
        snapshot->Entries.size());

    return snapshot;
}

void TCypressObjectRepository::RefreshSnapshot()
{
    try {
        auto snapshot = BuildSnapshot();
        auto guard = WriterGuard(SnapshotLock_);
        Snapshot_ = std::move(snapshot);
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to refresh Cypress object snapshot (RootPath: %v)",
            RootPath_);
    }
}

std::set<std::string> TCypressObjectRepository::GetAllDictionaryNames()
{
    if (auto snapshot = GetSnapshot()) {
        return snapshot->DictionaryNames;
    }
    return {};
}

bool TCypressObjectRepository::DictionaryExists(const std::string& dictionaryName)
{
    auto snapshot = GetSnapshot();
    return snapshot && snapshot->DictionaryNames.contains(dictionaryName);
}

std::optional<DBPoco::Timestamp> TCypressObjectRepository::GetDictionaryUpdateTime(const std::string& dictionaryName)
{
    if (auto snapshot = GetSnapshot()) {
        if (auto it = snapshot->Entries.find(dictionaryName);
            it != snapshot->Entries.end() && it->second.Type == DictionaryObjectType)
        {
            return it->second.UpdateTime;
        }
    }
    return std::nullopt;
}

DB::LoadablesConfigurationPtr TCypressObjectRepository::LoadDictionary(const std::string& dictionaryName)
{
    auto snapshot = GetSnapshot();
    if (!snapshot) {
        THROW_ERROR_EXCEPTION("Cypress object snapshot is not ready yet");
    }
    auto it = snapshot->Entries.find(dictionaryName);
    if (it == snapshot->Entries.end() || it->second.Type != DictionaryObjectType) {
        THROW_ERROR_EXCEPTION("Dictionary %Qv is not found in Cypress object repository", dictionaryName);
    }

    std::stringstream configStream(it->second.Value);

    DBPoco::AutoPtr<DBPoco::Util::XMLConfiguration> config(new DBPoco::Util::XMLConfiguration);
    config->load(configStream);
    return config;
}

void TCypressObjectRepository::WriteDictionary(
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
    auto path = GetObjectPath(configName);
    NApi::TCreateNodeOptions options;
    options.Attributes = CreateEphemeralAttributes();
    options.Attributes->Set("value", parsedConfigStream.str());
    options.Attributes->Set("chyt_object_type", DictionaryObjectType);

    auto resultOrError = WaitFor(client->CreateNode(path, NCypressClient::EObjectType::Document, options));
    if (!resultOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Error while writing dictionary %Qv", configName) << resultOrError;
    }

    RefreshSnapshot();

    host->ReloadDictionaryGlobally(configName);
}

void TCypressObjectRepository::DeleteDictionary(
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

    auto path = GetObjectPath(storageId.table_name);
    auto resultOrError = WaitFor(client->RemoveNode(path));
    if (!resultOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Error while deleting dictionary %Qv", storageId.table_name) << resultOrError;
    }

    RefreshSnapshot();

    // Global reload may fail, but eventually all instances will notice that the dictionary has been deleted
    // due to periodic updates to ExternalLoader.
    host->ReloadDictionaryGlobally(storageId.table_name);
}

TYPath TCypressObjectRepository::GetObjectPath(const std::string& objectName) const
{
    return TYPath(Format("%v/%v", RootPath_, ToYPathLiteral(objectName)));
}

const std::string TCypressObjectRepository::CypressConfigRepositoryName = "YT_Cypress";
const std::string TCypressObjectRepository::DictionaryObjectType = "dictionary";

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IExternalLoaderConfigRepository>
CreateExternalLoaderFromCypressObjectRepository(TCypressObjectRepositoryPtr repository)
{
    return std::make_unique<TExternalLoaderFromCypressObjectRepository>(std::move(repository));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
