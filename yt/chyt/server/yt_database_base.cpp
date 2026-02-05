#include "yt_database_base.h"

#include "storage_distributor.h"
#include "helpers.h"
#include "query_context.h"
#include "table.h"
#include "host.h"
#include "cypress_config_repository.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/convert.h>

#include <Common/Exception.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>

#include <Storages/IStorage.h>
#include <Storages/StorageDictionary.h>

#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <DBPoco/Util/XMLConfiguration.h>

#include <memory>
#include <string>
#include <vector>

namespace NYT::NClickHouseServer {

using namespace NApi;
using namespace NConcurrency;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;
using namespace NStatisticPath;

////////////////////////////////////////////////////////////////////////////////

TYtDatabaseBase::TYtDatabaseBase(String databaseName)
    : DB::IDatabase(std::move(databaseName))
{ }

void TYtDatabaseBase::createTable(
    const DB::ContextPtr context,
    const std::string& name,
    const DB::StoragePtr& table,
    const DB::ASTPtr& /*query*/)
{
    if (table->getName() == "Dictionary") {
        auto* queryContext = GetQueryContext(context);
        auto host = queryContext->Host;
        host->GetCypressDictionaryConfigRepository()->WriteDictionary(
            context,
            name,
            dynamic_pointer_cast<DB::StorageDictionary>(table)->getConfiguration());
    }
    else if (table->getName() != "StorageDistributor") {
        THROW_ERROR_EXCEPTION("Table engine %Qv may not be stored in YT database: only YtTable and Dictionary engine are supported",
            table->getName());
    }
    // Table already created, nothing to do here.
}

void TYtDatabaseBase::shutdown()
{
    // Do nothing.
}

bool TYtDatabaseBase::isTableExist(const String& name, DB::ContextPtr context) const
{
    return DoGetTable(context, name) != nullptr;
}

DB::StoragePtr TYtDatabaseBase::tryGetTable(const String& name, DB::ContextPtr context) const
{
    return DoGetTable(context, name);
}


bool TYtDatabaseBase::empty() const
{
    // TODO(max42): what does this affect?
    return false;
}

bool TYtDatabaseBase::canContainMergeTreeTables() const
{
    return false;
}

bool TYtDatabaseBase::canContainDistributedTables() const
{
    return false;
}

DB::ASTPtr TYtDatabaseBase::getCreateDatabaseQuery() const
{
    THROW_ERROR_EXCEPTION("Getting CREATE DATABASE query is not supported");
}

void TYtDatabaseBase::dropTable(DB::ContextPtr context, const String& name, bool /*noDelay*/)
{
    auto* queryContext = GetQueryContext(context);
    auto* host = queryContext->Host;
    auto timerGuard = queryContext->CreateStatisticsTimerGuard(
        SlashedStatisticPath(
            Format("/%v_database/drop_table", to_lower(TString(getDatabaseName())))).ValueOrThrow());

    DB::StorageID tableId(getDatabaseName(), name);
    if (context->getExternalDictionariesLoader().has(tableId.getFullTableName())) {
        host->GetCypressDictionaryConfigRepository()->DeleteDictionary(context, tableId);
        return;
    }

    TYPath path = getTableDataPath(name);

    // We can't use Client->RemoveNode() because we need to get the revision of the removed node.
    auto proxy = NObjectClient::CreateObjectServiceWriteProxy(queryContext->Client());
    auto batchReq = proxy.ExecuteBatch();
    batchReq->AddRequest(TYPathProxy::Remove(path));
    auto batchRsp = WaitFor(batchReq->Invoke()).ValueOrThrow();
    auto refreshRevision = NHydra::TRevision(batchRsp->GetRevision(0).Underlying() + 1);

    InvalidateCache(queryContext, {{path, refreshRevision}});
}

void TYtDatabaseBase::renameTable(
    DB::ContextPtr context,
    const String& name,
    IDatabase& /*toDatabase*/,
    const String& toName,
    bool exchange,
    bool dictionary)
{
    if (dictionary) {
        THROW_ERROR_EXCEPTION("Renaming dictionaries is not supported");
    }

    auto* queryContext = GetQueryContext(context);
    auto timerGuard = queryContext->CreateStatisticsTimerGuard(
        SlashedStatisticPath(
            Format("/%v_database/rename_table", to_lower(TString(getDatabaseName())))).ValueOrThrow());

    auto client = queryContext->Client();
    TYPath srcPath = getTableDataPath(name);
    TYPath dstPath = getTableDataPath(toName);

    const auto& Logger = ClickHouseYtLogger;
    YT_LOG_DEBUG("Renaming table (SrcPath: %v, DstPath: %v, Exchange: %v)", srcPath, dstPath, exchange);

    auto srcRefreshRevision = NHydra::NullRevision;
    auto dstRefreshRevision = NHydra::NullRevision;
    if (exchange) {
        auto transaction = WaitFor(client->StartTransaction(NTransactionClient::ETransactionType::Master))
            .ValueOrThrow();
        auto tmpPath = TYPath(Format("//tmp/tmp_exchange_table_%v", transaction->GetId()));

        WaitFor(transaction->MoveNode(srcPath, tmpPath))
            .ThrowOnError();
        WaitFor(transaction->MoveNode(dstPath, srcPath))
            .ThrowOnError();
        WaitFor(transaction->MoveNode(tmpPath, dstPath))
            .ThrowOnError();

        WaitFor(transaction->Commit())
            .ThrowOnError();

        srcRefreshRevision = GetRefreshRevision(client, srcPath);
    } else {
        WaitFor(client->MoveNode(srcPath, dstPath))
            .ThrowOnError();
    }
    dstRefreshRevision = GetRefreshRevision(client, dstPath);

    InvalidateCache(queryContext, {{srcPath, srcRefreshRevision}, {dstPath, dstRefreshRevision}});
}

DB::ASTPtr TYtDatabaseBase::getCreateTableQueryImpl(const String& name, DB::ContextPtr context, bool throwOnError) const
{
    auto* queryContext = GetQueryContext(context);
    auto path = TRichYPath::Parse(getTableDataPath(name));

    TGetNodeOptions options;
    static_cast<TMasterReadOptions&>(options) = *queryContext->SessionSettings->CypressReadOptions;
    options.Attributes = {
        "compression_codec",
        "erasure_codec",
        "replication_factor",
        "optimize_for",
        "schema",
    };
    auto result = NConcurrency::WaitFor(queryContext->Client()->GetNode(path.GetPath(), options))
        .ValueOrThrow();
    auto attributesYson = ConvertToYsonString(
        ConvertToNode(result)->Attributes().ToMap(),
        EYsonFormat::Text);
    auto query = Format("CREATE TABLE %v.\"%v\"(_ UInt8) ENGINE YtTable('%v')", getDatabaseName(), name, attributesYson);

    DB::ParserCreateQuery parser;
    std::string errorMessage;
    const char* data = query.data();
    auto ast = tryParseQuery(
        parser,
        data,
        data + query.size(),
        errorMessage,
        false /*hilite*/,
        "(n/a)",
        false /*allow_multi_statements*/,
        0 /*max_query_size*/,
        DB::DBMS_DEFAULT_MAX_PARSER_DEPTH,
        DB::DBMS_DEFAULT_MAX_PARSER_BACKTRACKS,
        true /*skip_insignificant*/);

    if (!ast && throwOnError) {
        THROW_ERROR_EXCEPTION("Caught following error while parsing table creation query: %v", errorMessage);
    }

    return ast;
}

DB::StoragePtr TYtDatabaseBase::DoGetTable(
    DB::ContextPtr context,
    const String& name) const
{
    auto tableId = DB::StorageID(getDatabaseName(), name);
    if (context->getExternalDictionariesLoader().has(tableId.getFullTableName())) {
        return DoGetDictionary(context, tableId);
    }

    // Normally it's called with a query context.
    // In rare cases CH tries to find special tables (e.g. Dictionary)
    // outside of query execution and provides a global context.
    // We do not support such tables, so returning nullptr is fine.
    if (context->isGlobalContext()) {
        return nullptr;
    }

    auto* queryContext = GetQueryContext(context);
    auto timerGuard = queryContext->CreateStatisticsTimerGuard(
        SlashedStatisticPath(
            Format("/%v_database/do_get_table", to_lower(TString(getDatabaseName())))).ValueOrThrow());


    TYPath path = getTableDataPath(name);
    TRichYPath richPath;
    try {
        richPath = TRichYPath::Parse(path);
    } catch (const std::exception& /*ex*/) {
        return nullptr;
    }

    try {
        auto tables = FetchTablesSoft(
            queryContext,
            {std::move(richPath)},
            /*skipUnsuitableNodes*/ false,
            queryContext->SessionSettings->DynamicTable->EnableDynamicStoreRead,
            queryContext->Logger);

        return CreateStorageDistributor(context, std::move(tables), DB::StorageID{getDatabaseName(), name});
    } catch (const TErrorException& ex) {
        if (ex.Error().FindMatching(NYTree::EErrorCode::ResolveError)) {
            return nullptr;
        }
        throw;
    }
}

DB::StoragePtr TYtDatabaseBase::DoGetDictionary(DB::ContextPtr context, const DB::StorageID& storageId) const {
    auto loadResult = context->getExternalDictionariesLoader().getLoadResult(storageId.getFullTableName());
    if (!loadResult.config) {
        return nullptr;
    }
    return std::make_shared<DB::StorageDictionary>(
        DB::StorageID(getDatabaseName(), loadResult.name),
        loadResult.config->config,
        context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
