#include "yt_database_base.h"

#include "storage_distributor.h"
#include "helpers.h"
#include "query_context.h"
#include "query_registry.h"
#include "table.h"
#include "host.h"
#include "table_traverser.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/convert.h>

#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

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
    const DB::ContextPtr /*context*/,
    const std::string& /*name*/,
    const DB::StoragePtr& table,
    const DB::ASTPtr& /*query*/)
{
    if (table->getName() != "StorageDistributor") {
        THROW_ERROR_EXCEPTION("Table engine %Qv may not be stored in YT database: only YtTable engine is supported",
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
    auto timerGuard = queryContext->CreateStatisticsTimerGuard(
        SlashedStatisticPath(
            Format("/%v_database/drop_table", to_lower(TString(getDatabaseName())))).ValueOrThrow());

    TYPath path = getTableDataPath(name);

    WaitFor(queryContext->Client()->RemoveNode(path))
        .ThrowOnError();

    InvalidateCache(queryContext, {path});
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
    } else {
        WaitFor(client->MoveNode(srcPath, dstPath))
            .ThrowOnError();
    }

    InvalidateCache(queryContext, {srcPath, dstPath});
}

DB::ASTPtr TYtDatabaseBase::getCreateTableQueryImpl(const String& name, DB::ContextPtr context, bool throwOnError) const
{
    auto* queryContext = GetQueryContext(context);
    auto path = TRichYPath::Parse(getTableDataPath(name));

    TGetNodeOptions options;
    static_cast<TMasterReadOptions&>(options) = *queryContext->Settings->CypressReadOptions;
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
    TYPath path = getTableDataPath(name);
    // Definitely not a YT table. Don't even try to parse it.
    if (!path.StartsWith("//") && !path.StartsWith("<")) {
        return nullptr;
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

    try {
        auto tables = FetchTables(
            queryContext,
            {TRichYPath::Parse(path)},
            /*skipUnsuitableNodes*/ false,
            queryContext->Settings->DynamicTable->EnableDynamicStoreRead,
            queryContext->Logger);

        return CreateStorageDistributor(context, std::move(tables), DB::StorageID{getDatabaseName(), name});
    } catch (const TErrorException& ex) {
        if (ex.Error().FindMatching(NYTree::EErrorCode::ResolveError)) {
            return nullptr;
        }
        throw;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
