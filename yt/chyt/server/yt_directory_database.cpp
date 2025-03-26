#include "yt_directory_database.h"

#include "host.h"
#include "query_context.h"
#include "storage_distributor.h"
#include "table.h"
#include "yt_database_base.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ypath/helpers.h>
#include <yt/yt/core/ytree/public.h>

#include <Interpreters/Context.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/IStorage.h>

#include <memory>
#include <vector>

namespace NYT::NClickHouseServer {

using namespace NYPath;
using namespace NYTree;
using namespace NApi;
using namespace NStatisticPath;

////////////////////////////////////////////////////////////////////////////////

class TYtDirectoryDatabase
    : public TYtDatabaseBase
{
public:
    TYtDirectoryDatabase(String databaseName, TYPath root)
        : TYtDatabaseBase(std::move(databaseName))
        , Root_(std::move(root))
    { }

    DB::DatabaseTablesIteratorPtr getTablesIterator(
        DB::ContextPtr context,
        const FilterByNameFunction& filterByTableName,
        bool /*skipNotLoaded*/) const override
    {
        auto* queryContext = GetQueryContext(context);

        auto timerGuard = queryContext->CreateStatisticsTimerGuard(
            SlashedStatisticPath(
                Format("/%v_database/get_tables_iterator", to_lower(TString(getDatabaseName())))).ValueOrThrow());

        DB::Tables resultingTargets;

        for (TTablePtr& table : FetchTablesFromRootDirectory(queryContext, filterByTableName)) {
            auto name = ParseNameFromPath(table->GetPath());
            auto storage = CreateStorageDistributor(context, {std::move(table)}, DB::StorageID{getDatabaseName(), name});

            resultingTargets.emplace(std::move(name), std::move(storage));
        }

        return std::make_unique<DB::DatabaseTablesSnapshotIterator>(std::move(resultingTargets), getDatabaseName());
    }

    DB::ASTPtr getCreateDatabaseQuery() const override
    {
        // For example, "CREATE DATABASE my_db_name ENGINE = YTDirectory('//home/dev/dakovalkov/my_dir')".
        auto query = Format("CREATE DATABASE %v ENGINE = %v('%v')", getDatabaseName(), getEngineName(), Root_);
        DB::ParserCreateQuery parser;
        return DB::parseQuery(parser,
            query.data(),
            query.data() + query.size(),
            "",
            0 /*maxQuerySize*/,
            0 /*maxQueryDepth*/,
            0 /*max_parser_backtracks=*/);
    }

    String getEngineName() const override
    {
        return "YtDirectory";
    }

    String getTableDataPath(const String& name) const override
    {
        return YPathJoin(Root_, name);
    }

    String getTableDataPath(const DB::ASTCreateQuery& query) const override
    {
        return getTableDataPath(query.getTable());
    }

private:
    TYPath Root_;

    std::vector<TTablePtr> FetchTablesFromRootDirectory(TQueryContext* queryContext, const FilterByNameFunction& filterByTableName) const
    {
        auto items = NConcurrency::WaitFor(queryContext->Client()->ListNode(Root_, BuildListTablesOptions(queryContext)))
            .ValueOrThrow();

        auto itemList = ConvertTo<IListNodePtr>(items)->GetChildren();

        std::vector<TRichYPath> itemPaths;
        itemPaths.reserve(itemList.size());

        for (const auto& child : itemList) {
            TYPath path = child->Attributes().Get<TYPath>("path");
            if (!filterByTableName || filterByTableName(ParseNameFromPath(path))) {
                itemPaths.push_back(std::move(path));
            }
        }

        return FetchTables(
            queryContext,
            std::move(itemPaths),
            /*skipUnsuitableNodes*/ true,
            queryContext->Settings->DynamicTable->EnableDynamicStoreRead,
            queryContext->Logger);
    }

    String ParseNameFromPath(const TYPath& path) const
    {
        // Remove root prefix (for example //tmp/root/my_table -> my_table).
        return path.substr(Root_.length() + 1);
    }

    TListNodeOptions BuildListTablesOptions(TQueryContext* queryContext) const
    {
        TListNodeOptions options;
        static_cast<TMasterReadOptions&>(options) = *queryContext->Settings->CypressReadOptions;
        options.Attributes = {
            "path",
        };
        options.SuppressAccessTracking = true;
        options.SuppressExpirationTimeoutRenewal = true;
        if (queryContext->Settings->Execution->TableReadLockMode == ETableReadLockMode::Sync) {
            options.TransactionId = queryContext->ReadTransactionId;
        }

        return options;
    }
};

////////////////////////////////////////////////////////////////////////////////

DB::DatabasePtr CreateDirectoryDatabase(String databaseName, TYPath root)
{
    return std::make_shared<TYtDirectoryDatabase>(std::move(databaseName), std::move(root));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
