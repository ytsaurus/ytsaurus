#include "yt_database.h"

#include "host.h"
#include "query_context.h"
#include "storage_distributor.h"
#include "table_traverser.h"
#include "table.h"
#include "yt_database_base.h"

#include <Interpreters/Context.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/IStorage.h>

#include <memory>
#include <vector>

namespace NYT::NClickHouseServer {

using namespace NYPath;
using namespace NStatisticPath;

////////////////////////////////////////////////////////////////////////////////

namespace {

std::vector<TRichYPath> ConvertToPaths(const std::vector<std::string>& tableNames)
{
    std::vector<TRichYPath> tablePaths;
    tablePaths.reserve(tableNames.size());
    for (const std::string& tableName : tableNames) {
        tablePaths.push_back(TRichYPath::Parse(tableName.data()));
    }
    return tablePaths;
}

} // namespace

class TYtDatabase
    : public TYtDatabaseBase
{
public:
    TYtDatabase()
        : TYtDatabaseBase("YT")
    { }

    DB::DatabaseTablesIteratorPtr getTablesIterator(
        DB::ContextPtr context,
        const FilterByNameFunction & filterByTableName,
        bool /*skipNotLoaded*/) const override
    {
        auto* queryContext = GetQueryContext(context);
        auto timerGuard = queryContext->CreateStatisticsTimerGuard("/yt_database/get_tables_iterator"_SP);

        TTableTraverser traverser(
            queryContext->Client(),
            *queryContext->Settings->CypressReadOptions,
            queryContext->Host->GetConfig()->ShowTables->Roots,
            filterByTableName);

        std::vector<std::string> tableNames = traverser.GetTables();

        auto tables = FetchTables(
            queryContext,
            ConvertToPaths(tableNames),
            /*skipUnsuitableNodes*/ true,
            queryContext->Settings->DynamicTable->EnableDynamicStoreRead,
            queryContext->Logger);

        DB::Tables resultingsTargets;

        for (int index = 0; index < std::ssize(tableNames); ++index) {
            auto storage = CreateStorageDistributor(context,
                {std::move(tables[index])},
                DB::StorageID{"YT", tableNames[index]});

            resultingsTargets.emplace(std::move(tableNames[index]), std::move(storage));
        }

        return std::make_unique<DB::DatabaseTablesSnapshotIterator>(std::move(resultingsTargets), "YT");
    }

    DB::ASTPtr getCreateDatabaseQuery() const override
    {
        auto query = Format("CREATE DATABASE %v ENGINE = %v", getDatabaseName(), getEngineName());
        DB::ParserCreateQuery parser;
        return DB::parseQuery(parser,
            query.data(),
            query.data() + query.size(),
            "",
            0 /*maxQuerySize*/,
            0 /*maxQueryDepth*/,
            0 /*max_parser_backtracks*/);
    }

    String getEngineName() const override
    {
        return "YT";
    }

    String getTableDataPath(const String& name) const override
    {
        return name;
    }

    String getTableDataPath(const DB::ASTCreateQuery& query) const override
    {
        return getTableDataPath(query.getTable());
    }
};

////////////////////////////////////////////////////////////////////////////////

DB::DatabasePtr CreateYTDatabase()
{
    return std::make_shared<TYtDatabase>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
