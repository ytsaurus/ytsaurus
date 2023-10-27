#include "table_functions.h"

#include "config.h"
#include "function_helpers.h"
#include "helpers.h"
#include "query_context.h"
#include "storage_distributor.h"
#include "table.h"

#include <yt/yt/client/ypath/rich.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Storages/IStorage.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>

namespace NYT::NClickHouseServer {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

namespace {

std::vector<TString> ExtractPathsFromSubquery(
    const DB::ASTSubquery& subquery,
    DB::ContextPtr context,
    size_t rowLimit)
{
    std::vector<TString> result;

    auto processBlock = [&result] (const DB::Block& block) {
        if (block.columns() != 1) {
            THROW_ERROR_EXCEPTION("Unexpected number of columns while executing paths subquery");
        }

        DB::WhichDataType type = DB::removeNullable(block.getByPosition(0).type);
        if (!type.isString()) {
            THROW_ERROR_EXCEPTION("Unexpected type of column while executing paths subquery");
        }

        auto column = block.getByPosition(0).column;

        if (result.empty()) {
            result.reserve(block.rows());
        }

        for (size_t index = 0; index < block.rows(); ++index) {
            const auto& value = (*column)[index];
            if (!value.isNull()) {
                result.emplace_back(value.safeGet<std::string>());
            }
        }
    };

    auto subquerySelect = subquery.children.at(0);

    // See getSubqueryContext in Interpreters/InterpreterSelectQuery.cpp.
    auto subqueryContext = DB::Context::createCopy(context);
    DB::Settings subquerySettings = context->getSettings();
    subquerySettings.max_result_rows = 0;
    subquerySettings.max_result_bytes = 0;
    subquerySettings.extremes = false;
    subqueryContext->setSettings(std::move(subquerySettings));

    auto interpreter = DB::InterpreterSelectWithUnionQuery(
        subquerySelect,
        subqueryContext,
        DB::SelectQueryOptions());

    // Process empty block to check column structure before execution.
    processBlock(interpreter.getSampleBlock());

    auto io = interpreter.execute();
    DB::PullingPipelineExecutor executor(io.pipeline);

    DB::Block block;
    while (executor.pull(block) && result.size() < rowLimit) {
        processBlock(block);
    }

    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TTableFunctionYtTables
    : public DB::ITableFunction
{
public:
    TTableFunctionYtTables()
    { }

    static constexpr auto name = "ytTables";

    std::string getName() const override
    {
        return name;
    }

    void parseArguments(const DB::ASTPtr& functionAst, DB::ContextPtr context) override
    {
        const auto& function = functionAst->as<DB::ASTFunction&>();

        if (!function.arguments || function.arguments->children.empty()) {
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "Table function {} must have arguments",
                getName());
        }

        DB::ASTs& args = function.arguments->children;

        auto* queryContext = GetQueryContext(context);

        size_t maxPaths = queryContext->Settings->ConcatTables->MaxTables;

        for (const auto& arg : args) {
            if (auto* subqueryArg = arg->as<DB::ASTSubquery>()) {
                auto paths = ExtractPathsFromSubquery(*subqueryArg, context, maxPaths + 1);
                for (auto& path : paths) {
                    TablePaths_.emplace_back(std::move(path));
                }
            } else if (auto* functionArg = arg->as<DB::ASTFunction>(); functionArg && AllowedTableFunctions.contains(functionArg->name)) {
                // Wrap it with 'SELECT $path FROM <tableFunction>' and execute.
                auto functionTableExpression = std::make_shared<DB::ASTTableExpression>();
                functionTableExpression->table_function = arg;
                std::vector<TString> columnNames = {"$path"};

                auto subqueryTableExpression = WrapTableExpressionWithSubquery(functionTableExpression, columnNames);
                auto subquery = subqueryTableExpression->as<DB::ASTTableExpression>()->subquery->as<DB::ASTSubquery>();

                auto paths = ExtractPathsFromSubquery(*subquery, context, maxPaths + 1);
                for (auto& path : paths) {
                    TablePaths_.emplace_back(std::move(path));
                }
            } else {
                auto path = EvaluateStringExpression(arg, context);
                TablePaths_.push_back(path);
            }

            if (TablePaths_.size() > maxPaths) {
                THROW_ERROR_EXCEPTION("Too many tables to read in ytTables (MaxTables: %v)", maxPaths);
            }
        }
    }

    DB::ColumnsDescription getActualTableStructure(DB::ContextPtr context, bool /*isInsertQuery*/) const override
    {
        if (!Storage_) {
            Storage_ = Execute(context);
        }
        return Storage_->getInMemoryMetadata().getColumns();
    }

private:
    std::vector<TRichYPath> TablePaths_;
    // 'Execute' is heavy, save storage after execution to avoid calling it twice.
    mutable DB::StoragePtr Storage_;

    static const inline THashSet<TString> AllowedTableFunctions {
        "ytListNodes",
        "ytListNodesL",
        "ytListTables",
        "ytListTablesL",
        "ytListLogTables",
    };

    DB::StoragePtr executeImpl(
        const DB::ASTPtr& /*functionAst*/,
        DB::ContextPtr context,
        const std::string& /*tableName*/,
        DB::ColumnsDescription /*cachedColumns*/,
        bool /*isInsertQuery*/) const override
    {
        if (!Storage_) {
            Storage_ = Execute(context);
        }
        return Storage_;
    }

    DB::StoragePtr Execute(DB::ContextPtr context) const
    {
        auto* queryContext = GetQueryContext(context);

        auto tables = FetchTables(
            queryContext,
            std::move(TablePaths_),
            /*skipUnsuitableNodes*/ false,
            queryContext->Settings->DynamicTable->EnableDynamicStoreRead,
            queryContext->Logger);

        return CreateStorageDistributor(context, std::move(tables));
    }

    const char* getStorageTypeName() const override
    {
        return "YT";
    }
};

////////////////////////////////////////////////////////////////////////////////

void RegisterTableFunctionYtTables()
{
    auto& factory = DB::TableFunctionFactory::instance();

    factory.registerFunction<TTableFunctionYtTables>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
