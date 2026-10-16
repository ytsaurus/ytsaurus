#include "table_functions.h"

#include "storages_yt_nodes.h"
#include "function_helpers.h"

#include <DataTypes/IDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/IStorage.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/evaluateConstantExpression.h>

namespace DB::ErrorCodes {

////////////////////////////////////////////////////////////////////////////////

extern const int BAD_ARGUMENTS;

////////////////////////////////////////////////////////////////////////////////

} // namespace DB::ErrorCodes

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TTableFunctionYtListLogTables
    : public DB::ITableFunction
{
public:
    static constexpr auto name = "ytListLogTables";

    TTableFunctionYtListLogTables()
    { }

    std::string getName() const override
    {
        return name;
    }

    void parseArguments(const DB::ASTPtr& functionAst, DB::ContextPtr context) override
    {
        const auto& function = functionAst->as<DB::ASTFunction&>();

        if (!function.arguments) {
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "Table function {} must have arguments",
                getName());
        }

        DB::ASTs& args = function.arguments->children;

        if (args.size() < 1 || args.size() > 3) {
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "Table function {} must have 1, 2 or 3 arguments",
                getName());
        }

        LogPath_ = EvaluateStringExpression(args[0], context);

        if (args.size() >= 2) {
            Options_.From = ParseDateTimeArg(args[1], context);
        }
        if (args.size() >= 3) {
            Options_.To = ParseDateTimeArg(args[2], context);
        }
    }

    DB::ColumnsDescription getActualTableStructure(DB::ContextPtr context, bool /*isInsertQuery*/) const override
    {
        // It's ok, creating StorageYtLogTables is not expensive.
        auto storage = Execute(context);
        return storage->getInMemoryMetadata().getColumns();
    }

private:
    TString LogPath_;
    TStorageYtLogTablesOptions Options_;

    DB::StoragePtr executeImpl(
        const DB::ASTPtr& /*functionAst*/,
        DB::ContextPtr context,
        const std::string& /*tableName*/,
        DB::ColumnsDescription /*cachedColumns*/,
        bool /*isInsertQuery*/) const override
    {
        return Execute(context);
    }

    DB::StoragePtr Execute(DB::ContextPtr /*context*/) const
    {
        return CreateStorageYtLogTables(LogPath_, Options_);
    }

    const char* getStorageTypeName() const override
    {
        return "YtNodes";
    }
};

////////////////////////////////////////////////////////////////////////////////

void RegisterTableFunctionYtListLogTables()
{
    auto& factory = DB::TableFunctionFactory::instance();

    factory.registerFunction<TTableFunctionYtListLogTables>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
