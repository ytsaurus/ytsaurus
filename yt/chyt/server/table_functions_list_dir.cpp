#include "table_functions.h"

#include "storages_yt_nodes.h"
#include "function_helpers.h"

#include <Parsers/ASTFunction.h>
#include <Storages/IStorage.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

template <typename Description>
class TTableFunctionYtListDir
    : public DB::ITableFunction
{
public:
    static constexpr auto name = Description::Name;

    TTableFunctionYtListDir()
    {
        Options_.TablesOnly = Description::TablesOnly;
        Options_.ResolveLinks = Description::ResolveLinks;
    }

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

        DirPath_ = EvaluateStringExpression(args[0], context);

        if (args.size() >= 2) {
            Options_.From = EvaluateStringExpression(args[1], context);
        }
        if (args.size() >= 3) {
            Options_.To = EvaluateStringExpression(args[2], context);
        }
    }

    DB::ColumnsDescription getActualTableStructure(
        DB::ContextPtr context,
        bool /*isInsertQuery*/) const override
    {
        // It's ok, creating StorageYtDir is not expensive.
        auto storage = Execute(context);
        return storage->getInMemoryMetadata().getColumns();
    }

private:
    TString DirPath_;
    TStorageYtDirOptions Options_;

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
        return CreateStorageYtDir(DirPath_, Options_);
    }

    const char* getStorageTypeName() const override
    {
        return "YtNodes";
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TDescriptionYtListNodes
{
    static constexpr auto Name = "ytListNodes";
    static constexpr bool TablesOnly = false;
    static constexpr bool ResolveLinks = false;
};

struct TDescriptionYtListTables
{
    static constexpr auto Name = "ytListTables";
    static constexpr bool TablesOnly = true;
    static constexpr bool ResolveLinks = false;
};

struct TDescriptionYtListNodesL
{
    static constexpr auto Name = "ytListNodesL";
    static constexpr bool TablesOnly = false;
    static constexpr bool ResolveLinks = true;
};

struct TDescriptionYtListTablesL
{
    static constexpr auto Name = "ytListTablesL";
    static constexpr bool TablesOnly = true;
    static constexpr bool ResolveLinks = true;
};

////////////////////////////////////////////////////////////////////////////////

void RegisterTableFunctionsListDir()
{
    auto& factory = DB::TableFunctionFactory::instance();

    factory.registerFunction<TTableFunctionYtListDir<TDescriptionYtListNodes>>();
    factory.registerFunction<TTableFunctionYtListDir<TDescriptionYtListTables>>();
    factory.registerFunction<TTableFunctionYtListDir<TDescriptionYtListNodesL>>();
    factory.registerFunction<TTableFunctionYtListDir<TDescriptionYtListTablesL>>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
