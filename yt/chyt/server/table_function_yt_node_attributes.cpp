#include "table_functions.h"

#include "storages_yt_nodes.h"
#include "function_helpers.h"

#include <Parsers/ASTFunction.h>
#include <Storages/IStorage.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TTableFunctionYtNodeAttributes
    : public DB::ITableFunction
{
public:
    static constexpr auto name = "ytNodeAttributes";

    TTableFunctionYtNodeAttributes()
    { }

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

        Paths_.reserve(args.size());

        for (const auto& arg : args) {
            auto path = EvaluateStringExpression(arg, context);
            Paths_.push_back(std::move(path));
        }
    }

    DB::ColumnsDescription getActualTableStructure(DB::ContextPtr context, bool /*isInsertQuery*/) const override
    {
        // It's ok, creating StorageYtNodeAttributes is not expensive.
        auto storage = Execute(context);
        return storage->getInMemoryMetadata().getColumns();
    }

private:
    std::vector<TString> Paths_;

    DB::StoragePtr executeImpl(
        const DB::ASTPtr& /*functionAst*/,
        DB::ContextPtr context,
        const std::string& /*tableName*/,
        DB::ColumnsDescription /*cachedClumns*/,
        bool /*isInsertQuery*/) const override
    {
        return Execute(context);
    }

    DB::StoragePtr Execute(DB::ContextPtr /*context*/) const
    {
        return CreateStorageYtNodeAttributes(Paths_);
    }

    const char* getStorageTypeName() const override
    {
        return "YtNodes";
    }
};

////////////////////////////////////////////////////////////////////////////////

void RegisterTableFunctionYtNodeAttributes()
{
    auto& factory = DB::TableFunctionFactory::instance();

    factory.registerFunction<TTableFunctionYtNodeAttributes>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
