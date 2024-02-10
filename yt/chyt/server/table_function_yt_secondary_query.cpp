#include "table_functions.h"

#include "storage_subquery.h"
#include "subquery.h"
#include "query_context.h"
#include "subquery_spec.h"
#include "config.h"

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageMemory.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <string>

namespace NYT::NClickHouseServer {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////
// select * from ytSubquery(subquerySpec)

// TODO(dakovalkov): 'Subuqery' is a misleading term. Rename it.
class TYtSubqueryTableFunction
    : public ITableFunction
{
public:
    static constexpr auto name = "ytSubquery";

    TYtSubqueryTableFunction() = default;

    std::string getName() const override
    {
        return name;
    }

    const char* getStorageTypeName() const override
    {
        return "YT";
    }

    void parseArguments(const ASTPtr& functionAst, ContextPtr context) override
    {
        auto* queryContext = GetQueryContext(context);
        const auto& Logger = queryContext->Logger;

        constexpr auto err = "Table function 'ytSubquery' requires 1 parameter: table part to read";

        auto& funcArgs = typeid_cast<ASTFunction &>(*functionAst).children;
        if (funcArgs.size() != 1) {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, err);
        }

        auto& args = typeid_cast<ASTExpressionList &>(*funcArgs.at(0)).children;
        if (args.size() != 1) {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, err);
        }

        auto [argValue, _] = evaluateConstantExpression(args[0], context->getQueryContext());

        auto protoSpecString = TString(argValue.safeGet<std::string>());

        YT_LOG_INFO("Deserializing subquery spec (SpecLength: %v)", protoSpecString.size());

        NProto::TSubquerySpec protoSpec;
        Y_PROTOBUF_SUPPRESS_NODISCARD protoSpec.ParseFromString(protoSpecString);
        subquerySpec = NYT::FromProto<TSubquerySpec>(protoSpec);
    }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool /*isInsertQuery*/) const override
    {
        auto* queryContext = GetQueryContext(context);
        return DB::ColumnsDescription(ToNamesAndTypesList(*subquerySpec.ReadSchema, queryContext->Settings->Composite));
    }

    StoragePtr executeImpl(
        const ASTPtr& /*functionAst*/,
        ContextPtr context,
        const std::string& /*tableName*/,
        ColumnsDescription /*cachedColumns*/,
        bool /*isInsertQuery*/) const override
    {
        return Execute(context, std::move(subquerySpec));
    }

private:
    StoragePtr Execute(ContextPtr context, TSubquerySpec subquerySpec) const
    {
        return CreateStorageSubquery(
            GetQueryContext(context),
            std::move(subquerySpec));
    }

    TSubquerySpec subquerySpec;
};

////////////////////////////////////////////////////////////////////////////////

void RegisterTableFunctionYtSecondaryQuery()
{
    auto& factory = TableFunctionFactory::instance();

    factory.registerFunction<TYtSubqueryTableFunction>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
