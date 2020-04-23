#include "table_functions.h"

#include "storage_subquery.h"
#include "subquery.h"
#include "query_context.h"
#include "subquery_spec.h"
#include "join_workaround.h"

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
// select * from ytTable('//home/user/table', ...)

class TYtTableTableFunction
    : public ITableFunction
{
public:
    static constexpr auto name = "ytTable";

    TYtTableTableFunction() = default;

    virtual std::string getName() const override
    {
        return name;
    }

    virtual StoragePtr executeImpl(const ASTPtr& functionAst, const Context& context, const std::string& /* tableName */) const override
    {
        const char* err = "Table function 'ytTable' requires at least 1 parameter: name of remote table";

        auto& funcArgs = typeid_cast<ASTFunction &>(*functionAst).children;
        if (funcArgs.size() != 1) {
            throw Exception(err, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        auto& args = typeid_cast<ASTExpressionList &>(*funcArgs.at(0)).children;
        if (args.size() != 1) {
            throw Exception(err, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(args[0], context);

        auto tableName = static_cast<const ASTLiteral &>(*args[0]).value.safeGet<std::string>();

        return context.getTable(context.getCurrentDatabase(), tableName);
    }
};

////////////////////////////////////////////////////////////////////////////////
// select * from ytSubquery(subquerySpec)

class TYtSubqueryTableFunction
    : public ITableFunction
{
public:
    static constexpr auto name = "ytSubquery";

    TYtSubqueryTableFunction() = default;

    virtual std::string getName() const override
    {
        return name;
    }

#ifdef CHYT154_WORKAROUND
    virtual StoragePtr execute(const ASTPtr& functionAst, const Context& context, const std::string& /* tableName */, IAST * queryAst) const override
#else
    virtual StoragePtr executeImpl(const ASTPtr& functionAst, const Context& context, const std::string& /* tableName */) const override
#endif
    {
        const auto& Logger = GetQueryContext(context)->Logger;

        const char* err = "Table function 'ytSubquery' requires 1 parameter: table part to read";

        auto& funcArgs = typeid_cast<ASTFunction &>(*functionAst).children;
        if (funcArgs.size() != 1) {
            throw Exception(err, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        auto& args = typeid_cast<ASTExpressionList &>(*funcArgs.at(0)).children;
        if (args.size() != 1) {
            throw Exception(err, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        args[0] = evaluateConstantExpressionAsLiteral(args[0], context);

        auto base64EncodedSpec = static_cast<const ASTLiteral &>(*args[0]).value.safeGet<std::string>();

        YT_LOG_INFO("Deserializing subquery spec (SpecLength: %v)", base64EncodedSpec.size());

        auto protoSpecString = Base64Decode(base64EncodedSpec);
        NProto::TSubquerySpec protoSpec;
        protoSpec.ParseFromString(protoSpecString);
        auto subquerySpec = NYT::FromProto<TSubquerySpec>(protoSpec);

#ifdef CHYT154_WORKAROUND
        if (queryAst) {
            try {
                ApplyMembershipHint(*queryAst, subquerySpec.MembershipHint, Logger);
            } catch (std::exception& ex) {
                YT_LOG_ERROR(ex, "Error while applying membership hint");
            }
        } else {
            YT_LOG_INFO("Query AST is not available, ignoring membership hint");
        }
#endif

        return Execute(context, std::move(subquerySpec));
    }

private:
    StoragePtr Execute(const Context& context, TSubquerySpec subquerySpec) const
    {
        auto* queryContext = GetQueryContext(context);

        return CreateStorageSubquery(
            queryContext,
            std::move(subquerySpec));
    }
};

////////////////////////////////////////////////////////////////////////////////

void RegisterTableFunctions()
{
    auto& factory = TableFunctionFactory::instance();

    factory.registerFunction<TYtTableTableFunction>();
    factory.registerFunction<TYtSubqueryTableFunction>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
