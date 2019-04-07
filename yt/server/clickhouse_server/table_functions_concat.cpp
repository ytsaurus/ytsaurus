#include "table_functions_concat.h"

#include "query_context.h"
#include "format_helpers.h"
#include "storage_concat.h"
#include "type_helpers.h"
#include "helpers.h"
#include "table_partition.h"
#include "query_context.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/client/object_client/public.h>

#include <yt/core/ytree/node.h>
#include <yt/core/ytree/convert.h>

#include <Common/Exception.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageNull.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <Poco/Glob.h>
#include <Poco/Logger.h>

#include <common/logger_useful.h>

namespace NYT::NClickHouseServer {

using namespace DB;
using namespace NYTree;
using namespace NApi;

namespace {

////////////////////////////////////////////////////////////////////////////////

// TODO: move to function_Helpers.h

void VerifyNonParametric(const ASTFunction& functionNode)
{
    if (functionNode.children.size() != 1) {
        throw Exception("Table function " + functionNode.name + " does not support parameters",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }
}

using TArguments = std::vector<ASTPtr>;

TArguments& GetAllArguments(const ASTFunction& functionNode)
{
    VerifyNonParametric(functionNode);

    auto& argumentListNode = typeid_cast<ASTExpressionList &>(*functionNode.children.at(0));
    return argumentListNode.children;
}

void ValidateNumberOfArguments(const TArguments& arguments, const size_t numArgumentsExpected)
{
    if (arguments.size() != numArgumentsExpected) {
        throw Exception(
            "Number of arguments mismatch: "
            "expected " + std::to_string(numArgumentsExpected) + ", provided: " + std::to_string(arguments.size()),
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }
}

// Arguments should be evaluated in-place
std::string EvaluateIdentifierArgument(ASTPtr& argument, const Context& context)
{
    argument = evaluateConstantExpressionOrIdentifierAsLiteral(argument, context);
    return static_cast<const ASTLiteral &>(*argument).value.safeGet<std::string>();
}

template <typename T>
T EvaluateArgument(ASTPtr& argument, const Context& context)
{
    argument = evaluateConstantExpressionAsLiteral(argument, context);
    return static_cast<const ASTLiteral &>(*argument).value.safeGet<T>();
}

std::vector<TClickHouseTablePtr> FetchClickHouseTables(TQueryContext* queryContext, const std::vector<TYPath>& paths)
{
    std::vector<TFuture<TClickHouseTablePtr>> asyncResults;
    for (auto& path : paths) {
        asyncResults.emplace_back(BIND(FetchClickHouseTable, queryContext->Client(), path, queryContext->Logger)
            .AsyncVia(queryContext->Bootstrap->GetWorkerInvoker())
            .Run());
    }
    return WaitFor(Combine(asyncResults))
        .ValueOrThrow();
}

// TODO(max42): move to core.
TString BaseName(const TYPath& path) {
    return TString(path.begin() + path.rfind('/') + 1, path.end());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

// select * from concatYtTables(`A`, `B`, ..., `Z`)

class TConcatenateTablesList
    : public ITableFunction
{
private:
    IExecutionClusterPtr Cluster;
    bool DropPrimaryKey;

public:
    TConcatenateTablesList(IExecutionClusterPtr cluster, bool dropPrimaryKey)
        : Cluster(std::move(cluster))
        , DropPrimaryKey(dropPrimaryKey)
    {}

    static std::string GetName()
    {
        return "concatYtTables";
    }

    std::string getName() const override
    {
        return GetName();
    }

    StoragePtr executeImpl(
        const ASTPtr& functionAst,
        const Context& context) const override
    {
        auto* queryContext = GetQueryContext(context);

        auto& functionNode = typeid_cast<ASTFunction &>(*functionAst);
        auto& arguments = GetAllArguments(functionNode);

        auto tableNames = EvaluateArguments(arguments, context);

        return Execute(tableNames, queryContext);
    }

private:
    std::vector<TString> EvaluateArguments(
        TArguments& arguments,
        const Context& context) const
    {
        std::vector<TString> tableNames;
        tableNames.reserve(arguments.size());
        for (auto& argument : arguments) {
            tableNames.push_back(ToString(EvaluateIdentifierArgument(argument, context)));
        }
        return tableNames;
    }

    StoragePtr Execute(const std::vector<TYPath>& tablePaths, TQueryContext* queryContext) const
    {
        auto tables = FetchClickHouseTables(queryContext, tablePaths);

        return CreateStorageConcat(
            std::move(tables),
            Cluster,
            DropPrimaryKey);
    }
};

////////////////////////////////////////////////////////////////////////////////

// Abstract base class for functions that
// list directory, filter tables (by regexp, by date range, last k, etc)
// and concatenate them.

class TListFilterAndConcatenateTables
    : public ITableFunction
{
private:
    IExecutionClusterPtr Cluster;

    Poco::Logger* Logger;

    bool DropPrimaryKey;

public:
    TListFilterAndConcatenateTables(
        IExecutionClusterPtr cluster,
        Poco::Logger* logger,
        bool dropPrimaryKey)
        : Cluster(std::move(cluster))
        , Logger(logger)
        , DropPrimaryKey(dropPrimaryKey)
    { }

    virtual StoragePtr executeImpl(const ASTPtr& functionAst, const Context& context) const override
    {
        auto* queryContext = GetQueryContext(context);
        const auto& Logger = queryContext->Logger;

        auto& functionNode = typeid_cast<ASTFunction &>(*functionAst);
        auto& arguments = GetAllArguments(functionNode);

        auto directory = TString(GetDirectoryRequiredArgument(arguments, context));

        YT_LOG_INFO("Listing directory (Path: %v)", directory);

        TListNodeOptions options;
        options.Attributes = {"type", "path"};
        options.SuppressAccessTracking = true;

        auto items = WaitFor(queryContext->Client()->ListNode(directory, options))
            .ValueOrThrow();
        auto itemList = ConvertTo<IListNodePtr>(items);

        std::vector<TYPath> tablePaths;
        for (const auto& child : itemList->GetChildren()) {
            const auto& attributes = child->Attributes();
            if (attributes.Get<NObjectClient::EObjectType>("type") == NObjectClient::EObjectType::Table) {
                tablePaths.emplace_back(attributes.Get<TYPath>("path"));
            }
        }

        YT_LOG_INFO("Tables listed (ItemCount: %v, TableCount: %v)", itemList->GetChildCount(), tablePaths.size());

        std::sort(tablePaths.begin(), tablePaths.end());

        tablePaths = FilterTables(tablePaths, arguments, context);

        auto tables = FetchClickHouseTables(queryContext, tablePaths);

        return CreateStorage(std::move(tables), context);
    }

protected:
    Poco::Logger* GetLogger() const
    {
        return Logger;
    }

    virtual std::vector<TYPath> FilterTables(
        const std::vector<TYPath>& tablePaths,
        TArguments& arguments,
        const Context& context) const = 0;

private:
    std::string GetDirectoryRequiredArgument(
        TArguments& arguments,
        const Context& context) const
    {
        if (arguments.empty()) {
            throw Exception(
                "Table function " + getName() + " expected at least one argument: directory path",
                ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION);
        }

        return EvaluateIdentifierArgument(arguments[0], context);
    }

    StoragePtr CreateStorage(
        const std::vector<TClickHouseTablePtr>& tables,
        const Context& /* context */) const
    {
        if (tables.empty()) {
            throw Exception("No tables found by " + getName(), ErrorCodes::CANNOT_SELECT);
        }

        return CreateStorageConcat(tables, Cluster, DropPrimaryKey);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TConcatenateTablesRange
    : public TListFilterAndConcatenateTables
{
public:
    TConcatenateTablesRange(
        IExecutionClusterPtr cluster,
        bool dropPrimaryKey)
        : TListFilterAndConcatenateTables(
            std::move(cluster),
            &Poco::Logger::get("ConcatYtTablesRange"),
            dropPrimaryKey)
    { }

    static std::string GetName()
    {
        return "concatYtTablesRange";
    }

    std::string getName() const override
    {
        return GetName();
    }

private:
    std::vector<TYPath> FilterTables(
        const std::vector<TYPath>& tablePaths,
        TArguments& arguments,
        const Context& context) const override
    {
        const size_t argumentCount = arguments.size();

        if (argumentCount == 1) {
            // All tables in directory
            return tablePaths;
        }

        std::vector<TYPath> result;

        if (argumentCount == 2) {
            // [from, ...)
            auto from = TString(EvaluateArgument<std::string>(arguments[1], context));

            std::copy_if(tablePaths.begin(), tablePaths.end(), std::back_inserter(result), [&] (const auto& name) {
                return BaseName(name) >= from;
            });
        } else if (argumentCount == 3) {
            // [from, to] name range
            auto from = TString(EvaluateArgument<std::string>(arguments[1], context));
            auto to = TString(EvaluateArgument<std::string>(arguments[2], context));

            std::copy_if(tablePaths.begin(), tablePaths.end(), std::back_inserter(result), [&] (const auto& name) {
                return BaseName(name) >= from && BaseName(name) <= to;
            });
        } else {
            throw Exception(
                "Too may arguments: "
                "expected 1, 2 or 3, provided: " + std::to_string(arguments.size()),
                ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);
        }

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TConcatenateTablesRegexp
    : public TListFilterAndConcatenateTables
{
public:
    TConcatenateTablesRegexp(IExecutionClusterPtr cluster, bool dropPrimaryKey)
        : TListFilterAndConcatenateTables(
            std::move(cluster),
            &Poco::Logger::get("ConcatYtTablesRegexp"),
            dropPrimaryKey)
    {}

    static std::string GetName()
    {
        return "concatYtTablesRegexp";
    }

    virtual std::string getName() const override
    {
        return GetName();
    }

private:
    std::vector<TYPath> FilterTables(
        const std::vector<TYPath>& tablePaths,
        TArguments& arguments,
        const Context& context) const override
    {
        // 1) directory, 2) regexp
        ValidateNumberOfArguments(arguments, 2);

        const auto regexp = EvaluateArgument<std::string>(arguments[1], context);

        auto matcher = std::make_shared<OptimizedRegularExpression>(std::move(regexp));

        std::vector<TYPath> result;

        std::copy_if(tablePaths.begin(), tablePaths.end(), std::back_inserter(result), [&] (const auto& path) {
            return matcher->match(BaseName(path));
        });

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TConcatenateTablesLike
    : public TListFilterAndConcatenateTables
{
public:
    TConcatenateTablesLike(
        IExecutionClusterPtr cluster,
        bool dropPrimaryKey)
        : TListFilterAndConcatenateTables(
            std::move(cluster),
            &Poco::Logger::get("ConcatYtTablesLike"),
            dropPrimaryKey)
    {}

    static std::string GetName()
    {
        return "concatYtTablesLike";
    }

    std::string getName() const override
    {
        return GetName();
    }

private:
    std::vector<TYPath> FilterTables(
        const std::vector<TYPath>& tablePaths,
        TArguments& arguments,
        const Context& context) const override
    {
        // 1) directory 2) pattern
        ValidateNumberOfArguments(arguments, 2);

        auto pattern = EvaluateArgument<std::string>(arguments[1], context);

        auto matcher = std::make_shared<Poco::Glob>(pattern);

        std::vector<TString> result;

        std::copy_if(tablePaths.begin(), tablePaths.end(), std::back_inserter(result), [&] (const auto& path) {
            return matcher->match(BaseName(path));
        });

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

void RegisterConcatenatingTableFunctions(IExecutionClusterPtr cluster)
{
    auto& factory = TableFunctionFactory::instance();

#define REGISTER_TABLE_FUNCTION(TFunction) \
    factory.registerFunction( \
        TFunction::GetName(), \
        [=] { return std::make_shared<TFunction>(cluster, false); } \
        ); \
    factory.registerFunction( \
        TFunction::GetName() + "DropPrimaryKey", \
        [=] { return std::make_shared<TFunction>(cluster, true); } \
        );

    REGISTER_TABLE_FUNCTION(TConcatenateTablesList);
    REGISTER_TABLE_FUNCTION(TConcatenateTablesRange);
    REGISTER_TABLE_FUNCTION(TConcatenateTablesRegexp);
    REGISTER_TABLE_FUNCTION(TConcatenateTablesLike);

#undef REGISTER_TABLE_FUNCTION

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
