#include "table_functions_concat.h"

#include "table.h"
#include "query_context.h"
#include "storage_distributor.h"
#include "type_helpers.h"
#include "helpers.h"
#include "query_context.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/object_attribute_cache.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/client/table_client/schema.h>

#include <yt/client/object_client/public.h>

#include <yt/client/ypath/rich.h>

#include <yt/core/ytree/node.h>
#include <yt/core/ytree/convert.h>

#include <yt/core/yson/string.h>

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

namespace NYT::NClickHouseServer {

using namespace DB;
using namespace NYTree;
using namespace NApi;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NYson;
using namespace NYPath;
using namespace NTableClient;

using NYT::ToProto;

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

// TODO(max42): unify with remaining functions.
std::vector<TClickHouseTablePtr> FetchClickHouseTables(TQueryContext* queryContext, const std::vector<NYPath::TRichYPath>& richPaths)
{
    std::vector<TYPath> paths;
    paths.reserve(richPaths.size());
    for (const auto& path: richPaths) {
        paths.emplace_back(path.GetPath());
    }

    auto attributes = WaitFor(queryContext->Bootstrap->GetHost()->GetTableAttributeCache()->Get(paths))
        .ValueOrThrow();

    std::vector<TClickHouseTablePtr> tables;
    std::vector<TError> errors;
    for (int index = 0; index < static_cast<int>(richPaths.size()); ++index) {
        const auto& path = richPaths[index];
        const auto& attrOrError = attributes[index];

        if (!attrOrError.IsOK()) {
            // We intentionally skip missing tables.
            if (!attrOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                errors.emplace_back(attrOrError
                    << TErrorAttribute("path", path));
            }
        } else {
            tables.emplace_back(std::make_shared<TClickHouseTable>(path, ConvertTo<TTableSchema>(attrOrError.Value().at("schema"))));
        }
    }
    if (!errors.empty()) {
        THROW_ERROR_EXCEPTION("Failed to fetch some of the tables")
            << errors;
    }
    return tables;
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
public:
    TConcatenateTablesList()
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
        const Context& context,
        const std::string& /* tableName */) const override
    {
        auto* queryContext = GetQueryContext(context);

        auto& functionNode = typeid_cast<ASTFunction &>(*functionAst);
        auto& arguments = GetAllArguments(functionNode);

        auto tableNames = EvaluateArguments(arguments, context);

        return Execute(tableNames, queryContext);
    }

private:
    std::vector<TRichYPath> EvaluateArguments(
        TArguments& arguments,
        const Context& context) const
    {
        std::vector<TRichYPath> tableNames;
        tableNames.reserve(arguments.size());
        for (auto& argument : arguments) {
            tableNames.push_back(TRichYPath::Parse(ToString(EvaluateIdentifierArgument(argument, context))));
        }
        return tableNames;
    }

    StoragePtr Execute(const std::vector<TRichYPath>& tablePaths, TQueryContext* queryContext) const
    {
        auto tables = FetchClickHouseTables(queryContext, tablePaths);

        return CreateStorageDistributor(std::move(tables));
    }
};

////////////////////////////////////////////////////////////////////////////////

// Abstract base class for functions that
// list directory, filter tables (by regexp, by date range, last k, etc)
// and concatenate them.

class TListFilterAndConcatenateTables
    : public ITableFunction
{
public:
    TListFilterAndConcatenateTables()
    { }

    StoragePtr executeImpl(
        const ASTPtr& functionAst,
        const Context& context,
        const std::string& /* tableName */) const override
    {
        auto* queryContext = GetQueryContext(context);
        const auto& Logger = queryContext->Logger;

        auto& functionNode = typeid_cast<ASTFunction &>(*functionAst);
        auto& arguments = GetAllArguments(functionNode);

        auto directory = TRichYPath(TString(GetDirectoryRequiredArgument(arguments, context)));

        YT_LOG_INFO("Listing directory (Path: %v)", directory);

        TListNodeOptions options;
        options.Attributes = {"type", "path", "dynamic"};
        options.SuppressAccessTracking = true;

        auto items = WaitFor(queryContext->Client()->ListNode(directory.GetPath(), options))
            .ValueOrThrow();
        auto itemList = ConvertTo<IListNodePtr>(items);

        std::vector<TRichYPath> tablePaths;
        for (const auto& child : itemList->GetChildren()) {
            const auto& attributes = child->Attributes();
            if (attributes.Get<NObjectClient::EObjectType>("type") == NObjectClient::EObjectType::Table &&
                attributes.Get<bool>("dynamic") == false)
            {
                auto richPath = TRichYPath(attributes.Get<TYPath>("path"), directory.Attributes());
                tablePaths.emplace_back(richPath);
            }
        }

        YT_LOG_INFO("Tables listed (ItemCount: %v, TableCount: %v)", itemList->GetChildCount(), tablePaths.size());

        std::sort(tablePaths.begin(), tablePaths.end(), [] (const auto& lhs, const auto& rhs) {
            return lhs.GetPath() < rhs.GetPath();
        });

        tablePaths = FilterTables(tablePaths, arguments, context);

        auto tables = FetchClickHouseTables(queryContext, tablePaths);

        return CreateStorage(std::move(tables), context);
    }

protected:
    virtual std::vector<TRichYPath> FilterTables(
        const std::vector<TRichYPath>& tablePaths,
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

        return CreateStorageDistributor(tables);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TConcatenateTablesRange
    : public TListFilterAndConcatenateTables
{
public:
    TConcatenateTablesRange()
        : TListFilterAndConcatenateTables()
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
    std::vector<TRichYPath> FilterTables(
        const std::vector<TRichYPath>& tablePaths,
        TArguments& arguments,
        const Context& context) const override
    {
        const size_t argumentCount = arguments.size();

        if (argumentCount == 1) {
            // All tables in directory
            return tablePaths;
        }

        std::vector<TRichYPath> result;

        if (argumentCount == 2) {
            // [from, ...)
            auto from = TString(EvaluateArgument<std::string>(arguments[1], context));

            std::copy_if(tablePaths.begin(), tablePaths.end(), std::back_inserter(result), [&] (const auto& name) {
                return BaseName(name.GetPath()) >= from;
            });
        } else if (argumentCount == 3) {
            // [from, to] name range
            auto from = TString(EvaluateArgument<std::string>(arguments[1], context));
            auto to = TString(EvaluateArgument<std::string>(arguments[2], context));

            std::copy_if(tablePaths.begin(), tablePaths.end(), std::back_inserter(result), [&] (const auto& name) {
                return BaseName(name.GetPath()) >= from && BaseName(name.GetPath()) <= to;
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
    TConcatenateTablesRegexp()
        : TListFilterAndConcatenateTables()
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
    std::vector<TRichYPath> FilterTables(
        const std::vector<TRichYPath>& tablePaths,
        TArguments& arguments,
        const Context& context) const override
    {
        // 1) directory, 2) regexp
        ValidateNumberOfArguments(arguments, 2);

        const auto regexp = EvaluateArgument<std::string>(arguments[1], context);

        auto matcher = std::make_shared<OptimizedRegularExpression>(std::move(regexp));

        std::vector<TRichYPath> result;

        std::copy_if(tablePaths.begin(), tablePaths.end(), std::back_inserter(result), [&] (const auto& path) {
            return matcher->match(BaseName(path.GetPath()));
        });

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TConcatenateTablesLike
    : public TListFilterAndConcatenateTables
{
public:
    TConcatenateTablesLike()
        : TListFilterAndConcatenateTables()
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
    std::vector<TRichYPath> FilterTables(
        const std::vector<TRichYPath>& tablePaths,
        TArguments& arguments,
        const Context& context) const override
    {
        // 1) directory 2) pattern
        ValidateNumberOfArguments(arguments, 2);

        auto pattern = EvaluateArgument<std::string>(arguments[1], context);

        auto matcher = std::make_shared<Poco::Glob>(pattern);

        std::vector<TRichYPath> result;

        std::copy_if(tablePaths.begin(), tablePaths.end(), std::back_inserter(result), [&] (const auto& path) {
            return matcher->match(BaseName(path.GetPath()));
        });

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

void RegisterConcatenatingTableFunctions()
{
    auto& factory = TableFunctionFactory::instance();

    // TODO(max42): simplify.
#define REGISTER_TABLE_FUNCTION(TFunction) \
    factory.registerFunction( \
        TFunction::GetName(), \
        [=] { return std::make_shared<TFunction>(); } \
        ); \

    REGISTER_TABLE_FUNCTION(TConcatenateTablesList);
    REGISTER_TABLE_FUNCTION(TConcatenateTablesRange);
    REGISTER_TABLE_FUNCTION(TConcatenateTablesRegexp);
    REGISTER_TABLE_FUNCTION(TConcatenateTablesLike);

#undef REGISTER_TABLE_FUNCTION

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
