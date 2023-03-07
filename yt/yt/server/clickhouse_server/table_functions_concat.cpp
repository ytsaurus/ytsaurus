#include "table_functions_concat.h"

#include "query_context.h"
#include "storage_distributor.h"
#include "table.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/client/table_client/schema.h>

#include <yt/client/ypath/rich.h>

#include <yt/core/ytree/convert.h>

#include <yt/core/yson/string.h>

#include <Common/Exception.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/typeid_cast.h>
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
using namespace NConcurrency;

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

// TODO(max42): move to core.
TString BaseName(const TYPath& path)
{
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
    { }

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

        auto tablePaths = EvaluateArguments(arguments, context);

        auto tables = FetchTables(
            queryContext->Client(),
            queryContext->Host,
            std::move(tablePaths),
            /* skipUnsuitableNodes */ false,
            queryContext->Logger);

        return CreateStorageDistributor(queryContext, std::move(tables));
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
        options.Attributes = {
            "path",
        };
        options.SuppressAccessTracking = true;

        auto items = WaitFor(queryContext->Client()->ListNode(directory.GetPath(), options))
            .ValueOrThrow();
        auto itemList = ConvertTo<IListNodePtr>(items);

        std::vector<TRichYPath> itemPaths;
        for (const auto& child : itemList->GetChildren()) {
            const auto& attributes = child->Attributes();
            auto path = attributes.Get<TYPath>("path");
            if (IsPathAllowed(path, arguments, context)) {
                itemPaths.emplace_back(path, directory.Attributes());
            }
        }

        // We intentionally skip all non-table items for better user experience.
        auto tables = FetchTables(
            queryContext->Client(),
            queryContext->Host,
            std::move(itemPaths),
            /* skipUnsuitableItems */ true,
            queryContext->Logger);

        std::sort(tables.begin(), tables.end(), [] (const TTablePtr& lhs, const TTablePtr& rhs) {
            return lhs->Path.GetPath() < rhs->Path.GetPath();
        });

        return CreateStorageDistributor(queryContext, std::move(tables));
    }

protected:
    virtual bool IsPathAllowed(
        const TYPath& path,
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
    bool IsPathAllowed(
        const TYPath& path,
        TArguments& arguments,
        const Context& context) const override
    {
        if (arguments.size() == 1) {
            return true;
        } else if (arguments.size() == 2) {
            auto from = TString(EvaluateArgument<std::string>(arguments[1], context));
            return BaseName(path) >= from;
        } else if (arguments.size() == 3) {
            auto from = TString(EvaluateArgument<std::string>(arguments[1], context));
            auto to = TString(EvaluateArgument<std::string>(arguments[2], context));
            return BaseName(path) >= from && BaseName(path) <= to;
        } else {
            throw Exception(
                "Too may arguments: "
                "expected 1, 2 or 3, provided: " + std::to_string(arguments.size()),
                ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TConcatenateTablesRegexp
    : public TListFilterAndConcatenateTables
{
public:
    TConcatenateTablesRegexp()
        : TListFilterAndConcatenateTables()
    { }

    static std::string GetName()
    {
        return "concatYtTablesRegexp";
    }

    virtual std::string getName() const override
    {
        return GetName();
    }

private:
    mutable std::unique_ptr<OptimizedRegularExpression> Matcher_;

    bool IsPathAllowed(
        const TYPath& path,
        TArguments& arguments,
        const Context& context) const override
    {
        // 1) directory, 2) regexp
        ValidateNumberOfArguments(arguments, 2);

        if (!Matcher_) {
            const auto regexp = EvaluateArgument<std::string>(arguments[1], context);
            Matcher_ = std::make_unique<OptimizedRegularExpression>(std::move(regexp));
        }

        return Matcher_->match(BaseName(path));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TConcatenateTablesLike
    : public TListFilterAndConcatenateTables
{
public:
    TConcatenateTablesLike()
        : TListFilterAndConcatenateTables()
    { }

    static std::string GetName()
    {
        return "concatYtTablesLike";
    }

    std::string getName() const override
    {
        return GetName();
    }

private:
    mutable std::unique_ptr<Poco::Glob> Matcher_;

    bool IsPathAllowed(
        const TYPath& path,
        TArguments& arguments,
        const Context& context) const override
    {
        // 1) directory 2) pattern
        ValidateNumberOfArguments(arguments, 2);

        if (!Matcher_) {
            auto pattern = EvaluateArgument<std::string>(arguments[1], context);
            Matcher_ = std::make_unique<Poco::Glob>(pattern);
        }

        return Matcher_->match(BaseName(path));
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
