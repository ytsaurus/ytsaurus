#include "table_functions.h"

#include "query_context.h"
#include "storage_distributor.h"
#include "table.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/yson/string.h>

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

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

using namespace DB;

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
std::string EvaluateIdentifierArgument(ASTPtr& argument, ContextPtr context)
{
    argument = evaluateConstantExpressionOrIdentifierAsLiteral(argument, context);
    return static_cast<const ASTLiteral &>(*argument).value.safeGet<std::string>();
}

template <typename T>
T EvaluateArgument(ASTPtr& argument, ContextPtr context)
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

    void parseArguments(const ASTPtr& functionAst, ContextPtr context) override
    {
        auto& functionNode = typeid_cast<ASTFunction &>(*functionAst);
        auto& arguments = GetAllArguments(functionNode);
        tablePaths.reserve(arguments.size());
        for (auto& argument : arguments) {
            tablePaths.push_back(TRichYPath::Parse(ToString(EvaluateIdentifierArgument(argument, context))));
        }
    }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override
    {
        auto table_tmp = Execute(context);
        return table_tmp->getInMemoryMetadataPtr()->getColumns();
    }

    StoragePtr executeImpl(
        const ASTPtr& /*functionAst*/,
        ContextPtr context,
        const std::string& /*tableName*/,
        ColumnsDescription /*cached_columns*/) const override
    {
        return Execute(context);
    }

    const char* getStorageTypeName() const override
    {
        return "YT";
    }

private:
    StoragePtr Execute(ContextPtr context) const
    {
        auto* queryContext = GetQueryContext(context);

        auto tables = FetchTables(
            queryContext,
            std::move(tablePaths),
            /*skipUnsuitableNodes*/ false,
            queryContext->Settings->DynamicTable->EnableDynamicStoreRead,
            queryContext->Logger);

        return CreateStorageDistributor(context, std::move(tables));
    }

    std::vector<TRichYPath> tablePaths;
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

    void parseArguments(const ASTPtr& functionAst, ContextPtr context) override
    {
        auto& functionNode = typeid_cast<ASTFunction &>(*functionAst);
        auto& arguments = GetAllArguments(functionNode);
        directory = TRichYPath(TString(GetDirectoryRequiredArgument(arguments, context)));
        parsePathArguments(arguments, context);
    }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override
    {
        auto table_tmp = Execute(context);
        return table_tmp->getInMemoryMetadataPtr()->getColumns();
    }

    StoragePtr executeImpl(
        const ASTPtr& /* functionAst */,
        ContextPtr context,
        const std::string& /* tableName */,
        ColumnsDescription /* cached_columns */) const override
    {
        return Execute(context);
    }

    const char* getStorageTypeName() const override
    {
        return "YT";
    }

protected:
    virtual void parsePathArguments(TArguments& arguments, ContextPtr context) = 0;
    virtual bool IsPathAllowed(const TYPath& path) const = 0;

private:
    std::string GetDirectoryRequiredArgument(
        TArguments& arguments,
        ContextPtr context) const
    {
        if (arguments.empty()) {
            throw Exception(
                "Table function " + getName() + " expected at least one argument: directory path",
                ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION);
        }

        return EvaluateIdentifierArgument(arguments[0], context);
    }

    StoragePtr Execute(ContextPtr context) const
    {
        auto* queryContext = GetQueryContext(context);
        const auto& Logger = queryContext->Logger;

        YT_LOG_INFO("Listing directory (Path: %v)", directory);

        TListNodeOptions options;
        static_cast<TMasterReadOptions&>(options) = *queryContext->Settings->CypressReadOptions;
        options.Attributes = {
            "path",
        };
        options.SuppressAccessTracking = true;
        options.SuppressExpirationTimeoutRenewal = true;

        auto items = WaitFor(queryContext->Client()->ListNode(directory.GetPath(), options))
            .ValueOrThrow();
        auto itemList = ConvertTo<IListNodePtr>(items);

        std::vector<TRichYPath> itemPaths;
        for (const auto& child : itemList->GetChildren()) {
            const auto& attributes = child->Attributes();
            auto path = attributes.Get<TYPath>("path");
            if (IsPathAllowed(path)) {
                itemPaths.emplace_back(path, directory.Attributes());
            }
        }

        // We intentionally skip all non-table items for better user experience.
        auto tables = FetchTables(
            queryContext,
            std::move(itemPaths),
            /*skipUnsuitableItems*/ true,
            queryContext->Settings->DynamicTable->EnableDynamicStoreRead,
            Logger);

        std::sort(tables.begin(), tables.end(), [] (const TTablePtr& lhs, const TTablePtr& rhs) {
            return lhs->Path.GetPath() < rhs->Path.GetPath();
        });

        return CreateStorageDistributor(context, std::move(tables));
    }

    TRichYPath directory;
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
    void parsePathArguments(TArguments& arguments, ContextPtr context) override
    {
        args_count = arguments.size();
        if (args_count == 2) {
            from = TString(EvaluateArgument<std::string>(arguments[1], context));
        } else if (args_count == 3) {
            from = TString(EvaluateArgument<std::string>(arguments[1], context));
            to = TString(EvaluateArgument<std::string>(arguments[2], context));
        } else if (3 < args_count){
            throw Exception(
                "Too may arguments: "
                "expected 1, 2 or 3, provided: " + std::to_string(arguments.size()),
                ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);
        }
    }

    bool IsPathAllowed(const TYPath& path) const override
    {
        if (args_count == 1) {
            return true;
        } else if (args_count == 2) {
            return BaseName(path) >= from;
        } else /* if (args_count == 3) */ {
            return BaseName(path) >= from && BaseName(path) <= to;
        }
    }

    size_t args_count;
    TString from;
    TString to;
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

    std::string getName() const override
    {
        return GetName();
    }

private:
    mutable std::unique_ptr<OptimizedRegularExpression> Matcher_;

    void parsePathArguments(TArguments& arguments, ContextPtr context) override
    {
        // 1) directory, 2) regexp
        ValidateNumberOfArguments(arguments, 2);
        const auto regexp = EvaluateArgument<std::string>(arguments[1], context);
        Matcher_ = std::make_unique<OptimizedRegularExpression>(std::move(regexp));
    }

    bool IsPathAllowed(const TYPath& path) const override
    {
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

    void parsePathArguments(TArguments& arguments, ContextPtr context) override
    {
        // 1) directory 2) pattern
        ValidateNumberOfArguments(arguments, 2);
        auto pattern = EvaluateArgument<std::string>(arguments[1], context);
        Matcher_ = std::make_unique<Poco::Glob>(pattern);
    }

    bool IsPathAllowed(const TYPath& path) const override
    {
        return Matcher_->match(BaseName(path));
    }
};

////////////////////////////////////////////////////////////////////////////////

void RegisterTableFunctionsConcat()
{
    auto& factory = TableFunctionFactory::instance();

    // TODO(max42): simplify.
#define REGISTER_TABLE_FUNCTION(TFunction) \
    factory.registerFunction( \
        TFunction::GetName(), \
        [=] { return std::make_shared<TFunction>(); }); \

    REGISTER_TABLE_FUNCTION(TConcatenateTablesList);
    REGISTER_TABLE_FUNCTION(TConcatenateTablesRange);
    REGISTER_TABLE_FUNCTION(TConcatenateTablesRegexp);
    REGISTER_TABLE_FUNCTION(TConcatenateTablesLike);

#undef REGISTER_TABLE_FUNCTION

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
