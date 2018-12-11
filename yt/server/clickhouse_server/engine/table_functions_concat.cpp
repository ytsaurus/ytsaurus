#include "table_functions_concat.h"

#include "auth_token.h"
#include "format_helpers.h"
#include "storage_concat.h"
#include "type_helpers.h"

#include <yt/server/clickhouse_server/native/table_partition.h>
#include <yt/server/clickhouse_server/native/storage.h>

//#include <Common/Exception.h>
//#include <Common/OptimizedRegularExpression.h>
//#include <Common/typeid_cast.h>
//#include <DataTypes/DataTypeString.h>
//#include <DataTypes/DataTypesNumber.h>
//#include <Interpreters/Context.h>
//#include <Interpreters/evaluateConstantExpression.h>
//#include <Parsers/ASTFunction.h>
//#include <Parsers/ASTLiteral.h>
//#include <Storages/StorageNull.h>
//#include <TableFunctions/ITableFunction.h>
//#include <TableFunctions/TableFunctionFactory.h>

//#include <Poco/Glob.h>
//#include <Poco/Logger.h>

//#include <common/logger_useful.h>


namespace DB {

namespace ErrorCodes {
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int CANNOT_SELECT;
}

} // namespace DB

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

using namespace DB;

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

////////////////////////////////////////////////////////////////////////////////

void SortTablesByName(NNative::TTableList& tables)
{
    std::sort(
        tables.begin(),
        tables.end(),
        [] (const NNative::TTablePtr& lhs, const NNative::TTablePtr& rhs) {
            return lhs->Name < rhs->Name;
        });
}

std::string GetTableBaseName(const NNative::TTable& table) {
    // TODO: abstract ypath
    return ToStdString(TStringBuf(table.Name).RNextTok('/'));
}

using TTableNameFilter = std::function<bool(const std::string& tableName)>;

NNative::TTableList FilterTablesByName(
    const NNative::TTableList& tables,
    TTableNameFilter nameFilter)
{
    NNative::TTableList filtered;
    for (const auto& table : tables) {
        const auto basename = GetTableBaseName(*table);
        if (nameFilter(basename)) {
            filtered.push_back(table);
        }
    }
    return filtered;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

// select * from concatYtTables(`A`, `B`, ..., `Z`)

class TConcatenateTablesList
    : public ITableFunction
{
private:
    NNative::IStoragePtr Storage;
    IExecutionClusterPtr Cluster;

    Poco::Logger* Logger;

public:
    TConcatenateTablesList(
        NNative::IStoragePtr storage,
        IExecutionClusterPtr cluster)
        : Storage(std::move(storage))
        , Cluster(std::move(cluster))
        , Logger(&Poco::Logger::get("ConcatYtTables"))
    {}

    static constexpr auto Name = "concatYtTables";

    std::string getName() const override
    {
        return Name;
    }

    StoragePtr executeImpl(
        const ASTPtr& functionAst,
        const Context& context) const override;

private:
    std::vector<TString> EvaluateArguments(
        TArguments& arguments,
        const Context& context) const;

    NNative::TTableList GetTables(
        const std::vector<TString>& tableNames,
        const NNative::IAuthorizationToken& token) const;

    StoragePtr Execute(
        const std::vector<TString>& tableNames,
        const Context& context) const;
};

////////////////////////////////////////////////////////////////////////////////

StoragePtr TConcatenateTablesList::executeImpl(
    const ASTPtr& functionAst,
    const Context& context) const
{
    auto& functionNode = typeid_cast<ASTFunction &>(*functionAst);
    auto& arguments = GetAllArguments(functionNode);

    auto tableNames = EvaluateArguments(arguments, context);

    return Execute(tableNames, context);
}

std::vector<TString> TConcatenateTablesList::EvaluateArguments(
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

NNative::TTableList TConcatenateTablesList::GetTables(
    const std::vector<TString>& tableNames,
    const NNative::IAuthorizationToken& token) const
{
    // TODO: batch GetTabes in NNative::IStorage

    NNative::TTableList tables;
    tables.reserve(tableNames.size());
    for (const auto& name : tableNames) {
        auto table = Storage->GetTable(token, name);
        tables.push_back(std::move(table));
    }

    return tables;
}

StoragePtr TConcatenateTablesList::Execute(
    const std::vector<TString>& tableNames,
    const Context& context) const
{
    CH_LOG_DEBUG(Logger, "Execute table function " << getName() << "(" << JoinStrings(", ", tableNames) << ")");

    auto token = CreateAuthToken(*Storage, context);
    auto tables = GetTables(tableNames, *token);

    return CreateStorageConcat(
        Storage,
        std::move(tables),
        Cluster);
}

////////////////////////////////////////////////////////////////////////////////

// Abstract base class for functions that
// list directory, filter tables (by regexp, by date range, last k, etc)
// and concatenate them.

class TListFilterAndConcatenateTables
    : public ITableFunction
{
private:
    NNative::IStoragePtr Storage;
    IExecutionClusterPtr Cluster;

    Poco::Logger* Logger;

public:
    TListFilterAndConcatenateTables(
        NNative::IStoragePtr storage,
        IExecutionClusterPtr cluster,
        Poco::Logger* logger)
        : Storage(std::move(storage))
        , Cluster(std::move(cluster))
        , Logger(logger)
    {}

    StoragePtr executeImpl(
        const ASTPtr& functionAst,
        const Context& context) const override;

protected:
    const NNative::IStoragePtr& GetStorage() const
    {
        return Storage;
    }

    Poco::Logger* GetLogger() const
    {
        return Logger;
    }

protected:
    // 0-th argument reserved to directory path
    virtual NNative::TTableList FilterTables(
        const NNative::TTableList& tables,
        TArguments& arguments,
        const Context& context) const = 0;

private:
    std::string GetDirectoryRequiredArgument(
        TArguments& arguments,
        const Context& context) const;

    NNative::TTableList ListAllTables(
        const std::string& directory,
        const NNative::IAuthorizationToken& authToken) const;

    // TODO: workaround, remove this
    void CollectSchemas(
        NNative::TTableList& tableNames,
        const NNative::IAuthorizationToken& authToken) const;

    StoragePtr CreateStorage(
        const NNative::TTableList& tables,
        const Context& context) const;
};

////////////////////////////////////////////////////////////////////////////////

StoragePtr TListFilterAndConcatenateTables::executeImpl(
    const ASTPtr& functionAst,
    const Context& context) const
{
    auto& functionNode = typeid_cast<ASTFunction &>(*functionAst);
    auto& arguments = GetAllArguments(functionNode);

    const auto directory = GetDirectoryRequiredArgument(
        arguments,
        context);

    auto authToken = CreateAuthToken(*Storage, context);

    auto allTables = ListAllTables(directory, *authToken);
    SortTablesByName(allTables);

    auto selectedTables = FilterTables(allTables, arguments, context);

    // TODO: ListTables doesn't provide table schemas
    CollectSchemas(selectedTables, *authToken);

    return CreateStorage(selectedTables, context);
}

std::string TListFilterAndConcatenateTables::GetDirectoryRequiredArgument(
    TArguments& arguments,
    const Context& context) const
{
    if (arguments.empty()) {
        throw Exception(
            "Table function " + getName() + " expected at least one argument: directory path",
            ErrorCodes::TOO_LESS_ARGUMENTS_FOR_FUNCTION);
    }

    return EvaluateIdentifierArgument(arguments[0], context);
}

NNative::TTableList TListFilterAndConcatenateTables::ListAllTables(
    const std::string& directory,
    const NNative::IAuthorizationToken& authToken) const
{
    return Storage->ListTables(authToken, ToString(directory), false);
}

void TListFilterAndConcatenateTables::CollectSchemas(
    NNative::TTableList& tables,
    const NNative::IAuthorizationToken& authToken) const
{
    for (auto& table : tables) {
        table = Storage->GetTable(authToken, table->Name);
    }
}

StoragePtr TListFilterAndConcatenateTables::CreateStorage(
    const NNative::TTableList& tables,
    const Context& context) const
{
    if (tables.empty()) {
        // TODO: create StorageNull
        throw Exception("No tables found by " + getName(), ErrorCodes::CANNOT_SELECT);
    }

    return CreateStorageConcat(Storage, tables, Cluster);
}

////////////////////////////////////////////////////////////////////////////////

class TConcatenateTablesRange
    : public TListFilterAndConcatenateTables
{
public:
    TConcatenateTablesRange(
        NNative::IStoragePtr storage,
        IExecutionClusterPtr cluster)
        : TListFilterAndConcatenateTables(
            std::move(storage),
            std::move(cluster),
            &Poco::Logger::get("ConcatYtTablesRange"))
    {}

    static constexpr auto Name = "concatYtTablesRange";

    std::string getName() const override
    {
        return Name;
    }

private:
    NNative::TTableList FilterTables(
        const NNative::TTableList& tables,
        TArguments& arguments,
        const Context& context) const override;
};

////////////////////////////////////////////////////////////////////////////////

NNative::TTableList TConcatenateTablesRange::FilterTables(
    const NNative::TTableList& tables,
    TArguments& arguments,
    const Context& context) const
{
    const size_t argumentCount = arguments.size();

    if (argumentCount == 1) {
        // All tables in directory
        return tables;
    }

    if (argumentCount == 2) {
        // [from, ...)
        auto from = EvaluateArgument<std::string>(arguments[1], context);

        auto nameFilter = [from] (const std::string& tableName) -> bool
        {
            return tableName >= from;
        };
        return FilterTablesByName(tables, std::move(nameFilter));
    }

    if (argumentCount == 3) {
        // [from, to] name range
        auto from = EvaluateArgument<std::string>(arguments[1], context);
        auto to = EvaluateArgument<std::string>(arguments[2], context);

        auto nameFilter = [from, to] (const std::string& tableName) -> bool
        {
            return tableName >= from && tableName <= to;
        };
        return FilterTablesByName(tables, std::move(nameFilter));
    }

    throw Exception(
        "Too may arguments: "
        "expected 1, 2 or 3, provided: " + std::to_string(arguments.size()),
        ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);
}

////////////////////////////////////////////////////////////////////////////////

class TConcatenateTablesRegexp
    : public TListFilterAndConcatenateTables
{
public:
    TConcatenateTablesRegexp(
        NNative::IStoragePtr storage,
        IExecutionClusterPtr cluster)
        : TListFilterAndConcatenateTables(
            std::move(storage),
            std::move(cluster),
            &Poco::Logger::get("ConcatYtTablesRegexp"))
    {}

    static constexpr auto Name = "concatYtTablesRegexp";

    std::string getName() const override
    {
        return Name;
    }

private:
    NNative::TTableList FilterTables(
        const NNative::TTableList& tables,
        TArguments& arguments,
        const Context& context) const override;
};

////////////////////////////////////////////////////////////////////////////////

NNative::TTableList TConcatenateTablesRegexp::FilterTables(
    const NNative::TTableList& tables,
    TArguments& arguments,
    const Context& context) const
{
    // 1) directory, 2) regexp
    ValidateNumberOfArguments(arguments, 2);

    const auto regexp = EvaluateArgument<std::string>(arguments[1], context);

    auto matcher = std::make_shared<OptimizedRegularExpression>(std::move(regexp));

    auto nameFilter = [matcher] (const std::string& tableName) -> bool
    {
        return matcher->match(tableName);
    };
    return FilterTablesByName(tables, std::move(nameFilter));
}

////////////////////////////////////////////////////////////////////////////////

class TConcatenateTablesLike
    : public TListFilterAndConcatenateTables
{
public:
    TConcatenateTablesLike(
        NNative::IStoragePtr storage,
        IExecutionClusterPtr cluster)
        : TListFilterAndConcatenateTables(
            std::move(storage),
            std::move(cluster),
            &Poco::Logger::get("ConcatYtTablesLike"))
    {}

    static constexpr auto Name = "concatYtTablesLike";

    std::string getName() const override
    {
        return Name;
    }

private:
    NNative::TTableList FilterTables(
        const NNative::TTableList& tables,
        TArguments& arguments,
        const Context& context) const override;
};


////////////////////////////////////////////////////////////////////////////////

NNative::TTableList TConcatenateTablesLike::FilterTables(
    const NNative::TTableList& tables,
    TArguments& arguments,
    const Context& context) const
{
    // 1) directory 2) pattern
    ValidateNumberOfArguments(arguments, 2);

    auto pattern = EvaluateArgument<std::string>(arguments[1], context);

    auto matcher = std::make_shared<Poco::Glob>(pattern);

    auto nameFilter = [matcher] (const std::string& tableName) -> bool
    {
        return matcher->match(tableName);
    };
    return FilterTablesByName(tables, std::move(nameFilter));
}

////////////////////////////////////////////////////////////////////////////////

void RegisterConcatenatingTableFunctions(
    NNative::IStoragePtr storage,
    IExecutionClusterPtr cluster)
{
    auto& factory = TableFunctionFactory::instance();

#define REGISTER_TABLE_FUNCTION(TFunction) \
    factory.registerFunction( \
        TFunction::Name, \
        [=] { return std::make_shared<TFunction>(storage, cluster); } \
        );

    REGISTER_TABLE_FUNCTION(TConcatenateTablesList);

    REGISTER_TABLE_FUNCTION(TConcatenateTablesRange);
    REGISTER_TABLE_FUNCTION(TConcatenateTablesRegexp);
    REGISTER_TABLE_FUNCTION(TConcatenateTablesLike);

#undef REGISTER_TABLE_FUNCTION

}

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
