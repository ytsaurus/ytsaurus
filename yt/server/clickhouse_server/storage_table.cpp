#include "storage_table.h"

#include "query_helpers.h"
#include "storage_distributed.h"
#include "private.h"
#include "config.h"
#include "block_output_stream.h"
#include "query_context.h"
#include "helpers.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/client/table_client/name_table.h>

#include <yt/client/object_client/public.h>

#include <yt/client/ypath/rich.h>

#include <yt/core/ytree/convert.h>

#include <yt/core/yson/string.h>

#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/StorageFactory.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <common/logger_useful.h>

namespace NYT::NClickHouseServer {

using namespace NYPath;
using namespace DB;
using namespace NTableClient;
using namespace NObjectClient;
using namespace NConcurrency;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TStorageTable
    : public TStorageDistributedBase
{
private:
    TClickHouseTablePtr Table;

public:
    TStorageTable(TClickHouseTablePtr table)
        : TStorageDistributedBase(
            table->TableSchema,
            TClickHouseTableSchema::From(*table))
        , Table(std::move(table))
    { }

    std::string getTableName() const override
    {
        return std::string(Table->Path.GetPath());
    }

    virtual QueryProcessingStage::Enum getQueryProcessingStage(const Context& /* context */) const
    {
        return QueryProcessingStage::WithMergeableState;
    }

    virtual BlockOutputStreamPtr write(const ASTPtr& /* ptr */, const Context& context) override
    {
        auto* queryContext = GetQueryContext(context);
        // Set append if it is not set.
        Table->Path.SetAppend(Table->Path.GetAppend(true /* defaultValue */));
        auto writer = WaitFor(CreateSchemalessTableWriter(
            queryContext->Bootstrap->GetConfig()->TableWriterConfig,
            New<TTableWriterOptions>(),
            Table->Path,
            New<TNameTable>(),
            queryContext->Client(),
            nullptr /* transaction */))
            .ValueOrThrow();
        return CreateBlockOutputStream(std::move(writer), queryContext->Logger);
    }

private:
    virtual std::vector<TRichYPath> GetTablePaths() const override
    {
        return {Table->Path};
    }

    virtual ASTPtr RewriteSelectQueryForTablePart(
        const ASTPtr& queryAst,
        const std::string& subquerySpec) override
    {
        auto modifiedQueryAst = queryAst->clone();

        auto& selectQuery = typeid_cast<ASTSelectQuery&>(*modifiedQueryAst);

        const auto& tableExpressions = GetAllTableExpressions(selectQuery);

        bool anyTableFunction = false;

        for (const auto& tableExpression : tableExpressions) {
            ASTPtr tableFunction;

            if (tableExpression->database_and_table_name) {
                const auto& tableName = static_cast<ASTIdentifier&>(*tableExpression->database_and_table_name).name;
                if (tableName != getTableName()) {
                    continue;
                }
            }

            if (tableExpression->table_function) {
                auto& function = typeid_cast<ASTFunction &>(*tableExpression->table_function);
                if (function.name == "ytTable") {
                    // TODO: forward all args
                    tableFunction = makeASTFunction(
                        "ytSubquery",
                        std::make_shared<ASTLiteral>(subquerySpec));
                }

            } else {
                tableFunction = makeASTFunction(
                    "ytSubquery",
                    std::make_shared<ASTLiteral>(subquerySpec));
            }

            if (tableFunction) {
                tableExpression->table_function = std::move(tableFunction);
                tableExpression->database_and_table_name = nullptr;
                tableExpression->subquery = nullptr;
                anyTableFunction = true;
            }

            static_cast<ASTTableExpression&>(*tableExpression).sample_offset = nullptr;
            static_cast<ASTTableExpression&>(*tableExpression).sample_size = nullptr;
        }

        if (!anyTableFunction) {
            throw Exception("Invalid SelectQuery, no table function produced", Exception(queryToString(queryAst), ErrorCodes::LOGICAL_ERROR), ErrorCodes::LOGICAL_ERROR);
        }

        return modifiedQueryAst;
    }
};

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageTableFromCH(StorageFactory::Arguments args)
{
    auto* queryContext = GetQueryContext(args.local_context);
    const auto& client = queryContext->Client();
    const auto& Logger = queryContext->Logger;

    TKeyColumns keyColumns;

    if (args.storage_def->order_by) {
        auto orderByAst = args.storage_def->order_by->ptr();
        orderByAst = MergeTreeData::extractKeyExpressionList(orderByAst);
        for (const auto& child : orderByAst->children) {
            auto* identifier = dynamic_cast<ASTIdentifier*>(child.get());
            if (!identifier) {
                THROW_ERROR_EXCEPTION("CHYT does not support compound expressions as parts of key")
                    << TErrorAttribute("expression", child->getColumnName());
            }
            keyColumns.emplace_back(identifier->getColumnName());
        }
    }

    auto path = TRichYPath::Parse(TString(args.table_name));
    YT_LOG_INFO("Creating table from CH engine (Path: %v, Columns: %v, KeyColumns: %v)",
        path,
        args.columns.toString(),
        keyColumns);

    auto schema = ConvertToTableSchema(args.columns, keyColumns);
    YT_LOG_DEBUG("Inferred table schema (Schema: %v)", schema);

    auto attributes = ConvertToAttributes(queryContext->Bootstrap->GetConfig()->Engine->CreateTableDefaultAttributes);
    attributes->Set("schema", schema);

    if (!args.engine_args.empty()) {
        if (static_cast<int>(args.engine_args.size()) > 1) {
            THROW_ERROR_EXCEPTION("YtTable accepts at most one argument");
        }
        const auto* ast = args.engine_args[0]->as<ASTLiteral>();
        if (ast && ast->value.getType() == Field::Types::String) {
            auto extraAttributes = ConvertToAttributes(TYsonString(TString(safeGet<String>(ast->value))));
            attributes->MergeFrom(*extraAttributes);
        } else {
            THROW_ERROR_EXCEPTION("Extra attributes must be a string literal");
        }
    }

    YT_LOG_INFO("Creating table (Attributes: %v)", ConvertToYsonString(attributes->ToMap(), EYsonFormat::Text));
    NApi::TCreateNodeOptions options;
    options.Attributes = std::move(attributes);
    WaitFor(client->CreateNode(path.GetPath(), NObjectClient::EObjectType::Table, options))
        .ThrowOnError();
    YT_LOG_INFO("Table created");

    auto table = FetchClickHouseTable(client, path, Logger);
    YCHECK(table);

    return CreateStorageTable(std::move(table));
}

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageTable(TClickHouseTablePtr table)
{
    auto storage = std::make_shared<TStorageTable>(std::move(table));
    storage->startup();

    return storage;
}

/////////////////////////////////////////////////////////////////////////////

void RegisterStorageTable()
{
    auto& factory = StorageFactory::instance();
    factory.registerStorage("YtTable", CreateStorageTableFromCH);
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
