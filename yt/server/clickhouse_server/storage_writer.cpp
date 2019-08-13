#include "storage_writer.h"

#include "block_output_stream.h"
#include "query_context.h"
#include "config.h"
#include "helpers.h"
#include "table.h"

#include <yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/client/ypath/rich.h>
#include <yt/client/table_client/name_table.h>

#include <Parsers/ASTCreateQuery.h>
#include <Storages/StorageFactory.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>

namespace NYT::NClickHouseServer {

using namespace NYPath;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TStorageDistributor
    : public DB::IStorage
{
public:
    TStorageDistributor(TRichYPath tablePath)
        : TablePath_(std::move(tablePath))
    { }

    std::string getName() const override
    {
        return "StorageDistributor";
    }

    virtual std::string getTableName() const
    {
        return std::string(TablePath_.GetPath().data());
    }

    virtual DB::BlockInputStreams read(
        const DB::Names& /* columnNames */,
        const DB::SelectQueryInfo& /* queryInfo */,
        const DB::Context& /* context */,
        DB::QueryProcessingStage::Enum /* processedStage */,
        size_t /* maxBlockSize */,
        unsigned /* numStreams */) override
    {
        THROW_ERROR_EXCEPTION("StorageWriter does not support reading");
    }

    virtual DB::BlockOutputStreamPtr write(const DB::ASTPtr& /* ptr */, const DB::Context& context) override
    {
        auto* queryContext = GetQueryContext(context);
        // Set append if it is not set.

        TablePath_.SetAppend(TablePath_.GetAppend(true /* defaultValue */));
        auto writer = WaitFor(CreateSchemalessTableWriter(
            queryContext->Bootstrap->GetConfig()->TableWriterConfig,
            New<TTableWriterOptions>(),
            TablePath_,
            New<TNameTable>(),
            queryContext->Client(),
            nullptr /* transaction */))
            .ValueOrThrow();
        return CreateBlockOutputStream(std::move(writer), queryContext->Logger);
    }

private:
    TRichYPath TablePath_;
};

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageWriter(DB::StorageFactory::Arguments args)
{
    auto* queryContext = GetQueryContext(args.local_context);
    const auto& client = queryContext->Client();
    const auto& Logger = queryContext->Logger;

    TKeyColumns keyColumns;

    if (args.storage_def->order_by) {
        auto orderByAst = args.storage_def->order_by->ptr();
        orderByAst = DB::MergeTreeData::extractKeyExpressionList(orderByAst);
        for (const auto& child : orderByAst->children) {
            auto* identifier = dynamic_cast<DB::ASTIdentifier*>(child.get());
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

    auto attributes = ConvertToAttributes(queryContext->Bootstrap->GetConfig()->Engine->CreateTableDefaultAttributes);
    if (!args.engine_args.empty()) {
        if (static_cast<int>(args.engine_args.size()) > 1) {
            THROW_ERROR_EXCEPTION("YtTable accepts at most one argument");
        }
        const auto* ast = args.engine_args[0]->as<DB::ASTLiteral>();
        if (ast && ast->value.getType() == DB::Field::Types::String) {
            auto extraAttributes = ConvertToAttributes(TYsonString(TString(DB::safeGet<std::string>(ast->value))));
            attributes->MergeFrom(*extraAttributes);
        } else {
            THROW_ERROR_EXCEPTION("Extra attributes must be a string literal");
        }
    }

    // Underscore indicates that the columns should be ignored, and that schema should be taken from the attributes.
    if (args.columns.getNamesOfPhysical() != std::vector<std::string>{"_"}) {
        auto schema = ConvertToTableSchema(args.columns, keyColumns);
        YT_LOG_DEBUG("Inferred table schema from columns (Schema: %v)", schema);
        attributes->Set("schema", schema);
    } else if (attributes->Contains("schema")) {
        YT_LOG_DEBUG("Table schema is taken from attributes (Schema: %v)", attributes->FindYson("schema"));
    } else {
        THROW_ERROR_EXCEPTION(
            "Table schema should be specified either by column list (possibly with ORDER BY) or by "
            "YT schema in attributes (as the only storage argument in YSON under key `schema`, in this case "
            "column list should consist of the only column named `_`)");
    };

    YT_LOG_DEBUG("Creating table (Attributes: %v)", ConvertToYsonString(attributes->ToMap(), EYsonFormat::Text));
    NApi::TCreateNodeOptions options;
    options.Attributes = std::move(attributes);
    auto id = WaitFor(client->CreateNode(path.GetPath(), NObjectClient::EObjectType::Table, options))
        .ValueOrThrow();
    YT_LOG_DEBUG("Table created (ObjectId: %v)", id);

    auto table = FetchClickHouseTable(client, path, Logger);
    YT_VERIFY(table);

    return std::make_shared<TStorageDistributor>(table->Path);
}

////////////////////////////////////////////////////////////////////////////////

void RegisterStorageWriter()
{
    auto& factory = DB::StorageFactory::instance();
    factory.registerStorage("YtTable", CreateStorageWriter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
