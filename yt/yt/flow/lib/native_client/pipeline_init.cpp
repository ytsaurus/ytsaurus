#include "pipeline_init.h"

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/ypath/helpers.h>

namespace NYT::NFlow {

using namespace NApi;
using namespace NYPath;
using namespace NYTree;
using namespace NCypressClient;
using namespace NTransactionClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

IAttributeDictionaryPtr CreateDynamicTableAttributes(const TTableSchema& tableSchema)
{
    auto attributes = CreateEphemeralAttributes();
    attributes->Set("schema", tableSchema);
    attributes->Set("dynamic", true);
    return attributes;
}

IAttributeDictionaryPtr GetInputMessagesTableAttributes()
{
    auto attributes = CreateDynamicTableAttributes(TTableSchema(
        std::vector{
            TColumnSchema("computation_id", EValueType::String, ESortOrder::Ascending),
            TColumnSchema("key", EValueType::Any, ESortOrder::Ascending),
            TColumnSchema("message_id", EValueType::String, ESortOrder::Ascending),
            TColumnSchema("system_timestamp", EValueType::Uint64),
        },
        /*strict*/ true,
        /*uniqueKeys*/ true));

    attributes->Set(
        "mount_config",
        BuildYsonStringFluently(NYson::EYsonFormat::Binary)
            .BeginMap()
                .Item("min_data_versions").Value(0)
                .Item("min_data_ttl").Value(0)
                .Item("row_merger_type").Value(NTabletClient::ERowMergerType::Watermark)
            .EndMap());

    return attributes;
}

IAttributeDictionaryPtr GetOutputMessagesTableAttributes()
{
    return CreateDynamicTableAttributes(TTableSchema(
        std::vector{
            TColumnSchema("partition_id", EValueType::String, ESortOrder::Ascending),
            TColumnSchema("message_id", EValueType::String, ESortOrder::Ascending),
            TColumnSchema("message", EValueType::String),
            TColumnSchema("system_timestamp", EValueType::Uint64),
            TColumnSchema("codec", EValueType::Int64),
        },
        /*strict*/ true,
        /*uniqueKeys*/ true));
}

IAttributeDictionaryPtr GetCheckpointsTableAttributes()
{
    return CreateDynamicTableAttributes(TTableSchema(
        std::vector{
            TColumnSchema("computation_id", EValueType::String, ESortOrder::Ascending),
            TColumnSchema("key", EValueType::Any, ESortOrder::Ascending),
            TColumnSchema("checkpoint", EValueType::Any),
        },
        /*strict*/ true,
        /*uniqueKeys*/ true));
}

IAttributeDictionaryPtr GetTimerMessagesTableAttributes()
{
    return CreateDynamicTableAttributes(TTableSchema(
        std::vector{
            TColumnSchema("computation_id", EValueType::String, ESortOrder::Ascending),
            TColumnSchema("key", EValueType::Any, ESortOrder::Ascending),
            TColumnSchema("message_id", EValueType::String, ESortOrder::Ascending),
            TColumnSchema("message", EValueType::String),
            TColumnSchema("system_timestamp", EValueType::Uint64),
            TColumnSchema("codec", EValueType::Int64),
        },
        /*strict*/ true,
        /*uniqueKeys*/ true));
}

IAttributeDictionaryPtr GetControllerLogsTableAttributes()
{
    auto attributes = CreateDynamicTableAttributes(TTableSchema(
        std::vector{
            TColumnSchema("host", EValueType::String),
            TColumnSchema("data", EValueType::String),
            TColumnSchema("codec", EValueType::String),
            TColumnSchema("$timestamp", EValueType::Uint64),
            TColumnSchema("$cumulative_data_weight", EValueType::Int64),
        },
        /*strict*/ true));

    attributes->Set("tablet_count", 1);
    attributes->Set(
        "mount_config",
        BuildYsonStringFluently(NYson::EYsonFormat::Binary)
            .BeginMap()
                .Item("min_data_versions").Value(0)
                .Item("min_data_ttl").Value(0)
                .Item("max_data_ttl").Value(86400000)  // 1d
            .EndMap());

    return attributes;
}

auto GetTables()
{
    return std::vector<std::tuple<TStringBuf, IAttributeDictionaryPtr>>{
        {InputMessagesTableName, GetInputMessagesTableAttributes()},
        {OutputMessagesTableName, GetOutputMessagesTableAttributes()},
        {CheckpointsTableName, GetCheckpointsTableAttributes()},
        {TimerMessagesTableName, GetTimerMessagesTableAttributes()},
        {ControllerLogsTableName, GetControllerLogsTableAttributes()},
    };
}

} // namespace

TNodeId CreatePipelineNode(
    const IClientPtr& client,
    const TYPath& path,
    const TCreateNodeOptions& options)
{
    auto getTablePath = [&path] (TStringBuf tableName) {
        return YPathJoin(path, ToYPathLiteral(tableName));
    };

    auto transaction = [&] {
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Create pipeline %v", path));
        TTransactionStartOptions startOptions{
            .ParentId = options.TransactionId,
            .Attributes = std::move(attributes),
        };
        return WaitFor(client->StartTransaction(ETransactionType::Master, startOptions))
            .ValueOrThrow();
    }();

    auto pipelineNodeId = [&] {
        auto attributes = options.Attributes ? options.Attributes->Clone() : CreateEphemeralAttributes();
        attributes->Set(PipelineFormatVersionAttribute, CurrentPipelineFormatVersion);
        auto createNodeOptions = options;
        createNodeOptions.Attributes = std::move(attributes);
        return WaitFor(transaction->CreateNode(path, EObjectType::MapNode, createNodeOptions))
            .ValueOrThrow();
    }();

    auto attributes = options.Attributes ? options.Attributes->Clone() : EmptyAttributes().Clone();
    auto initializeTables = attributes->Get<bool>("initialize_tables", true);
    if (initializeTables) {
        std::vector<TFuture<void>> createTableFutures;
        for (const auto& [tableName, tableAttributes] : GetTables()) {
            TCreateNodeOptions createOptions;
            createOptions.Attributes = tableAttributes;
            createTableFutures.push_back(
                transaction->CreateNode(getTablePath(tableName), EObjectType::Table, createOptions)
                .AsVoid());
        }

        WaitFor(AllSucceeded(std::move(createTableFutures)))
            .ThrowOnError();
    }

    WaitFor(transaction->Commit())
        .ThrowOnError();

    if (initializeTables) {
        std::vector<TFuture<void>> mountTableFutures;
        for (const auto& [tableName, _] : GetTables()) {
            mountTableFutures.push_back(client->MountTable(getTablePath(tableName))
                .AsVoid());
        }
        WaitFor(AllSucceeded(std::move(mountTableFutures)))
            .ThrowOnError();
    }

    return pipelineNodeId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
