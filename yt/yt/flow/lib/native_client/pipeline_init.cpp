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
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

TTableSchema GetInputMessagesTableSchema()
{
    return TTableSchema(
        std::vector{
            TColumnSchema("computation_id", EValueType::String, ESortOrder::Ascending),
            TColumnSchema("key", EValueType::String, ESortOrder::Ascending),
            TColumnSchema("message_id", EValueType::String, ESortOrder::Ascending),
            TColumnSchema("system_timestamp", EValueType::Uint64),
        },
        /*strict*/ true,
        /*uniqueKeys*/ true);
}

TTableSchema GetOutputMessagesTableSchema()
{
    return TTableSchema(
        std::vector{
            TColumnSchema("computation_id", EValueType::String, ESortOrder::Ascending),
            TColumnSchema("partition_id", EValueType::String, ESortOrder::Ascending),
            TColumnSchema("message_id", EValueType::String, ESortOrder::Ascending),
            TColumnSchema("message", EValueType::String),
            TColumnSchema("system_timestamp", EValueType::Uint64),
        },
        /*strict*/ true,
        /*uniqueKeys*/ true);
}

TTableSchema GetPartitionDataTableSchema()
{
    return TTableSchema(
        std::vector{
            TColumnSchema("partition_id", EValueType::String, ESortOrder::Ascending),
            TColumnSchema("data", EValueType::String),
        },
        /*strict*/ true,
        /*uniqueKeys*/ true);
}

auto GetTables()
{
    return std::vector<std::tuple<TString, TTableSchema>>{
        {InputMessagesTableName, GetInputMessagesTableSchema()},
        {OutputMessagesTableName, GetOutputMessagesTableSchema()},
        {PartitionDataTableName, GetPartitionDataTableSchema()},
    };
}

} // namespace

TNodeId CreatePipelineNode(
    const IClientPtr& client,
    const TYPath& path,
    const TCreateNodeOptions& options)
{
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

    std::vector<TFuture<void>> createTableFutures;
    for (const auto& [tableName, tableSchema] : GetTables()) {
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("schema", tableSchema);
        attributes->Set("dynamic", true);
        TCreateNodeOptions createOptions;
        createOptions.Attributes = std::move(attributes);
        createTableFutures.push_back(transaction->CreateNode(YPathJoin(path, ToYPathLiteral(tableName)), EObjectType::Table, createOptions)
            .AsVoid());
    }

    WaitFor(AllSucceeded(std::move(createTableFutures)))
        .ThrowOnError();

    WaitFor(transaction->Commit())
        .ThrowOnError();

    std::vector<TFuture<void>> mountTableFutures;
    for (const auto& [tableName, _] : GetTables()) {
        mountTableFutures.push_back(client->MountTable(YPathJoin(path, ToYPathLiteral(tableName)))
            .AsVoid());
    }

    WaitFor(AllSucceeded(std::move(mountTableFutures)))
        .ThrowOnError();

    return pipelineNodeId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
