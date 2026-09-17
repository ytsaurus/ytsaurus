#include "pipeline_init.h"

#include <yt/yt/flow/library/cpp/pipeline_tables/public.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/ypath/helpers.h>

namespace NYT::NFlow {

using namespace NApi;
using namespace NYPath;
using namespace NYTree;
using namespace NCypressClient;
using namespace NTransactionClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TNodeId CreatePipelineNode(
    const IClientPtr& client,
    const TYPath& path,
    const TCreateNodeOptions& options)
{
    auto attributes = options.Attributes ? options.Attributes->Clone() : CreateEphemeralAttributes();
    auto initializeTables = attributes->GetAndRemove<bool>("initialize_tables", true);
    attributes->Set(PipelineFormatVersionAttribute, CurrentPipelineFormatVersion);

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
        auto createNodeOptions = options;
        createNodeOptions.Attributes = std::move(attributes);
        return WaitFor(transaction->CreateNode(path, EObjectType::MapNode, createNodeOptions))
            .ValueOrThrow();
    }();

    if (initializeTables) {
        std::vector<TFuture<void>> createTableFutures;
        auto addCreateFutures = [&] (const auto& definitions) {
            for (const auto& [name, definition] : definitions) {
                TCreateNodeOptions createOptions;
                createOptions.Attributes = BuildPipelineTableAttributes(definition);
                createOptions.IgnoreExisting = options.IgnoreExisting;
                createTableFutures.push_back(
                    transaction->CreateNode(getTablePath(name), EObjectType::Table, createOptions)
                        .AsVoid());
            }
        };
        const auto& definitions = GetPipelineTableDefinitions();
        addCreateFutures(definitions.Tables);
        addCreateFutures(definitions.Queues);

        WaitFor(AllSucceeded(std::move(createTableFutures)))
            .ThrowOnError();
    }

    WaitFor(transaction->Commit())
        .ThrowOnError();

    if (initializeTables) {
        std::vector<TFuture<void>> mountTableFutures;
        auto addMountFutures = [&] (const auto& definitions) {
            for (const auto& [name, _] : definitions) {
                mountTableFutures.push_back(client->MountTable(getTablePath(name))
                        .AsVoid());
            }
        };
        const auto& definitions = GetPipelineTableDefinitions();
        addMountFutures(definitions.Tables);
        addMountFutures(definitions.Queues);
        WaitFor(AllSucceeded(std::move(mountTableFutures)))
            .ThrowOnError();
    }

    return pipelineNodeId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
