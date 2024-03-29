#include "queue_consumer_init.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/queue_client/consumer_client.h>

namespace NYT::NQueueClient {

using namespace NApi;
using namespace NYTree;
using namespace NTransactionClient;
using namespace NCypressClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TNodeId CreateConsumerNode(
    const NApi::IClientPtr& client,
    const NYPath::TYPath& path,
    const NApi::TCreateNodeOptions& options)
{
    auto transaction = [&] {
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Create consumer %v", path));
        TTransactionStartOptions startOptions{
            .ParentId = options.TransactionId,
            .Attributes = std::move(attributes),
        };
        return WaitFor(client->StartTransaction(ETransactionType::Master, startOptions))
            .ValueOrThrow();
    }();

    auto consumerNodeId = [&] {
        auto attributes = options.Attributes ? options.Attributes->Clone() : CreateEphemeralAttributes();

        attributes->Set("dynamic", true);
        attributes->Set("treat_as_queue_consumer", true);
        attributes->Set("schema", GetConsumerSchema());

        auto createNodeOptions = options;
        createNodeOptions.Attributes = std::move(attributes);
        return WaitFor(transaction->CreateNode(path, EObjectType::Table, createNodeOptions))
            .ValueOrThrow();
    }();

    WaitFor(transaction->Commit())
        .ThrowOnError();

    WaitFor(client->MountTable(path))
        .ThrowOnError();

    return consumerNodeId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
