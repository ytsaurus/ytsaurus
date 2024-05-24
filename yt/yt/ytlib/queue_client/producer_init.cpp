#include "producer_init.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/queue_client/records/queue_producer_session.record.h>

namespace NYT::NQueueClient {

using namespace NApi;
using namespace NYTree;
using namespace NTransactionClient;
using namespace NCypressClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TNodeId CreateProducerNode(
    const NApi::IClientPtr& client,
    const NYPath::TYPath& path,
    const NApi::TCreateNodeOptions& options)
{
    auto transaction = [&] {
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Create producer %v", path));
        TTransactionStartOptions startOptions{
            .ParentId = options.TransactionId,
            .Attributes = std::move(attributes),
        };
        return WaitFor(client->StartTransaction(ETransactionType::Master, startOptions))
            .ValueOrThrow();
    }();

    auto producerNodeId = [&] {
        auto attributes = options.Attributes ? options.Attributes->Clone() : CreateEphemeralAttributes();

        attributes->Set("dynamic", true);
        attributes->Set("schema", NRecords::TQueueProducerSessionDescriptor::Get()->GetSchema());

        auto createNodeOptions = options;
        createNodeOptions.Attributes = std::move(attributes);
        return WaitFor(transaction->CreateNode(path, EObjectType::Table, createNodeOptions))
            .ValueOrThrow();
    }();

    WaitFor(transaction->Commit())
        .ThrowOnError();

    WaitFor(client->MountTable(path))
        .ThrowOnError();

    return producerNodeId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
