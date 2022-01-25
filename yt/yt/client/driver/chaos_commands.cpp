#include "chaos_commands.h"

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

namespace NYT::NDriver {

using namespace NApi;
using namespace NChaosClient;
using namespace NConcurrency;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TReplicationCardCommandBase::TReplicationCardCommandBase()
{
    RegisterParameter("replication_card_id", ReplicationCardId);
}

////////////////////////////////////////////////////////////////////////////////

TCreateReplicationCardCommand::TCreateReplicationCardCommand()
{
    RegisterParameter("chaos_cell_id", ChaosCellId);
}

void TCreateReplicationCardCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto replicationCardIdFuture = client->CreateReplicationCard(ChaosCellId, Options);
    auto replicationCardId = WaitFor(replicationCardIdFuture)
        .ValueOrThrow();
    ProduceSingleOutputValue(context, "replication_card_id", replicationCardId);
}

////////////////////////////////////////////////////////////////////////////////

TUpdateReplicationProgressCommand::TUpdateReplicationProgressCommand()
{
    RegisterParameter("replica_id", ReplicaId);
    RegisterParameter("progress", Options.Progress);
}

void TUpdateReplicationProgressCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();

    auto future = client->UpdateReplicationProgress(ReplicationCardId, ReplicaId, Options);
    WaitFor(future)
        .ThrowOnError();
    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
