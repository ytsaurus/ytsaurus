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
    RegisterParameter("chaos_cell_id", ReplicationCardToken.ChaosCellId);
    RegisterParameter("replication_card_id", ReplicationCardToken.ReplicationCardId);
}

////////////////////////////////////////////////////////////////////////////////

TCreateReplicationCardCommand::TCreateReplicationCardCommand()
{
    RegisterParameter("chaos_cell_id", ReplicationCardToken.ChaosCellId);
}

void TCreateReplicationCardCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto replicationCardIdFuture = client->CreateReplicationCard(ReplicationCardToken, Options);
    auto replicationCardToken = WaitFor(replicationCardIdFuture)
        .ValueOrThrow();
    ProduceSingleOutputValue(context, "replication_card_id", replicationCardToken.ReplicationCardId);
}

////////////////////////////////////////////////////////////////////////////////

TGetReplicationCardCommand::TGetReplicationCardCommand()
{
    RegisterParameter("include_coordinators", Options.IncludeCoordinators)
        .Optional();
    RegisterParameter("include_progress", Options.IncludeProgress)
        .Optional();
    RegisterParameter("include_history", Options.IncludeHistory)
        .Optional();
}

void TGetReplicationCardCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto replicationCardFuture = client->GetReplicationCard(ReplicationCardToken, Options);
    auto replicationCard = WaitFor(replicationCardFuture)
        .ValueOrThrow();

    ProduceOutput(context, [&] (IYsonConsumer* consumer) {
        Serialize(*replicationCard, consumer);
    });
}

////////////////////////////////////////////////////////////////////////////////

TCreateReplicationCardReplicaCommand::TCreateReplicationCardReplicaCommand()
{
    RegisterParameter("replica_info", ReplicaInfo);
}

void TCreateReplicationCardReplicaCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto replicaIdFuture = client->CreateReplicationCardReplica(ReplicationCardToken, ReplicaInfo, Options);
    auto replicaId = WaitFor(replicaIdFuture)
        .ValueOrThrow();
    ProduceSingleOutputValue(context, "replica_id", replicaId);
}

////////////////////////////////////////////////////////////////////////////////

TRemoveReplicationCardReplicaCommand::TRemoveReplicationCardReplicaCommand()
{
    RegisterParameter("replica_id", ReplicaId);
}

void TRemoveReplicationCardReplicaCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();

    auto future = client->RemoveReplicationCardReplica(ReplicationCardToken, ReplicaId, Options);
    WaitFor(future)
        .ThrowOnError();
    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TAlterReplicationCardReplicaCommand::TAlterReplicationCardReplicaCommand()
{
    RegisterParameter("replica_id", ReplicaId);
    RegisterParameter("mode", Options.Mode);
    RegisterParameter("enabled", Options.Enabled);
    RegisterParameter("table_path", Options.TablePath);
}

void TAlterReplicationCardReplicaCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();

    auto future = client->AlterReplicationCardReplica(ReplicationCardToken, ReplicaId, Options);
    WaitFor(future)
        .ThrowOnError();
    ProduceEmptyOutput(context);
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

    auto future = client->UpdateReplicationProgress(ReplicationCardToken, ReplicaId, Options);
    WaitFor(future)
        .ThrowOnError();
    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
