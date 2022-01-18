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
    auto replicationCardFuture = client->GetReplicationCard(ReplicationCardId, Options);
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
    auto replicaIdFuture = client->CreateReplicationCardReplica(ReplicationCardId, ReplicaInfo, Options);
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

    auto future = client->RemoveReplicationCardReplica(ReplicationCardId, ReplicaId, Options);
    WaitFor(future)
        .ThrowOnError();
    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TAlterReplicationCardReplicaCommand::TAlterReplicationCardReplicaCommand()
{
    RegisterParameter("replica_id", ReplicaId);
    RegisterParameter("mode", Options.Mode)
        .Optional();
    RegisterParameter("enabled", Options.Enabled)
        .Optional();
    RegisterParameter("table_path", Options.TablePath)
        .Optional();
}

void TAlterReplicationCardReplicaCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();

    auto future = client->AlterReplicationCardReplica(ReplicationCardId, ReplicaId, Options);
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

    auto future = client->UpdateReplicationProgress(ReplicationCardId, ReplicaId, Options);
    WaitFor(future)
        .ThrowOnError();
    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
