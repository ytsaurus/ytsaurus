#include "chaos_commands.h"

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

namespace NYT::NDriver {

using namespace NApi;
using namespace NChaosClient;
using namespace NConcurrency;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TUpdateChaosTableReplicaProgressCommand::TUpdateChaosTableReplicaProgressCommand()
{
    RegisterParameter("replica_id", ReplicaId);
    RegisterParameter("progress", Options.Progress);
}

void TUpdateChaosTableReplicaProgressCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();

    auto future = client->UpdateChaosTableReplicaProgress(ReplicaId, Options);
    WaitFor(future)
        .ThrowOnError();
    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
