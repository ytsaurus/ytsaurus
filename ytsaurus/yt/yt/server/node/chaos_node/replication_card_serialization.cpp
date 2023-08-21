#include "serialize.h"

#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/client/tablet_client/config.h>

namespace NYT {

using namespace NChaosClient;
using namespace NChaosNode;

////////////////////////////////////////////////////////////////////////////////

template <>
void Save(NChaosNode::TSaveContext& context, const TReplicaInfo& replicaInfo)
{
    using NYT::Save;

    Save(context, replicaInfo.ClusterName);
    Save(context, replicaInfo.ReplicaPath);
    Save(context, replicaInfo.ContentType);
    Save(context, replicaInfo.Mode);
    Save(context, replicaInfo.State);
    Save(context, replicaInfo.History);
    Save(context, replicaInfo.ReplicationProgress);
    Save(context, replicaInfo.EnableReplicatedTableTracker);

}

template <>
void Load<TReplicaInfo, TLoadContext>(
    TLoadContext& context,
    TReplicaInfo& replicaInfo)
{
    using NYT::Load;

    Load(context, replicaInfo.ClusterName);
    Load(context, replicaInfo.ReplicaPath);
    Load(context, replicaInfo.ContentType);
    Load(context, replicaInfo.Mode);
    Load(context, replicaInfo.State);
    Load(context, replicaInfo.History);
    Load(context, replicaInfo.ReplicationProgress);
    // COMPAT(savrus)
    if (context.GetVersion() >= NChaosNode::EChaosReign::ReplicatedTableOptions) {
        Load(context, replicaInfo.EnableReplicatedTableTracker);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
