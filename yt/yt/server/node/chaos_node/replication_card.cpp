#include "replication_card.h"

#include "serialize.h"

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/core/misc/format.h>

namespace NYT::NChaosNode {

using namespace NChaosClient;

////////////////////////////////////////////////////////////////////////////////

void TCoordinatorInfo::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, State);
}

////////////////////////////////////////////////////////////////////////////////

TReplicaInfo* TReplicationCard::FindReplica(TReplicaId replicaId)
{
    auto it = Replicas_.find(replicaId);
    return it == Replicas_.end() ? nullptr : &it->second;
}

TReplicaInfo* TReplicationCard::GetReplicaOrThrow(TReplicaId replicaId)
{
    auto* replicaInfo = FindReplica(replicaId);
    if (!replicaInfo) {
        THROW_ERROR_EXCEPTION("No such replica %v", replicaId);
    }
    return replicaInfo;
}

void TReplicationCard::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, Replicas_);
    Save(context, CurrentReplicaIdIndex_);
    Save(context, Coordinators_);
    Save(context, Era_);
    Save(context, TableId_);
    Save(context, TablePath_);
    Save(context, TableClusterName_);
    Save(context, CurrentTimestamp_);
}

void TReplicationCard::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, Replicas_);
    Load(context, CurrentReplicaIdIndex_);
    Load(context, Coordinators_);
    Load(context, Era_);
    Load(context, TableId_);
    Load(context, TablePath_);
    Load(context, TableClusterName_);
    // COMPAT(savrus)
    if (context.GetVersion() >= EChaosReign::CurrentTimestamp) {
        Load(context, CurrentTimestamp_);
    }
}

void FormatValue(TStringBuilderBase* builder, const TReplicationCard& replicationCard, TStringBuf /*spec*/)
{
    builder->AppendFormat("{Id: %v, Replicas: %v, Era: %v, TableId: %v, TablePath: %v, TableClusterName: %v, CurrentTimestamp: %llx}",
        replicationCard.GetId(),
        replicationCard.Replicas(),
        replicationCard.GetEra(),
        replicationCard.GetTableId(),
        replicationCard.GetTablePath(),
        replicationCard.GetTableClusterName(),
        replicationCard.GetCurrentTimestamp());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
