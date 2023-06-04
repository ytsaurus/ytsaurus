#pragma once

#include "public.h"

#include <yt/yt/server/node/tablet_node/object_detail.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/core/misc/ref_tracked.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EShortcutState,
    ((Granting)     (0))
    ((Granted)      (1))
    ((Revoking)     (2))
    ((Revoked)      (3))
);

DEFINE_ENUM(EReplicationCardState,
    ((Normal)                        (0))
    ((RevokingShortcutsForAlter)     (1))
    ((GeneratingTimestampForNewEra)  (2))
    ((RevokingShortcutsForMigration) (3))
    ((Migrated)                      (4))
);

struct TCoordinatorInfo
{
    EShortcutState State;

    void Persist(const TPersistenceContext& context);
};

struct TMigration
{
    NObjectClient::TCellId OriginCellId;
    NObjectClient::TCellId ImmigratedToCellId;
    NObjectClient::TCellId EmigratedFromCellId;
    TInstant ImmigrationTime;
    TInstant EmigrationTime;

    void Persist(const TPersistenceContext& context);
};

class TReplicationCard
    : public NTabletNode::TObjectBase
    , public TRefTracked<TReplicationCard>
{
public:
    using TReplicas = THashMap<NChaosClient::TReplicaId, NChaosClient::TReplicaInfo>;
    DEFINE_BYREF_RW_PROPERTY(TReplicas, Replicas);

    DEFINE_BYVAL_RW_PROPERTY(NChaosClient::TReplicaIdIndex, CurrentReplicaIdIndex);

    using TCoordinators = THashMap<NObjectClient::TCellId, TCoordinatorInfo>;
    DEFINE_BYREF_RW_PROPERTY(TCoordinators, Coordinators);

    DEFINE_BYVAL_RW_PROPERTY(NChaosClient::TReplicationEra, Era, NChaosClient::InitialReplicationEra);
    DEFINE_BYVAL_RW_PROPERTY(NTableClient::TTableId, TableId);
    DEFINE_BYVAL_RW_PROPERTY(NYPath::TYPath, TablePath);
    DEFINE_BYVAL_RW_PROPERTY(TString, TableClusterName);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, CurrentTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(NTabletClient::TReplicatedTableOptionsPtr, ReplicatedTableOptions);

    DEFINE_BYREF_RW_PROPERTY(TMigration, Migration);
    DEFINE_BYVAL_RW_PROPERTY(EReplicationCardState, State);

    DEFINE_BYVAL_RW_PROPERTY(TReplicationCardCollocation*, Collocation);

    NChaosClient::TReplicaInfo* FindReplica(NChaosClient::TReplicaId replicaId);
    NChaosClient::TReplicaInfo* GetReplicaOrThrow(NChaosClient::TReplicaId replicaId);

public:
    TReplicationCard(NObjectClient::TObjectId id);

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    bool IsMigrated() const;
    bool IsCollocationMigrating() const;
    void ValidateCollocationNotMigrating() const;
};

void FormatValue(TStringBuilderBase* builder, const TReplicationCard& replicationCard, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode

