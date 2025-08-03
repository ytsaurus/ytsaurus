#pragma once

#include "chaos_object_base.h"
#include "public.h"

#include <yt/yt/server/node/tablet_node/object_detail.h>

#include <yt/yt/client/chaos_client/public.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EReplicationCardState,
    ((Normal)                           (0))
    ((RevokingShortcutsForAlter)        (1))
    ((GeneratingTimestampForNewEra)     (2))
    ((RevokingShortcutsForMigration)    (3))
    ((Migrated)                         (4))
    ((AwaitingMigrationConfirmation)    (5))
    ((RemoteCollocationAttachPrepared)  (6))
);

struct TReplicaCounters
{
    explicit TReplicaCounters(const NProfiling::TProfiler& profiler);

    const NProfiling::TTimeGauge LagTime;
};

class TReplicationCard
    : public TChaosObjectBase
    , public TRefTracked<TReplicationCard>
{
public:
    using TReplicas = THashMap<NChaosClient::TReplicaId, NChaosClient::TReplicaInfo>;
    DEFINE_BYREF_RW_PROPERTY(TReplicas, Replicas);

    DEFINE_BYVAL_RW_PROPERTY(NChaosClient::TReplicaIdIndex, CurrentReplicaIdIndex);

    DEFINE_BYVAL_RW_PROPERTY(NTableClient::TTableId, TableId);
    DEFINE_BYVAL_RW_PROPERTY(NYPath::TYPath, TablePath);
    DEFINE_BYVAL_RW_PROPERTY(std::string, TableClusterName);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, CurrentTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(NTabletClient::TReplicatedTableOptionsPtr, ReplicatedTableOptions);

    DEFINE_BYVAL_RW_PROPERTY(EReplicationCardState, State);

    DEFINE_BYVAL_RW_PROPERTY(TReplicationCardCollocation*, Collocation);
    DEFINE_BYVAL_RW_PROPERTY(TReplicationCardCollocationId, AwaitingCollocationId);

    NChaosClient::TReplicaInfo* FindReplica(NChaosClient::TReplicaId replicaId);
    NChaosClient::TReplicaInfo* GetReplicaOrThrow(NChaosClient::TReplicaId replicaId);

public:
    TReplicationCard(NObjectClient::TObjectId id);

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    bool IsReadyToMigrate() const;
    bool IsMigrated() const;
    bool IsCollocationMigrating() const;
    void ValidateCollocationNotMigrating() const;
    NChaosClient::TReplicationCardPtr ConvertToClientCard(const NChaosClient::TReplicationCardFetchOptions& options);
};

void FormatValue(TStringBuilderBase* builder, const TReplicationCard& replicationCard, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode

