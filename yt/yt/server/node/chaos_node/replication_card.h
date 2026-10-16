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

////////////////////////////////////////////////////////////////////////////////

// Until pending changes are committed on era change, they are not visible to proxies.
// For example, proxies do not see indices in PendingCreation state at all.
DEFINE_ENUM(ESecondaryIndexTransitionState,
    ((PendingCreation)                  (0))
    ((PendingRemoval)                   (1))
    ((PendingCorrespondenceChange)      (2))
);

// Only used for internal bookkeeping between chaos cells.
struct TSecondaryIndexPendingTransition
    : public NYTree::TYsonStruct
{
    ESecondaryIndexTransitionState State;
    NChaosClient::TReplicationCardId IndexReplicationCardId;
    std::optional<NTabletClient::ETableToIndexCorrespondence> NewCorrespondence;

    REGISTER_YSON_STRUCT(TSecondaryIndexPendingTransition);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSecondaryIndexPendingTransition)

////////////////////////////////////////////////////////////////////////////////

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

    bool IsNormalState() const override;

    using TSecondaryIndices = std::vector<NTabletClient::TIndexInfo>;
    DEFINE_BYREF_RW_PROPERTY(TSecondaryIndices, SecondaryIndices);
    DEFINE_BYREF_RW_PROPERTY(NChaosClient::TReplicationCardId, IndexTo);
    DEFINE_BYREF_RW_PROPERTY(TSecondaryIndexPendingTransitionPtr, SecondaryIndexPendingTransition);

    NChaosClient::TReplicaInfo* FindReplica(NChaosClient::TReplicaId replicaId);
    NChaosClient::TReplicaInfo* GetReplicaOrThrow(NChaosClient::TReplicaId replicaId);

    TReplicationCard::TSecondaryIndices::iterator FindSecondaryIndex(NChaosClient::TReplicationCardId indexCardId);

public:
    TReplicationCard(NObjectClient::TObjectId id);

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    bool IsReadyToMigrate() const;
    bool IsMigrated() const;
    bool IsCollocationMigrating() const;
    void ValidateCollocationNotMigrating() const;
    void ValidateNoPendingSecondaryIndexChanges() const;
    NChaosClient::TReplicationCardPtr ConvertToClientCard(const NChaosClient::TReplicationCardFetchOptions& options);
};

void FormatValue(TStringBuilderBase* builder, const TReplicationCard& replicationCard, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
