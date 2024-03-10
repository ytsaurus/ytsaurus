#pragma once

#include "public.h"
#include "schemaful_node.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/chunk_server/chunk_owner_base.h>

#include <yt/yt/server/master/cypress_server/node_detail.h>

#include <yt/yt/server/master/tablet_server/public.h>
#include <yt/yt/server/master/tablet_server/tablet.h>
#include <yt/yt/server/master/tablet_server/tablet_owner_base.h>
#include <yt/yt/server/master/tablet_server/tablet_resources.h>

#include <yt/yt/server/lib/tablet_balancer/config.h>

#include <yt/yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/aggregate_property.h>
#include <yt/yt/core/misc/property.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTableLock
{
    NTransactionClient::TTimestamp Timestamp;
    int PendingTabletCount;

    void Persist(const NCellMaster::TPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

class TTableNode
    : public NTabletServer::TTabletOwnerBase
    , public TSchemafulNode
{
private:
    using TTabletStateIndexedVector = TEnumIndexedArray<
        NTabletClient::ETabletState,
        int,
        NTabletClient::MinValidTabletState,
        NTabletClient::MaxValidTabletState>;
    using TTabletList = std::vector<NTabletServer::TTablet*>;
    using TSecondaryIndexList = std::vector<TSecondaryIndexId>;

    struct TDynamicTableAttributes
    {
        NTransactionClient::EAtomicity Atomicity = NTransactionClient::EAtomicity::Full;
        NTransactionClient::ECommitOrdering CommitOrdering = NTransactionClient::ECommitOrdering::Weak;
        NTabletClient::TTableReplicaId UpstreamReplicaId;
        NTransactionClient::TTimestamp LastCommitTimestamp = NTransactionClient::NullTimestamp;
        std::optional<NHydra::TRevision> ForcedCompactionRevision;
        std::optional<NHydra::TRevision> ForcedStoreCompactionRevision;
        std::optional<NHydra::TRevision> ForcedHunkCompactionRevision;
        std::optional<NHydra::TRevision> ForcedChunkViewCompactionRevision;
        bool Dynamic = false;
        NTabletBalancer::TMasterTableTabletBalancerConfigPtr TabletBalancerConfig = New<NTabletBalancer::TMasterTableTabletBalancerConfig>();
        THashMap<NTransactionClient::TTransactionId, TDynamicTableLock> DynamicTableLocks;
        int UnconfirmedDynamicTableLockCount = 0;
        std::optional<bool> EnableDynamicStoreRead;
        bool MountedWithEnabledDynamicStoreRead = false;
        std::optional<NTabletNode::EDynamicTableProfilingMode> ProfilingMode;
        std::optional<TString> ProfilingTag;
        bool EnableDetailedProfiling = false;
        bool EnableConsistentChunkReplicaPlacement = false;
        NTabletClient::ETableBackupState BackupState = NTabletClient::ETableBackupState::None;
        TEnumIndexedArray<NTabletClient::ETabletBackupState, int> TabletCountByBackupState;
        NTabletClient::ETabletBackupState AggregatedTabletBackupState = NTabletClient::ETabletBackupState::None;
        NTransactionClient::TTimestamp BackupCheckpointTimestamp = NTransactionClient::NullTimestamp;
        NTabletClient::EBackupMode BackupMode = NTabletClient::EBackupMode::None;
        TError BackupError;
        std::vector<NTabletClient::TTableReplicaBackupDescriptor> ReplicaBackupDescriptors;
        std::optional<TString> QueueAgentStage;
        bool TreatAsConsumer = false;
        bool IsVitalConsumer = false;
        NTabletServer::TMountConfigStoragePtr MountConfigStorage;
        NTabletServer::THunkStorageNodePtr HunkStorageNode;
        THashSet<TSecondaryIndex*> SecondaryIndices;
        TSecondaryIndex* IndexTo = nullptr;
        bool EnableSharedWriteLocks = false;

        TDynamicTableAttributes();

        void Save(NCellMaster::TSaveContext& context) const;
        void Load(NCellMaster::TLoadContext& context);

        void CopyFrom(const TDynamicTableAttributes* other);
        void BeginCopy(NCypressServer::TBeginCopyContext* context) const;
        void EndCopy(NCypressServer::TEndCopyContext* context);
    };

public:
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, RetainedTimestamp, NTransactionClient::NullTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, UnflushedTimestamp, NTransactionClient::NullTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(TTableCollocation*, ReplicationCollocation);

    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TTableNode, NTableClient::EOptimizeFor, OptimizeFor);
    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TTableNode, NChunkClient::EChunkFormat, ChunkFormat);
    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TTableNode, NErasure::ECodec, HunkErasureCodec);

    DECLARE_EXTRA_PROPERTY_HOLDER(TDynamicTableAttributes, DynamicTableAttributes);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, Atomicity);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, CommitOrdering);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, UpstreamReplicaId);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, LastCommitTimestamp);
    DEFINE_BYREF_RW_EXTRA_PROPERTY(DynamicTableAttributes, TabletBalancerConfig);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, ForcedCompactionRevision);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, ForcedStoreCompactionRevision);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, ForcedHunkCompactionRevision);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, ForcedChunkViewCompactionRevision);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, Dynamic);
    DEFINE_BYREF_RW_EXTRA_PROPERTY(DynamicTableAttributes, DynamicTableLocks);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, UnconfirmedDynamicTableLockCount);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, EnableDynamicStoreRead);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, MountedWithEnabledDynamicStoreRead);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, ProfilingMode);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, ProfilingTag);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, EnableDetailedProfiling);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, EnableConsistentChunkReplicaPlacement);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, EnableSharedWriteLocks);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, BackupState);
    DEFINE_BYREF_RW_EXTRA_PROPERTY(DynamicTableAttributes, TabletCountByBackupState);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, AggregatedTabletBackupState);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, BackupCheckpointTimestamp);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, BackupMode);
    DEFINE_BYREF_RW_EXTRA_PROPERTY(DynamicTableAttributes, BackupError);
    DEFINE_BYREF_RW_EXTRA_PROPERTY(DynamicTableAttributes, ReplicaBackupDescriptors);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, QueueAgentStage);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, TreatAsConsumer);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, IsVitalConsumer);
    DEFINE_BYREF_RW_EXTRA_PROPERTY(DynamicTableAttributes, SecondaryIndices);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, IndexTo);

    // COMPAT(ifsmirnov)
    DECLARE_BYVAL_RW_PROPERTY(std::optional<bool>, EnableTabletBalancer);
    DECLARE_BYVAL_RW_PROPERTY(std::optional<i64>, MinTabletSize);
    DECLARE_BYVAL_RW_PROPERTY(std::optional<i64>, MaxTabletSize);
    DECLARE_BYVAL_RW_PROPERTY(std::optional<i64>, DesiredTabletSize);
    DECLARE_BYVAL_RW_PROPERTY(std::optional<int>, DesiredTabletCount);

public:
    using TTabletOwnerBase::TTabletOwnerBase;
    explicit TTableNode(NCypressServer::TVersionedNodeId id);

    TTableNode* GetTrunkNode();
    const TTableNode* GetTrunkNode() const;

    // COMPAT(h0pless): remove this when clients will send table schema options during begin upload.
    void ParseCommonUploadContext(const TCommonUploadContext& context) override;

    void BeginUpload(const TBeginUploadContext& context) override;

    // COMPAT(h0pless): remove this when clients will send table schema options during begin upload.
    void EndUpload(const TEndUploadContext& context) override;

    NSecurityServer::TDetailedMasterMemory GetDetailedMasterMemoryUsage() const override;

    bool IsSorted() const override;

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;

    bool IsDynamic() const;
    bool IsQueue() const;
    bool IsTrackedQueueObject() const;
    bool IsConsumer() const;
    bool IsTrackedConsumerObject() const;
    bool IsEmpty() const;
    bool IsLogicallyEmpty() const;
    bool IsUniqueKeys() const;
    bool IsReplicated() const;
    bool IsPhysicallyLog() const;
    bool IsPhysicallySorted() const;

    NChaosClient::TReplicationCardId GetReplicationCardId() const;

    NTransactionClient::TTimestamp GetCurrentRetainedTimestamp() const;
    NTransactionClient::TTimestamp GetCurrentUnflushedTimestamp(
        NTransactionClient::TTimestamp latestTimestamp) const;

    NSecurityServer::TAccount* GetAccount() const override;

    // COMPAT(h0pless): This is a temporary workaround until schemaful node typehandler is introduced.
    NObjectClient::TCellTag GetExternalCellTag() const override;
    bool IsExternal() const override;

    void ValidateNotBackup(TStringBuf message) const;

    void AddDynamicTableLock(
        NTransactionClient::TTransactionId transactionId,
        NTransactionClient::TTimestamp timestamp,
        int pendingTabletCount);
    void ConfirmDynamicTableLock(NTransactionClient::TTransactionId transactionId);
    void RemoveDynamicTableLock(NTransactionClient::TTransactionId transactionId);

    void ValidateMount() const override;
    void ValidateUnmount() const override;
    void ValidateRemount() const override;
    void ValidateFreeze() const override;
    void ValidateUnfreeze() const override;
    void ValidateReshard(
        const NCellMaster::TBootstrap* bootstrap,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount,
        const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys,
        const std::vector<i64>& trimmedRowCounts) const override;

    void CheckInvariants(NCellMaster::TBootstrap* bootstrap) const override;

    const NTabletServer::TMountConfigStorage* FindMountConfigStorage() const;
    NTabletServer::TMountConfigStorage* GetMutableMountConfigStorage();

    void SetHunkStorageNode(NTabletServer::THunkStorageNode* node);
    void ResetHunkStorageNode();
    NTabletServer::THunkStorageNode* GetHunkStorageNode() const;

private:
    NTransactionClient::TTimestamp CalculateRetainedTimestamp() const;
    NTransactionClient::TTimestamp CalculateUnflushedTimestamp(
        NTransactionClient::TTimestamp latestTimestamp) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer

