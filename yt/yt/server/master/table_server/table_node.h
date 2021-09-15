#pragma once

#include "public.h"
#include "config.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/chunk_server/chunk_owner_base.h>

#include <yt/yt/server/master/cypress_server/node_detail.h>

#include <yt/yt/server/master/tablet_server/public.h>
#include <yt/yt/server/master/tablet_server/tablet.h>
#include <yt/yt/server/master/tablet_server/tablet_resources.h>

#include <yt/yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/core/misc/aggregate_property.h>
#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/small_vector.h>

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
    : public NChunkServer::TChunkOwnerBase
{
private:
    using TTabletStateIndexedVector = TEnumIndexedVector<
        NTabletClient::ETabletState,
        int,
        NTabletClient::MinValidTabletState,
        NTabletClient::MaxValidTabletState>;
    using TTabletList = std::vector<NTabletServer::TTablet*>;

    struct TDynamicTableAttributes
    {
        NTransactionClient::EAtomicity Atomicity = NTransactionClient::EAtomicity::Full;
        NTransactionClient::ECommitOrdering CommitOrdering = NTransactionClient::ECommitOrdering::Weak;
        NTabletClient::EInMemoryMode InMemoryMode = NTabletClient::EInMemoryMode::None;
        NTabletClient::TTableReplicaId UpstreamReplicaId;
        NTransactionClient::TTimestamp LastCommitTimestamp = NTransactionClient::NullTimestamp;
        TTabletStateIndexedVector TabletCountByState;
        TTabletStateIndexedVector TabletCountByExpectedState;
        TTabletList Tablets;
        i64 TabletMasterMemoryUsage = 0;
        int TabletErrorCount = 0;
        std::optional<NHydra::TRevision> ForcedCompactionRevision;
        std::optional<NHydra::TRevision> ForcedStoreCompactionRevision;
        std::optional<NHydra::TRevision> ForcedHunkCompactionRevision;
        bool Dynamic = false;
        TString MountPath;
        NTabletServer::TTabletResources ExternalTabletResourceUsage;
        NTabletClient::ETabletState ActualTabletState = NTabletClient::ETabletState::Unmounted;
        NTabletClient::ETabletState ExpectedTabletState = NTabletClient::ETabletState::Unmounted;
        NTransactionClient::TTransactionId LastMountTransactionId;
        NTransactionClient::TTransactionId PrimaryLastMountTransactionId;
        NTransactionClient::TTransactionId CurrentMountTransactionId;
        TTabletBalancerConfigPtr TabletBalancerConfig;
        THashMap<NTransactionClient::TTransactionId, TDynamicTableLock> DynamicTableLocks;
        int UnconfirmedDynamicTableLockCount = 0;
        std::optional<bool> EnableDynamicStoreRead;
        bool MountedWithEnabledDynamicStoreRead = false;
        NTabletServer::TTabletStatisticsAggregate TabletStatistics;
        // If ProfilingMode is nullopt, cluster-wise attribute will be used.
        std::optional<NTabletNode::EDynamicTableProfilingMode> ProfilingMode;
        std::optional<TString> ProfilingTag;
        bool EnableDetailedProfiling = false;

        TDynamicTableAttributes();
        void Save(NCellMaster::TSaveContext& context) const;
        void Load(NCellMaster::TLoadContext& context);

        void CopyFrom(const TDynamicTableAttributes* other);
        void BeginCopy(NCypressServer::TBeginCopyContext* context) const;
        void EndCopy(NCypressServer::TEndCopyContext* context);
    };

public:
    TMasterTableSchema* Schema_ = nullptr;

    DEFINE_BYVAL_RW_PROPERTY(NTableClient::ETableSchemaMode, SchemaMode, NTableClient::ETableSchemaMode::Weak);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, RetainedTimestamp, NTransactionClient::NullTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, UnflushedTimestamp, NTransactionClient::NullTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(NTabletServer::TTabletCellBundle*, TabletCellBundle);
    DEFINE_BYVAL_RW_PROPERTY(TTableCollocation*, ReplicationCollocation);

    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TTableNode, NTableClient::EOptimizeFor, OptimizeFor);

    DECLARE_EXTRA_PROPERTY_HOLDER(TDynamicTableAttributes, DynamicTableAttributes);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, Atomicity);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, CommitOrdering);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, InMemoryMode);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, UpstreamReplicaId);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, LastCommitTimestamp);
    DEFINE_BYREF_RW_EXTRA_PROPERTY(DynamicTableAttributes, TabletCountByState);
    DEFINE_BYREF_RW_EXTRA_PROPERTY(DynamicTableAttributes, TabletCountByExpectedState);
    DEFINE_BYREF_RW_EXTRA_PROPERTY(DynamicTableAttributes, Tablets);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, TabletMasterMemoryUsage);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, TabletErrorCount);
    DEFINE_BYREF_RW_EXTRA_PROPERTY(DynamicTableAttributes, TabletBalancerConfig);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, ForcedCompactionRevision);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, ForcedStoreCompactionRevision);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, ForcedHunkCompactionRevision);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, Dynamic);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, MountPath);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, ExternalTabletResourceUsage);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, ActualTabletState);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, ExpectedTabletState);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, LastMountTransactionId);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, PrimaryLastMountTransactionId);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, CurrentMountTransactionId);
    DEFINE_BYREF_RW_EXTRA_PROPERTY(DynamicTableAttributes, DynamicTableLocks);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, UnconfirmedDynamicTableLockCount);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, EnableDynamicStoreRead);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, MountedWithEnabledDynamicStoreRead);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, ProfilingMode);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, ProfilingTag);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, EnableDetailedProfiling);
    DEFINE_BYVAL_EXTRA_AGGREGATE_PROPERTY(DynamicTableAttributes, TabletStatistics);

    // COMPAT(ifsmirnov)
    DECLARE_BYVAL_RW_PROPERTY(std::optional<bool>, EnableTabletBalancer);
    DECLARE_BYVAL_RW_PROPERTY(std::optional<i64>, MinTabletSize);
    DECLARE_BYVAL_RW_PROPERTY(std::optional<i64>, MaxTabletSize);
    DECLARE_BYVAL_RW_PROPERTY(std::optional<i64>, DesiredTabletSize);
    DECLARE_BYVAL_RW_PROPERTY(std::optional<int>, DesiredTabletCount);

public:
    explicit TTableNode(NCypressServer::TVersionedNodeId id);

    TTableNode* GetTrunkNode();
    const TTableNode* GetTrunkNode() const;

    virtual void EndUpload(const TEndUploadContext& context) override;

    virtual NSecurityServer::TClusterResources GetDeltaResourceUsage() const override;
    virtual NSecurityServer::TClusterResources GetTotalResourceUsage() const override;

    virtual NSecurityServer::TDetailedMasterMemory GetDetailedMasterMemoryUsage() const override;

    virtual NTabletServer::TTabletResources GetTabletResourceUsage() const override;
    void RecomputeTabletMasterMemoryUsage();

    virtual bool IsSorted() const override;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

    void SaveTableSchema(NCellMaster::TSaveContext& context) const;
    void LoadTableSchema(NCellMaster::TLoadContext& context);

    using TTabletListIterator = TTabletList::const_iterator;
    std::pair<TTabletListIterator, TTabletListIterator> GetIntersectingTablets(
        const NTableClient::TLegacyOwningKey& minKey,
        const NTableClient::TLegacyOwningKey& maxKey);

    bool IsDynamic() const;
    bool IsEmpty() const;
    bool IsLogicallyEmpty() const;
    bool IsUniqueKeys() const;
    bool IsReplicated() const;
    bool IsPhysicallySorted() const;

    NTabletClient::ETabletState GetTabletState() const;

    NTabletClient::ETabletState ComputeActualTabletState() const;

    NTransactionClient::TTimestamp GetCurrentRetainedTimestamp() const;
    NTransactionClient::TTimestamp GetCurrentUnflushedTimestamp(
        NTransactionClient::TTimestamp latestTimestamp) const;

    TMasterTableSchema* GetSchema() const;
    void SetSchema(TMasterTableSchema* schema);

    void UpdateExpectedTabletState(NTabletClient::ETabletState state);

    void LockCurrentMountTransaction(NTransactionClient::TTransactionId transactionId);
    void UnlockCurrentMountTransaction(NTransactionClient::TTransactionId transactionId);

    void ValidateNoCurrentMountTransaction(TStringBuf message) const;
    void ValidateTabletStateFixed(TStringBuf message) const;
    void ValidateAllTabletsFrozenOrUnmounted(TStringBuf message) const;
    void ValidateAllTabletsUnmounted(TStringBuf message) const;

    void AddDynamicTableLock(
        NTransactionClient::TTransactionId transactionId,
        NTransactionClient::TTimestamp timestamp,
        int pendingTabletCount);
    void ConfirmDynamicTableLock(NTransactionClient::TTransactionId transactionId);
    void RemoveDynamicTableLock(NTransactionClient::TTransactionId transactionId);

private:
    NTransactionClient::TTimestamp CalculateRetainedTimestamp() const;
    NTransactionClient::TTimestamp CalculateUnflushedTimestamp(
        NTransactionClient::TTimestamp latestTimestamp) const;

    void ValidateExpectedTabletState(TStringBuf message, bool allowFrozen) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer

