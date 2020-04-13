#pragma once

#include "public.h"
#include "config.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/chunk_server/chunk_owner_base.h>

#include <yt/server/master/cypress_server/node_detail.h>

#include <yt/server/master/tablet_server/public.h>

#include <yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <yt/client/table_client/schema.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/small_vector.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTableLock
{
    NTransactionClient::TTimestamp Timestamp;
    int PendingTabletCount;

    void Persist(NCellMaster::TPersistenceContext& context);
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
        NTabletServer::TTabletCellBundle* TabletCellBundle = nullptr;
        NTransactionClient::TTimestamp LastCommitTimestamp = NTransactionClient::NullTimestamp;
        TTabletStateIndexedVector TabletCountByState;
        TTabletStateIndexedVector TabletCountByExpectedState;
        TTabletList Tablets;
        i64 TabletMasterMemoryUsage = 0;
        int TabletErrorCount = 0;
        std::optional<NHydra::TRevision> ForcedCompactionRevision;
        bool Dynamic = false;
        TString MountPath;
        NSecurityServer::TClusterResources ExternalTabletResourceUsage;
        NTabletClient::ETabletState ActualTabletState = NTabletClient::ETabletState::Unmounted;
        NTabletClient::ETabletState ExpectedTabletState = NTabletClient::ETabletState::Unmounted;
        NTransactionClient::TTransactionId LastMountTransactionId;
        NTransactionClient::TTransactionId PrimaryLastMountTransactionId;
        NTransactionClient::TTransactionId CurrentMountTransactionId;
        TTabletBalancerConfigPtr TabletBalancerConfig;
        THashMap<NTransactionClient::TTransactionId, TDynamicTableLock> DynamicTableLocks;
        int UnconfirmedDynamicTableLockCount = 0;

        TDynamicTableAttributes();
        void Save(NCellMaster::TSaveContext& context) const;
        void Load(NCellMaster::TLoadContext& context);
    };

public:
    DEFINE_BYREF_RW_PROPERTY(TSharedTableSchemaPtr, SharedTableSchema);
    DEFINE_BYVAL_RW_PROPERTY(NTableClient::ETableSchemaMode, SchemaMode, NTableClient::ETableSchemaMode::Weak);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, RetainedTimestamp, NTransactionClient::NullTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, UnflushedTimestamp, NTransactionClient::NullTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(NTabletServer::TTabletCellBundle*, TabletCellBundle);

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

    // COMPAT(ifsmirnov)
    DECLARE_BYVAL_RW_PROPERTY(std::optional<bool>, EnableTabletBalancer);
    DECLARE_BYVAL_RW_PROPERTY(std::optional<i64>, MinTabletSize);
    DECLARE_BYVAL_RW_PROPERTY(std::optional<i64>, MaxTabletSize);
    DECLARE_BYVAL_RW_PROPERTY(std::optional<i64>, DesiredTabletSize);
    DECLARE_BYVAL_RW_PROPERTY(std::optional<int>, DesiredTabletCount);

public:
    explicit TTableNode(const NCypressServer::TVersionedNodeId& id);

    TTableNode* GetTrunkNode();
    const TTableNode* GetTrunkNode() const;

    virtual void EndUpload(const TEndUploadContext& context) override;

    virtual NSecurityServer::TClusterResources GetDeltaResourceUsage() const override;
    virtual NSecurityServer::TClusterResources GetTotalResourceUsage() const override;

    NSecurityServer::TClusterResources GetTabletResourceUsage() const;
    virtual i64 GetMasterMemoryUsage() const override;
    void RecomputeTabletMasterMemoryUsage();

    virtual bool IsSorted() const override;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

    void SaveTableSchema(NCellMaster::TSaveContext& context) const;
    void LoadTableSchema(NCellMaster::TLoadContext& context);

    typedef TTabletList::const_iterator TTabletListIterator;
    std::pair<TTabletListIterator, TTabletListIterator> GetIntersectingTablets(
        const NTableClient::TOwningKey& minKey,
        const NTableClient::TOwningKey& maxKey);

    bool IsDynamic() const;
    bool IsEmpty() const;
    bool IsUniqueKeys() const;
    bool IsReplicated() const;
    bool IsPhysicallySorted() const;

    NTabletClient::ETabletState GetTabletState() const;

    NTabletClient::ETabletState ComputeActualTabletState() const;

    NTransactionClient::TTimestamp GetCurrentRetainedTimestamp() const;
    NTransactionClient::TTimestamp GetCurrentUnflushedTimestamp(
        NTransactionClient::TTimestamp latestTimestamp) const;

    const NTableClient::TTableSchema& GetTableSchema() const;

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

