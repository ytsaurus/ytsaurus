#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/chunk_server/chunk_owner_base.h>

#include <yt/server/cypress_server/node_detail.h>

#include <yt/server/tablet_server/public.h>

#include <yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <yt/client/table_client/schema.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/small_vector.h>

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TTableNode
    : public NChunkServer::TChunkOwnerBase
{
private:
    using TTabletStateIndexedVector = TEnumIndexedVector<
        int,
        NTabletClient::ETabletState,
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
        TNullable<bool> EnableTabletBalancer;
        TNullable<i64> MinTabletSize;
        TNullable<i64> MaxTabletSize;
        TNullable<i64> DesiredTabletSize;
        TNullable<int> DesiredTabletCount;
        int TabletErrorCount = 0;
        TNullable<i64> ForcedCompactionRevision;
        bool Dynamic = false;
        TString MountPath;
        NSecurityServer::TClusterResources ExternalTabletResourceUsage;
        NTabletClient::ETabletState ActualTabletState = NTabletClient::ETabletState::Unmounted;
        NTabletClient::ETabletState ExpectedTabletState = NTabletClient::ETabletState::Unmounted;
        NTransactionClient::TTransactionId LastMountTransactionId;

        TDynamicTableAttributes();
        void Save(NCellMaster::TSaveContext& context) const;
        void Load(NCellMaster::TLoadContext& context);
    };

public:
    DEFINE_BYREF_RW_PROPERTY(TSharedTableSchemaPtr, SharedTableSchema);
    DEFINE_BYVAL_RW_PROPERTY(NTableClient::ETableSchemaMode, SchemaMode, NTableClient::ETableSchemaMode::Weak);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, RetainedTimestamp, NTransactionClient::NullTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, UnflushedTimestamp, NTransactionClient::NullTimestamp);

    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TTableNode, NTableClient::EOptimizeFor, OptimizeFor);

    DECLARE_EXTRA_PROPERTY_HOLDER(TDynamicTableAttributes, DynamicTableAttributes);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, Atomicity);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, CommitOrdering);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, InMemoryMode);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, UpstreamReplicaId);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, TabletCellBundle);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, LastCommitTimestamp);
    DEFINE_BYREF_RW_EXTRA_PROPERTY(DynamicTableAttributes, TabletCountByState);
    DEFINE_BYREF_RW_EXTRA_PROPERTY(DynamicTableAttributes, TabletCountByExpectedState);
    DEFINE_BYREF_RW_EXTRA_PROPERTY(DynamicTableAttributes, Tablets);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, EnableTabletBalancer);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, MinTabletSize);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, MaxTabletSize);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, DesiredTabletSize);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, DesiredTabletCount);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, TabletErrorCount);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, ForcedCompactionRevision);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, Dynamic);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, MountPath);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, ExternalTabletResourceUsage);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, ActualTabletState);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, ExpectedTabletState);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(DynamicTableAttributes, LastMountTransactionId);

public:
    explicit TTableNode(const NCypressServer::TVersionedNodeId& id);

    TTableNode* GetTrunkNode();
    const TTableNode* GetTrunkNode() const;

    virtual void BeginUpload(NChunkClient::EUpdateMode mode) override;
    virtual void EndUpload(
        const NChunkClient::NProto::TDataStatistics* statistics,
        const TSharedTableSchemaPtr& schema,
        NTableClient::ETableSchemaMode schemaMode,
        TNullable<NTableClient::EOptimizeFor> optimizeFor,
        const TNullable<NCrypto::TMD5Hasher>& md5Hasher) override;

    virtual NSecurityServer::TClusterResources GetDeltaResourceUsage() const override;
    virtual NSecurityServer::TClusterResources GetTotalResourceUsage() const override;

    NSecurityServer::TClusterResources GetTabletResourceUsage() const;

    virtual bool IsSorted() const override;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;
    void LoadPre609(NCellMaster::TLoadContext& context);
    void LoadCompatAfter609(NCellMaster::TLoadContext& context);

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

    void ValidateTabletStateFixed(const TString& message) const;
    void ValidateAllTabletsFrozenOrUnmounted(const TString& message) const;
    void ValidateAllTabletsUnmounted(const TString& message) const;

private:
    NTransactionClient::TTimestamp CalculateRetainedTimestamp() const;
    NTransactionClient::TTimestamp CalculateUnflushedTimestamp(
        NTransactionClient::TTimestamp latestTimestamp) const;

    void ValidateExpectedTabletState(const TString& message, bool allowFrozen) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

