#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/chunk_server/chunk_owner_base.h>

#include <yt/server/cypress_server/node_detail.h>

#include <yt/server/tablet_server/public.h>

#include <yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <yt/ytlib/table_client/schema.h>

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
    using TTabletStateIndexedVector = TEnumIndexedVector<int, NTabletClient::ETabletState>;

public:
    DEFINE_BYREF_RW_PROPERTY(NTableClient::TTableSchema, TableSchema);
    DEFINE_BYVAL_RW_PROPERTY(NTableClient::ETableSchemaMode, SchemaMode, NTableClient::ETableSchemaMode::Weak);

    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, LastCommitTimestamp, NTransactionClient::NullTimestamp);

    DEFINE_BYVAL_RW_PROPERTY(NTabletServer::TTabletCellBundle*, TabletCellBundle);

    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::EAtomicity, Atomicity, NTransactionClient::EAtomicity::Full);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::ECommitOrdering, CommitOrdering, NTransactionClient::ECommitOrdering::Weak);
    DEFINE_BYVAL_RW_PROPERTY(NTabletClient::TTableReplicaId, UpstreamReplicaId);

    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, RetainedTimestamp, NTransactionClient::NullTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, UnflushedTimestamp, NTransactionClient::NullTimestamp);

    DEFINE_BYREF_RW_PROPERTY(TTabletStateIndexedVector, TabletCountByState);

    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TTableNode, NTableClient::EOptimizeFor, OptimizeFor);

public:
    explicit TTableNode(const NCypressServer::TVersionedNodeId& id);

    virtual NObjectClient::EObjectType GetObjectType() const;

    TTableNode* GetTrunkNode();
    const TTableNode* GetTrunkNode() const;

    virtual void BeginUpload(NChunkClient::EUpdateMode mode) override;
    virtual void EndUpload(
        const NChunkClient::NProto::TDataStatistics* statistics,
        const NTableClient::TTableSchema& schema,
        NTableClient::ETableSchemaMode schemaMode,
        TNullable<NTableClient::EOptimizeFor> optimizeFor) override;

    virtual bool IsSorted() const override;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

    typedef std::vector<NTabletServer::TTablet*> TTabletList;
    typedef TTabletList::iterator TTabletListIterator;
    std::pair<TTabletListIterator, TTabletListIterator> GetIntersectingTablets(
        const NTableClient::TOwningKey& minKey,
        const NTableClient::TOwningKey& maxKey);

    bool IsDynamic() const;
    bool IsEmpty() const;
    bool IsUniqueKeys() const;
    bool IsReplicated() const;
    bool IsPhysicallySorted() const;

    NTabletClient::ETabletState GetTabletState() const;

    NTransactionClient::TTimestamp GetCurrentRetainedTimestamp() const;
    NTransactionClient::TTimestamp GetCurrentUnflushedTimestamp() const;

    // For dynamic trunk tables only.
    const TTabletList& Tablets() const;
    TTabletList& Tablets();

private:
    NTransactionClient::TTimestamp CalculateRetainedTimestamp() const;
    NTransactionClient::TTimestamp CalculateUnflushedTimestamp() const;

    TTabletList Tablets_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

