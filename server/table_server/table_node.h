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
public:
    DEFINE_BYREF_RW_PROPERTY(NTableClient::TTableSchema, TableSchema);
    DEFINE_BYVAL_RW_PROPERTY(NTableClient::ETableSchemaMode, SchemaMode);

    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, LastCommitTimestamp);

    DEFINE_BYVAL_RW_PROPERTY(NTabletServer::TTabletCellBundle*, TabletCellBundle);

    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::EAtomicity, Atomicity);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::ECommitOrdering, CommitOrdering);

    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, RetainedTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, UnflushedTimestamp);

    DEFINE_BYVAL_RW_PROPERTY(NTableClient::ETableReplicationMode, ReplicationMode, NTableClient::ETableReplicationMode::None);

public:
    explicit TTableNode(const NCypressServer::TVersionedNodeId& id);

    virtual NObjectClient::EObjectType GetObjectType() const;

    TTableNode* GetTrunkNode();
    const TTableNode* GetTrunkNode() const;

    virtual void BeginUpload(NChunkClient::EUpdateMode mode) override;
    virtual void EndUpload(
        const NChunkClient::NProto::TDataStatistics* statistics,
        const NTableClient::TTableSchema& schema,
        NTableClient::ETableSchemaMode schemaMode) override;

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

