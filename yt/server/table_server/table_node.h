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
    DEFINE_BYVAL_RW_PROPERTY(bool, PreserveSchemaOnWrite);

    // For dynamic tables only.
    typedef std::vector<NTabletServer::TTablet*> TTabletList;
    typedef TTabletList::iterator TTabletListIterator; 
    DEFINE_BYREF_RW_PROPERTY(TTabletList, Tablets);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, LastCommitTimestamp);

    DEFINE_BYVAL_RW_PROPERTY(NTabletServer::TTabletCellBundle*, TabletCellBundle);

    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::EAtomicity, Atomicity);

public:
    explicit TTableNode(const NCypressServer::TVersionedNodeId& id);

    virtual NObjectClient::EObjectType GetObjectType() const;
    TTableNode* GetTrunkNode() const;

    virtual void BeginUpload(NChunkClient::EUpdateMode mode) override;
    virtual void EndUpload(
        const NChunkClient::NProto::TDataStatistics* statistics,
        const NTableClient::TTableSchema& schema,
        bool preserveSchemaOnWrite) override;

    virtual bool IsSorted() const override;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

    std::pair<TTabletListIterator, TTabletListIterator> GetIntersectingTablets(
        const NTableClient::TOwningKey& minKey,
        const NTableClient::TOwningKey& maxKey);

    bool IsDynamic() const;
    bool IsEmpty() const;
    bool HasMountedTablets() const;
    bool IsUniqueKeys() const;
    NTabletClient::ETabletState GetTabletState() const;

    void SetCustomSchema(NTableClient::TTableSchema, bool dynamic);
};

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateTableTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

