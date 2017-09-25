#pragma once

#include "public.h"

#include <yt/server/chunk_server/chunk_owner_base.h>

#include <yt/server/cypress_server/node_detail.h>

namespace NYT {
namespace NJournalServer {

////////////////////////////////////////////////////////////////////////////////

class TJournalNode
    : public NChunkServer::TChunkOwnerBase
{
public:
    using TBase = NChunkServer::TChunkOwnerBase;

    DEFINE_BYVAL_RW_PROPERTY(int, ReadQuorum);
    DEFINE_BYVAL_RW_PROPERTY(int, WriteQuorum);

public:
    explicit TJournalNode(const NCypressServer::TVersionedNodeId& id);

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

    virtual void BeginUpload(NChunkClient::EUpdateMode mode) override;

    NChunkServer::TChunk* GetTrailingChunk() const;

    TJournalNode* GetTrunkNode();
    const TJournalNode* GetTrunkNode() const;

    bool GetSealed() const;
    void SetSealed(bool value);

    virtual NSecurityServer::TClusterResources GetDeltaResourceUsage() const override;
    virtual NSecurityServer::TClusterResources GetTotalResourceUsage() const override;

private:
    // Only maintained at trunk nodes.
    bool Sealed_ = true;

};

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateJournalTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJournalServer
} // namespace NYT

