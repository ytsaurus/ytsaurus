#pragma once

#include "public.h"

#include <server/chunk_server/chunk_owner_base.h>

#include <server/cypress_server/node_detail.h>

namespace NYT {
namespace NJournalServer {

////////////////////////////////////////////////////////////////////////////////

class TJournalNode
    : public NChunkServer::TChunkOwnerBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(int, ReadQuorum);
    DEFINE_BYVAL_RW_PROPERTY(int, WriteQuorum);

public:
    static const int DefaultReadQuorum = 2;
    static const int DefaultWriteQuorum = 2;

    explicit TJournalNode(const NCypressServer::TVersionedNodeId& id);

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

    NChunkServer::TChunk* GetTrailingChunk() const;
    bool IsSealed() const;

    TJournalNode* GetTrunkNode();

};

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateJournalTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJournalServer
} // namespace NYT

