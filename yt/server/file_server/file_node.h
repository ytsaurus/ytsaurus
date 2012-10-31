#pragma once

#include <server/cell_master/public.h>
#include <ytlib/misc/property.h>
#include <server/cypress_server/node_detail.h>

namespace NYT {
namespace NFileServer {

////////////////////////////////////////////////////////////////////////////////

class TFileNode
    : public NCypressServer::TCypressNodeBase
{
    DEFINE_BYVAL_RW_PROPERTY(NChunkServer::TChunkList*, ChunkList);
    DEFINE_BYVAL_RW_PROPERTY(int, ReplicationFactor);

public:
    explicit TFileNode(const NCypressServer::TVersionedNodeId& id);

    virtual int GetOwningReplicationFactor() const override;

    virtual NObjectClient::EObjectType GetObjectType() const override;

    virtual void Save(const NCellMaster::TSaveContext& context) const override;
    virtual void Load(const NCellMaster::TLoadContext& context) override;
};

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateFileTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

