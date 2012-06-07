#pragma once

#include <ytlib/cell_master/public.h>
#include <ytlib/misc/property.h>
#include <ytlib/cypress/node_detail.h>

namespace NYT {
namespace NFileServer {

////////////////////////////////////////////////////////////////////////////////

class TFileNode
    : public NCypress::TCypressNodeBase
{
    DEFINE_BYVAL_RW_PROPERTY(NChunkServer::TChunkList*, ChunkList);

public:
    explicit TFileNode(const NCypress::TVersionedNodeId& id);
    TFileNode(const NCypress::TVersionedNodeId& id, const TFileNode& other);

    virtual NCypress::EObjectType GetObjectType() const;

    virtual void Save(TOutputStream* output) const;
    
    virtual void Load(const NCellMaster::TLoadContext& context, TInputStream* input);
};

////////////////////////////////////////////////////////////////////////////////

NCypress::INodeTypeHandler::TPtr CreateFileTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

