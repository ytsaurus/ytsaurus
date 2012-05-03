#pragma once

#include <ytlib/misc/property.h>
#include <ytlib/cypress/node_detail.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TTableNode
    : public NCypress::TCypressNodeBase
{
    DEFINE_BYVAL_RW_PROPERTY(NChunkServer::TChunkList*, ChunkList);
    DEFINE_BYREF_RW_PROPERTY(std::vector<Stroka>, KeyColumns);

public:
    explicit TTableNode(const NCypress::TVersionedNodeId& id);
    TTableNode(const NCypress::TVersionedNodeId& id, const TTableNode& other);

    virtual NCypress::EObjectType GetObjectType() const;

    virtual void Save(TOutputStream* output) const;
    
    virtual void Load(const NCellMaster::TLoadContext& context, TInputStream* input);
};

////////////////////////////////////////////////////////////////////////////////

NCypress::INodeTypeHandler::TPtr CreateTableTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

