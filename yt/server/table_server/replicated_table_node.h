#pragma once

#include "table_node.h"

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableNode
    : public TTableNode
{
public:
    explicit TReplicatedTableNode(const NCypressServer::TVersionedNodeId& id);

    virtual NObjectClient::EObjectType GetObjectType() const;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

};

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateReplicatedTableTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

