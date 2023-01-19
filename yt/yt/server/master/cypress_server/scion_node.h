#pragma once

#include "node_detail.h"
#include "node_proxy_detail.h"

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TScionNode
    : public TMapNode
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TObjectId, RootstockId);

    DEFINE_BYVAL_RW_PROPERTY(bool, RemovalStarted);

    // TODO: In Sequoia this will be required for all nodes, so
    // some general mechanism should be used.
    DEFINE_BYVAL_RW_PROPERTY(std::optional<NYPath::TYPath>, Key);
    DEFINE_BYVAL_RW_PROPERTY(TNodeId, ParentId);
    DEFINE_BYVAL_RW_PROPERTY(TString, Path);

    DEFINE_BYREF_RW_PROPERTY(std::optional<TPersistentAttributes>, EffectiveInheritableAttributes);
    DEFINE_BYREF_RW_PROPERTY(std::optional<NYPath::TYPath>, EffectiveAnnotationPath);

    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, DirectAcd);

public:
    explicit TScionNode(TVersionedNodeId nodeId);
    explicit TScionNode(NObjectClient::TObjectId objectId);

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
