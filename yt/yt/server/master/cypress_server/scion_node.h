#pragma once

#include "node_detail.h"
#include "node_proxy_detail.h"

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TScionNode
    : public TSequoiaMapNode
{
public:
    using TSequoiaMapNode::TSequoiaMapNode;

    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TObjectId, RootstockId);

    DEFINE_BYREF_RW_PROPERTY(std::optional<TPersistentAttributes>, EffectiveInheritableAttributes);
    DEFINE_BYREF_RW_PROPERTY(std::optional<NYPath::TYPath>, EffectiveAnnotationPath);

    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, DirectAcd);

public:
    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
