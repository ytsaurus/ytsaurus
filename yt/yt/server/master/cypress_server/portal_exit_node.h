#pragma once

#include "node_detail.h"

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TPortalExitNode
    : public TCypressMapNode
{
public:
    DEFINE_BYVAL_RW_PROPERTY(bool, RemovalStarted);
    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TCellTag, EntranceCellTag);
    DEFINE_BYVAL_RW_PROPERTY(NYPath::TYPath, Path);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<NYPath::TYPath>, Key);
    DEFINE_BYVAL_RW_PROPERTY(TNodeId, ParentId);

    DEFINE_BYREF_RW_PROPERTY(std::optional<TPersistentAttributes>, EffectiveInheritableAttributes);
    DEFINE_BYREF_RW_PROPERTY(std::optional<NYPath::TYPath>, EffectiveAnnotationPath);

    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, DirectAcd);

public:
    friend class TPortalExitTypeHandler;

    explicit TPortalExitNode(TVersionedNodeId nodeId);
    explicit TPortalExitNode(NObjectClient::TObjectId objectId);

    void FillInheritableAttributes(TTransientAttributes* attributes, ENodeMaterializationReason reason = ENodeMaterializationReason::Create) const override;

private:
    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TPortalExitNode, 0x8fa7cf90);
};

DEFINE_MASTER_OBJECT_TYPE(TPortalExitNode)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
