#include "portal_exit_node.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/chaos_server/chaos_cell_bundle.h>

#include <yt/yt/server/master/tablet_server/tablet_cell_bundle.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

TPortalExitNode::TPortalExitNode(TVersionedNodeId nodeId)
    : TCypressMapNode(nodeId)
    , DirectAcd_(this)
{ }

TPortalExitNode::TPortalExitNode(TObjectId objectId)
    : TCypressMapNode(objectId)
{ }

void TPortalExitNode::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TCypressMapNode>();

    PHOENIX_REGISTER_FIELD(1, RemovalStarted_);
    PHOENIX_REGISTER_FIELD(2, EntranceCellTag_);
    PHOENIX_REGISTER_FIELD(3, Path_);
    PHOENIX_REGISTER_FIELD(4, Key_);
    PHOENIX_REGISTER_FIELD(5, ParentId_);
    PHOENIX_REGISTER_FIELD(6, EffectiveInheritableAttributes_);
    PHOENIX_REGISTER_FIELD(7, EffectiveAnnotationPath_);
    PHOENIX_REGISTER_FIELD(8, DirectAcd_);
}

PHOENIX_DEFINE_TYPE(TPortalExitNode);

void TPortalExitNode::FillInheritableAttributes(TTransientAttributes *attributes, ENodeMaterializationReason reason) const
{
    TCompositeCypressNode::FillInheritableAttributes(attributes, reason);

    if (EffectiveInheritableAttributes_) {
#define XX(FieldName, AttributeKey) \
        if (!attributes->FieldName.IsSet()) { \
            if (EffectiveInheritableAttributes_->FieldName.IsSet()) { \
                attributes->FieldName.Set(EffectiveInheritableAttributes_->FieldName.Unbox()); \
            } \
        }

        FOR_EACH_INHERITABLE_ATTRIBUTE(XX)

#undef XX
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
