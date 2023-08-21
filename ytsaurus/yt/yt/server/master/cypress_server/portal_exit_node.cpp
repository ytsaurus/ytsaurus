#include "portal_exit_node.h"

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

TPortalExitNode::TPortalExitNode(TVersionedNodeId nodeId)
    : TMapNode(nodeId)
    , DirectAcd_(this)
{ }

TPortalExitNode::TPortalExitNode(TObjectId objectId)
    : TMapNode(objectId)
{ }

void TPortalExitNode::Save(TSaveContext& context) const
{
    TMapNode::Save(context);

    using NYT::Save;
    Save(context, RemovalStarted_);
    Save(context, EntranceCellTag_);
    Save(context, Path_);
    Save(context, Key_);
    Save(context, ParentId_);
    Save(context, EffectiveInheritableAttributes_);
    Save(context, EffectiveAnnotationPath_);
    Save(context, DirectAcd_);
}

void TPortalExitNode::Load(TLoadContext& context)
{
    TMapNode::Load(context);

    using NYT::Load;
    Load(context, RemovalStarted_);
    Load(context, EntranceCellTag_);
    Load(context, Path_);
    Load(context, Key_);
    Load(context, ParentId_);
    Load(context, EffectiveInheritableAttributes_);
    Load(context, EffectiveAnnotationPath_);
    Load(context, DirectAcd_);
}

void TPortalExitNode::FillInheritableAttributes(TTransientAttributes *attributes) const
{
    TCompositeNodeBase::FillInheritableAttributes(attributes);

    if (EffectiveInheritableAttributes_) {
#define XX(camelCaseName, snakeCaseName) \
        if (!attributes->camelCaseName.IsSet()) { \
            if (EffectiveInheritableAttributes_->camelCaseName.IsSet()) { \
                attributes->camelCaseName.Set(EffectiveInheritableAttributes_->camelCaseName.Unbox()); \
            } \
        }

        FOR_EACH_INHERITABLE_ATTRIBUTE(XX)

#undef XX
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
