#include "scion_node.h"

#include <yt/yt/core/ypath/helpers.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

void TScionNode::Save(TSaveContext& context) const
{
    TSequoiaMapNode::Save(context);

    using NYT::Save;

    Save(context, RootstockId_);
    Save(context, EffectiveInheritableAttributes_);
    Save(context, EffectiveAnnotationPath_);
    Save(context, DirectAcd_);
}

void TScionNode::Load(TLoadContext& context)
{
    TSequoiaMapNode::Load(context);

    using NYT::Load;

    Load(context, RootstockId_);
    // COMPAT(kvk1920)
    if (context.GetVersion() < EMasterReign::SequoiaMapNode) {
        Load<bool>(context); // RemovalStarted.
        auto legacyKey = Load<std::optional<TString>>(context);
        Load<TNodeId>(context); // ParentId.

        auto path = Load<TYPath>(context);
        auto key = legacyKey ? *legacyKey : DirNameAndBaseName(path).second;
        ImmutableSequoiaProperties_ = std::make_unique<TCypressNode::TImmutableSequoiaProperties>(key, path);
        MutableSequoiaProperties_ = std::make_unique<TCypressNode::TMutableSequoiaProperties>();
    }
    Load(context, EffectiveInheritableAttributes_);
    Load(context, EffectiveAnnotationPath_);
    Load(context, DirectAcd_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
