#include "scion_node.h"

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/chaos_server/chaos_cell_bundle.h>

#include <yt/yt/server/master/tablet_server/tablet_cell_bundle.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TScionNode::TScionNode(TVersionedNodeId nodeId)
    : TSequoiaMapNode(nodeId)
    , DirectAcd_(this)
{ }

void TScionNode::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TSequoiaMapNode>();

    PHOENIX_REGISTER_FIELD(1, RootstockId_);
    PHOENIX_REGISTER_FIELD(2, EffectiveInheritableAttributes_);
    PHOENIX_REGISTER_FIELD(3, EffectiveAnnotationPath_);
    PHOENIX_REGISTER_FIELD(4, DirectAcd_);
}

PHOENIX_DEFINE_TYPE(TScionNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
