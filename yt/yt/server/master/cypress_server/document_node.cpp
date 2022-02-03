#include "document_node.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TDocumentNode::TDocumentNode(TVersionedNodeId id)
    : TCypressNode(id)
    , Value_(GetEphemeralNodeFactory()->CreateEntity())
{ }

ENodeType TDocumentNode::GetNodeType() const
{
    return ENodeType::Entity;
}

void TDocumentNode::Save(NCellMaster::TSaveContext& context) const
{
    TCypressNode::Save(context);

    using NYT::Save;
    auto serializedValue = ConvertToYsonString(Value_);
    Save(context, serializedValue.ToString());
}

void TDocumentNode::Load(NCellMaster::TLoadContext& context)
{
    TCypressNode::Load(context);

    using NYT::Load;
    auto serializedValue = Load<TString>(context);
    Value_ = ConvertToNode(TYsonString(serializedValue));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
