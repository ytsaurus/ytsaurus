#include "document_node.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

#include <yt/yt/core/phoenix/type_def.h>

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

void TDocumentNode::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TCypressNode>();

    registrar.template PropertyField<1,
        [] (const TThis* this_) {
            return std::string(ConvertToYsonString(this_->Value_).AsStringBuf());
        },
        [] (TThis* this_, std::string serializedValue) {
            this_->Value_ = ConvertToNode(TYsonString(serializedValue));
        }>("Value_")();
}

PHOENIX_DEFINE_TYPE(TDocumentNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
