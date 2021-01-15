#include "serialize.h"

#include <yt/core/ytree/node.h>

namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

void Serialize(EWireType wireType, NYT::NYson::IYsonConsumer* consumer)
{
    consumer->OnStringScalar(::ToString(wireType));
}

void Deserialize(EWireType& wireType, NYT::NYTree::INodePtr node)
{
    if (node->GetType() != NYT::NYTree::ENodeType::String) {
        THROW_ERROR_EXCEPTION("Cannot deserialize Skiff wire type from %Qlv node, expected %Qlv",
            node->GetType(),
            NYT::NYTree::ENodeType::String);
    }
    wireType = ::FromString<EWireType>(node->GetValue<TString>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkiff
