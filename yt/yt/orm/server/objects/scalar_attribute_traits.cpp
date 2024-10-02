#include "scalar_attribute_traits.h"

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

NYTree::IMapNodePtr TScalarAttributeTraits<NYTree::IMapNodePtr>::GetDefaultValue()
{
    return NYTree::GetEphemeralNodeFactory()->CreateMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
