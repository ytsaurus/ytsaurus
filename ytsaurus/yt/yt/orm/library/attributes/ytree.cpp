#include "ytree.h"

#include <yt/yt/core/ytree/exception_helpers.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NOrm::NAttributes {

using namespace NYT::NYTree;
using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

INodePtr GetNodeByPathOrEntity(
    const INodePtr& value,
    const TYPath& path)
{
    static const TNodeWalkOptions WalkOptions{
        .MissingAttributeHandler = [] (const TString& /*key*/) {
            return GetEphemeralNodeFactory()->CreateEntity();
        },
        .MissingChildKeyHandler = [] (const IMapNodePtr& /*node*/, const TString& /*key*/) {
            return GetEphemeralNodeFactory()->CreateEntity();
        },
        .MissingChildIndexHandler = [] (const IListNodePtr& /*node*/, int /*index*/) {
            return GetEphemeralNodeFactory()->CreateEntity();
        },
        .NodeCannotHaveChildrenHandler = [] (const INodePtr& node) {
            if (node->GetType() != ENodeType::Entity) {
                ThrowCannotHaveChildren(node);
            }
            return GetEphemeralNodeFactory()->CreateEntity();
        }};
    return WalkNodeByYPath(value, path, WalkOptions);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
