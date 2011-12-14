#include "stdafx.h"
#include "new_config.h"

#include "../ytree/ypath_detail.h"

namespace NYT {
namespace NConfig {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TConfigBase::Load(NYTree::INode* node, const NYTree::TYPath& path)
{
    YASSERT(node != NULL);
    NYTree::IMapNode::TPtr mapNode;
    try {
        mapNode = node->AsMap();
    } catch(...) {
        ythrow yexception()
            << Sprintf("Configuration must be loaded from a map node (Path: %s)\n%s",
                ~path,
                ~CurrentExceptionMessage());
    }
    FOREACH (auto pair, Parameters) {
        auto name = pair.First();
        auto childPath = CombineYPaths(path, name);
        auto child = mapNode->FindChild(name); // can be NULL
        pair.Second()->Load(~child, childPath);
    }
}

void TConfigBase::Validate(const NYTree::TYPath& path) const
{
    FOREACH (auto pair, Parameters) {
        pair.Second()->Validate(CombineYPaths(path, pair.First()));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConfig
} // namespace NYT
