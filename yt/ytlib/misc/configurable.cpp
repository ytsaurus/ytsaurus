#include "stdafx.h"
#include "configurable.h"

#include "../ytree/ypath_detail.h"

namespace NYT {
namespace NConfig {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TConfigurable::LoadAndValidate(NYTree::INode* node, const NYTree::TYPath& path)
{
    Load(node, path);
    Validate(path);
}

void TConfigurable::Load(NYTree::INode* node, const NYTree::TYPath& path)
{
    YASSERT(node);
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

void TConfigurable::Validate(const NYTree::TYPath& path) const
{
    FOREACH (auto pair, Parameters) {
        pair.Second()->Validate(CombineYPaths(path, pair.First()));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConfig
} // namespace NYT
