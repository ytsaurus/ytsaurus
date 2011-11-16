#include "stdafx.h"
#include "new_config.h"

namespace NYT {

TConfigBase::~TConfigBase()
{ }

void TConfigBase::Load(NYTree::INode* node, const Stroka& path)
{
    YASSERT(node != NULL);
    NYTree::IMapNode::TPtr mapNode;
    try {
        mapNode = node->AsMap();
    } catch(...) {
        ythrow yexception()
            << Sprintf("Config can be loaded only from map node (Path: %s, InnerException: %s)",
                ~path, ~CurrentExceptionMessage());
    }
    FOREACH (auto pair, Parameters) {
        auto name = pair.First();
        Stroka childPath = path + "/" + name;
        auto child = mapNode->FindChild(name); // can be NULL
        pair.Second()->Load(~child, childPath);
    }
}

void TConfigBase::Validate(const Stroka& path) const
{
    FOREACH (auto pair, Parameters) {
        pair.Second()->Validate(path + "/" + pair.First());
    }
}

void TConfigBase::SetDefaults(bool skipRequiredParameters, const Stroka& path)
{
    FOREACH (auto pair, Parameters) {
        auto name = pair.First();
        Stroka childPath = path + "/" + name;
        pair.Second()->SetDefaults(skipRequiredParameters, childPath);
    }
}

} // namespace NYT
