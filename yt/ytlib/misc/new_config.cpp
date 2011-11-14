#include "stdafx.h"
#include "new_config.h"

namespace NYT {

TConfigBase::~TConfigBase()
{ }

void TConfigBase::Load(NYTree::INode* node, const Stroka& path)
{
    NYTree::IMapNode::TPtr mapNode;
    if (node != NULL) {
        try {
            mapNode = node->AsMap();
        } catch(...) {
            ythrow yexception()
                << Sprintf("Config can be loaded only from map node (Path: %s, InnerException: %s)",
                    ~path, ~CurrentExceptionMessage());
        }
    }
    FOREACH (auto pair, Parameters) {
        auto name = pair.First();
        Stroka childPath = path + "/" + name;
        auto child = ~mapNode != NULL ? mapNode->FindChild(name) : NULL;
        pair.Second()->Load(~child, childPath);
    }
}

void TConfigBase::Validate(const Stroka& path) const
{
    FOREACH (auto pair, Parameters) {
        pair.Second()->Validate(path + "/" + pair.First());
    }
}

} // namespace NYT
