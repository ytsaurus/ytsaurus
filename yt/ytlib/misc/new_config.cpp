#include "stdafx.h"
#include "new_config.h"

namespace NYT {

TConfigBase::~TConfigBase()
{ }

void TConfigBase::Load(NYTree::IMapNode* node, Stroka path)
{
    FOREACH (auto pair, Parameters) {
        auto name = pair.First();
        Stroka childPath = path + "/" + name;
        auto child = node != NULL ? node->FindChild(name) : NULL;
        pair.Second()->Load(~child, childPath);
    }
}

void TConfigBase::Validate(Stroka path) const
{
    FOREACH (auto pair, Parameters) {
        pair.Second()->Validate(path + "/" + pair.First());
    }
}

} // namespace NYT
