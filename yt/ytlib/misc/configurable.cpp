#include "stdafx.h"
#include "configurable.h"

#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/ypath_detail.h>

namespace NYT {
namespace NConfig {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TConfigurable::TConfigurable()
    : Options_(GetEphemeralNodeFactory()->CreateMap())
{ }

void TConfigurable::LoadAndValidate(const NYTree::INode* node, const NYTree::TYPath& path)
{
    Load(node, path);
    Validate(path);
}

void TConfigurable::Load(const NYTree::INode* node, const NYTree::TYPath& path)
{
    YASSERT(node);
    TIntrusivePtr<const IMapNode> mapNode;
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

    Options_->Clear();
    FOREACH (const auto& pair, mapNode->GetChildren()) {
        const auto& name = pair.First();
        auto child = pair.Second();
        if (Parameters.find(name) == Parameters.end()) {
            Options_->AddChild(~CloneNode(~child), name);
        }
    }
}

void TConfigurable::Validate(const NYTree::TYPath& path) const
{
    FOREACH (auto pair, Parameters) {
        pair.Second()->Validate(CombineYPaths(path, pair.First()));
    }
    try {
        DoValidate();
    } catch (...) {
        ythrow yexception() << Sprintf("Validation failed (Path: %s)\n%s",
            ~path,
            ~CurrentExceptionMessage());
    }
}

void TConfigurable::DoValidate() const
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NConfig
} // namespace NYT
