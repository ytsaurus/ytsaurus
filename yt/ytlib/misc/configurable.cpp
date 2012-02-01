#include "stdafx.h"
#include "configurable.h"

#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/ypath_detail.h>

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TConfigurable::TConfigurable()
    : KeepOptions_(false)
{ }

NYTree::IMapNode::TPtr TConfigurable::GetOptions() const
{
    YASSERT(KeepOptions_);
    return Options;
}

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
    } catch (const std::exception& ex) {
        ythrow yexception()
            << Sprintf("Configuration must be loaded from a map node (Path: %s)\n%s",
                ~path,
                ex.what());
    }
    FOREACH (const auto& pair, Parameters) {
        auto name = pair.first;
        auto childPath = CombineYPaths(path, name);
        auto child = mapNode->FindChild(name); // can be NULL
        pair.second->Load(~child, childPath);
    }

    if (KeepOptions_) {
        Options = GetEphemeralNodeFactory()->CreateMap();
        FOREACH (const auto& pair, mapNode->GetChildren()) {
            const auto& name = pair.first;
            auto child = pair.second;
            if (Parameters.find(name) == Parameters.end()) {
                Options->AddChild(~CloneNode(~child), name);
            }
        }
    }
}

void TConfigurable::Validate(const NYTree::TYPath& path) const
{
    FOREACH (auto pair, Parameters) {
        pair.second->Validate(CombineYPaths(path, pair.first));
    }
    try {
        DoValidate();
    } catch (const std::exception& ex) {
        ythrow yexception() << Sprintf("Validation failed (Path: %s)\n%s",
            ~path,
            ex.what());
    }
}

void TConfigurable::DoValidate() const
{ }

void TConfigurable::Save(IYsonConsumer* consumer) const
{
    consumer->OnBeginMap();
    auto sortedItems = GetSortedIterators(Parameters);
    FOREACH (const auto& pair, sortedItems) {
        const auto& parameter = pair->second;
        if (parameter->IsPresent()) {
            consumer->OnMapItem(pair->first);
            pair->second->Save(consumer);
        }
    }
    consumer->OnEndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
