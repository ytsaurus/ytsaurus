#include "stdafx.h"
#include "configurable.h"

#include <ytlib/ytree/ytree.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/yson_consumer.h>

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TConfigurable::TConfigurable()
    : KeepOptions_(false)
{ }

NYTree::IMapNodePtr TConfigurable::GetOptions() const
{
    YASSERT(KeepOptions_);
    return Options;
}

void TConfigurable::Load(NYTree::INodePtr node, bool validate, const NYTree::TYPath& path)
{
    YASSERT(node);

    if (node->GetType() != ENodeType::Map) {
        ythrow yexception() << Sprintf("Configuration must be loaded from a map node (Path: %s)",
            ~path);
    }

    auto mapNode = node->AsMap();
    FOREACH (const auto& pair, Parameters) {
        auto name = pair.first;
        auto childPath = path + "/" + name;
        auto child = mapNode->FindChild(name); // can be NULL
        pair.second->Load(child, childPath);
    }

    if (KeepOptions_) {
        Options = GetEphemeralNodeFactory()->CreateMap();
        FOREACH (const auto& pair, mapNode->GetChildren()) {
            const auto& key = pair.first;
            auto child = pair.second;
            if (Parameters.find(key) == Parameters.end()) {
                Options->AddChild(~CloneNode(child), key);
            }
        }
    }

    if (validate) {
        Validate(path);
    }
}

void TConfigurable::Validate(const NYTree::TYPath& path) const
{
    FOREACH (auto pair, Parameters) {
        pair.second->Validate(path + "/" + pair.first);
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
            consumer->OnKeyedItem(pair->first);
            pair->second->Save(consumer);
        }
    }
    consumer->OnEndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
