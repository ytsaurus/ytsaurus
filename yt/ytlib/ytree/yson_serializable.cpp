#include "stdafx.h"
#include "yson_serializable.h"

#include <ytlib/ytree/ytree.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/yson_consumer.h>

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYsonSerializable::TYsonSerializable()
    : KeepOptions_(false)
{ }

NYTree::IMapNodePtr TYsonSerializable::GetOptions() const
{
    YASSERT(KeepOptions_);
    return Options;
}

void TYsonSerializable::Load(NYTree::INodePtr node, bool validate, const NYTree::TYPath& path)
{
    YASSERT(node);

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
                Options->AddChild(ConvertToNode(child), key);
            }
        }
    }

    if (validate) {
        Validate(path);
    }

    OnLoaded();
}

void TYsonSerializable::Validate(const NYTree::TYPath& path) const
{
    FOREACH (auto pair, Parameters) {
        pair.second->Validate(path + "/" + pair.first);
    }
    try {
        DoValidate();
    } catch (const std::exception& ex) {
        ythrow yexception() << Sprintf("Validation failed for %s\n%s",
            ~path,
            ex.what());
    }
}

void TYsonSerializable::DoValidate() const
{ }

void TYsonSerializable::OnLoaded()
{ }

void TYsonSerializable::Save(IYsonConsumer* consumer) const
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

void Serialize(const TYsonSerializable& value, IYsonConsumer* consumer)
{
    value.Save(consumer);
}

void Deserialize(TYsonSerializable& value, INodePtr node)
{
    value.Load(node, false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
