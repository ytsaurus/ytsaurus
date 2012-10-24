#include "stdafx.h"
#include "yson_serializable.h"

#include <ytlib/ytree/node.h>
#include <ytlib/ytree/ephemeral_node_factory.h>
#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/yson_consumer.h>

namespace NYT {

using namespace NYTree;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TYsonSerializable::TYsonSerializable()
    : KeepOptions_(false)
{ }

IMapNodePtr TYsonSerializable::GetOptions() const
{
    YCHECK(KeepOptions_);
    return Options;
}

std::vector<Stroka> TYsonSerializable::GetRegisteredKeys() const
{
    std::vector<Stroka> result;
    FOREACH (const auto& pair, Parameters) {
        result.push_back(pair.first);
    }
    return result;
}

void TYsonSerializable::Load(INodePtr node, bool validate, const TYPath& path)
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

void TYsonSerializable::Validate(const TYPath& path) const
{
    FOREACH (auto pair, Parameters) {
        pair.second->Validate(path + "/" + pair.first);
    }
    try {
        DoValidate();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Validation failed at %s", ~path)
            << ex;
    }
}

void TYsonSerializable::DoValidate() const
{ }

void TYsonSerializable::OnLoaded()
{ }

void TYsonSerializable::Save(IYsonConsumer* consumer) const
{
    consumer->OnBeginMap();
    FOREACH (const auto& pair, Parameters) {
        const auto& key = pair.first;
        const auto& parameter = pair.second;
        if (parameter->IsPresent()) {
            consumer->OnKeyedItem(key);
            parameter->Save(consumer);
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
