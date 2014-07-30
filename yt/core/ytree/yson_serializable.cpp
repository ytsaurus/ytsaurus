#include "stdafx.h"
#include "yson_serializable.h"

#include <core/ytree/node.h>
#include <core/ytree/ephemeral_node_factory.h>
#include <core/ytree/ypath_detail.h>

#include <core/yson/consumer.h>

namespace NYT {
namespace NYTree {

using namespace NYPath;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TYsonSerializableLite::TYsonSerializableLite()
    : KeepOptions_(false)
{ }

IMapNodePtr TYsonSerializableLite::GetOptions() const
{
    YCHECK(KeepOptions_);
    return Options;
}

std::vector<Stroka> TYsonSerializableLite::GetRegisteredKeys() const
{
    std::vector<Stroka> result;
    for (const auto& pair : Parameters) {
        result.push_back(pair.first);
    }
    return result;
}

void TYsonSerializableLite::Load(
    INodePtr node,
    bool validate,
    bool setDefaults,
    const TYPath& path)
{
    YCHECK(node);

    if (setDefaults) {
        SetDefaults();
    }

    auto mapNode = node->AsMap();
    for (const auto& pair : Parameters) {
        auto name = pair.first;
        auto childPath = path + "/" + name;
        auto child = mapNode->FindChild(name); // can be NULL
        pair.second->Load(child, childPath);
    }

    if (KeepOptions_) {
        Options = GetEphemeralNodeFactory()->CreateMap();
        for (const auto& pair : mapNode->GetChildren()) {
            const auto& key = pair.first;
            auto child = pair.second;
            if (Parameters.find(key) == Parameters.end()) {
                YCHECK(Options->AddChild(ConvertToNode(child), key));
            }
        }
    }

    if (validate) {
        Validate(path);
    }

    OnLoaded();
}

void TYsonSerializableLite::Validate(const TYPath& path) const
{
    for (auto pair : Parameters) {
        pair.second->Validate(path + "/" + pair.first);
    }

    try {
        for (const auto& validator : Validators) {
            validator.Run();
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Validation failed at %v",
            path.empty() ? "/" : path)
            << ex;
    }
}

void TYsonSerializableLite::SetDefaults()
{
    for (auto pair : Parameters) {
        pair.second->SetDefaults();
    }
    for (const auto& initializer : Initializers) {
        initializer.Run();
    }
}

void TYsonSerializableLite::OnLoaded()
{ }

void TYsonSerializableLite::Save(
    IYsonConsumer* consumer,
    bool sortKeys) const
{
    std::vector<std::pair<Stroka, IParameterPtr>> parameters;
    for (const auto& pair : Parameters) {
        parameters.push_back(pair);
    }

    if (sortKeys) {
        typedef std::pair<Stroka, IParameterPtr> TPair;
        std::sort(
            parameters.begin(),
            parameters.end(),
            [] (const TPair& lhs, const TPair& rhs) {
                return lhs.first < rhs.first;
            });
    }

    consumer->OnBeginMap();
    for (const auto& pair : parameters) {
        const auto& key = pair.first;
        const auto& parameter = pair.second;
        if (parameter->HasValue()) {
            consumer->OnKeyedItem(key);
            parameter->Save(consumer);
        }
    }
    consumer->OnEndMap();
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonSerializableLite& value, IYsonConsumer* consumer)
{
    value.Save(consumer);
}

void Deserialize(TYsonSerializableLite& value, INodePtr node)
{
    value.Load(node);
}

TYsonString ConvertToYsonStringStable(const TYsonSerializableLite& value)
{
    Stroka result;
    TStringOutput output(result);
    TYsonWriter writer(&output, EYsonFormat::Binary, EYsonType::Node);
    value.Save(
        &writer,
        true); // truth matters :)
    return TYsonString(result, EYsonType::Node);   
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT


namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TBinaryYsonSerializer::Save(TStreamSaveContext& context, const TYsonSerializableLite& obj)
{
    auto str = ConvertToYsonStringStable(obj);
    NYT::Save(context, str);
}

void TBinaryYsonSerializer::Load(TStreamLoadContext& context, TYsonSerializableLite& obj)
{
    auto str = NYT::Load<TYsonString>(context);
    auto node = ConvertTo<INodePtr>(str);
    obj.Load(node);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
