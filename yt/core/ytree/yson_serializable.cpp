#include "yson_serializable.h"

#include <yt/core/yson/consumer.h>

#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/node.h>
#include <yt/core/ytree/ypath_detail.h>

namespace NYT {
namespace NYTree {

using namespace NYPath;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TYsonSerializableLite::TYsonSerializableLite()
{ }

IMapNodePtr TYsonSerializableLite::GetUnrecognized() const
{
    return Unrecognized;
}

IMapNodePtr TYsonSerializableLite::GetUnrecognizedRecursively() const
{
    // Take a copy of `Unrecognized` and add parameter->GetUnrecognizedRecursively()
    // for all parameters that are TYsonSerializable's themselves.
    auto result = Unrecognized ? ConvertTo<IMapNodePtr>(Unrecognized) : GetEphemeralNodeFactory()->CreateMap();
    for (const auto& pair : Parameters) {
        const auto& parameter = pair.second;
        const auto& name = pair.first;
        auto unrecognized = parameter->GetUnrecognizedRecursively();
        if (unrecognized && unrecognized->AsMap()->GetChildCount() > 0) {
            result->AddChild(unrecognized, name);
        }
    }
    return result;
}

void TYsonSerializableLite::SetUnrecognizedStrategy(EUnrecognizedStrategy strategy)
{
    UnrecognizedStrategy = strategy;
    if (strategy == EUnrecognizedStrategy::KeepRecursive) {
        for (const auto& pair : Parameters) {
            pair.second->SetKeepUnrecognizedRecursively();
        }
    }
}

THashSet<TString> TYsonSerializableLite::GetRegisteredKeys() const
{
    THashSet<TString> result;
    for (const auto& pair : Parameters) {
        result.insert(pair.first);
        for (const auto& key : pair.second->GetAliases()) {
            result.insert(key);
        }
    }
    return result;
}

void TYsonSerializableLite::Load(
    INodePtr node,
    bool postprocess,
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
        auto& parameter = pair.second;
        TString key = name;
        auto child = mapNode->FindChild(name); // can be NULL
        for (const auto& alias : parameter->GetAliases()) {
            auto otherChild = mapNode->FindChild(alias);
            if (child && otherChild && !AreNodesEqual(child, otherChild)) {
                THROW_ERROR_EXCEPTION("Different values for aliased parameters %Qv and %Qv", key, alias)
                    << TErrorAttribute("main_value", child)
                    << TErrorAttribute("aliased_value", otherChild);
            }
            if (!child && otherChild) {
                child = otherChild;
                key = alias;
            }
        }
        auto childPath = path + "/" + key;
        parameter->Load(child, childPath);
    }

    if (UnrecognizedStrategy != EUnrecognizedStrategy::Drop) {
        auto registeredKeys = GetRegisteredKeys();
        if (!Unrecognized) {
            Unrecognized = GetEphemeralNodeFactory()->CreateMap();
        }
        for (const auto& pair : mapNode->GetChildren()) {
            const auto& key = pair.first;
            auto child = pair.second;
            if (registeredKeys.find(key) == registeredKeys.end()) {
                Unrecognized->RemoveChild(key);
                YCHECK(Unrecognized->AddChild(ConvertToNode(child), key));
            }
        }
    }

    if (postprocess) {
        Postprocess(path);
    }
}

void TYsonSerializableLite::Save(
    IYsonConsumer* consumer,
    bool stable) const
{
    std::vector<std::pair<TString, IParameterPtr>> parameters;
    for (const auto& pair : Parameters) {
        parameters.push_back(pair);
    }

    if (stable) {
        typedef std::pair<TString, IParameterPtr> TPair;
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
        if (!parameter->CanOmitValue()) {
            consumer->OnKeyedItem(key);
            parameter->Save(consumer);
        }
    }

    if (Unrecognized) {
        for (const auto& pair : Unrecognized->GetChildren()) {
            consumer->OnKeyedItem(pair.first);
            Serialize(pair.second, consumer);
        }
    }

    consumer->OnEndMap();
}

void TYsonSerializableLite::Postprocess(const TYPath& path) const
{
    for (const auto& pair : Parameters) {
        pair.second->Postprocess(path + "/" + pair.first);
    }

    try {
        for (const auto& postprocessor : Postprocessors) {
            postprocessor();
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Postprocess failed at %v",
            path.empty() ? "root" : path)
            << ex;
    }
}

void TYsonSerializableLite::SetDefaults()
{
    for (const auto& pair : Parameters) {
        pair.second->SetDefaults();
    }
    for (const auto& initializer : Preprocessors) {
        initializer();
    }
}

void TYsonSerializableLite::RegisterPreprocessor(const TPreprocessor& func)
{
    func();
    Preprocessors.push_back(func);
}

void TYsonSerializableLite::RegisterPostprocessor(const TPostprocessor& func)
{
    Postprocessors.push_back(func);
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
    TStringStream output;
    TBufferedBinaryYsonWriter writer(&output);
    value.Save(
        &writer,
        true); // truth matters :)
    writer.Flush();
    return TYsonString(output.Str());
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TYsonSerializable);

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
