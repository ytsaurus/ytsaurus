#ifndef YSON_STRUCT_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include yson_struct_detail.h"
// For the sake of sane code completion.
#include "yson_struct_detail.h"
#endif

#include "ypath_client.h"

#include <yt/yt/core/yson/token_writer.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

template <class T>
concept IsYsonStructOrYsonSerializable = std::is_base_of_v<TYsonStruct, T> || std::is_base_of_v<TYsonSerializable, T>;

// TODO(shakurov): get rid of this once concept support makes it into the standard
// library implementation. Use equality-comparability instead.
template <class T>
concept SupportsDontSerializeDefaultImpl =
    std::is_arithmetic_v<T> ||
    std::is_same_v<T, TString> ||
    std::is_same_v<T, TDuration> ||
    std::is_same_v<T, TGuid>;

template <class T>
concept SupportsDontSerializeDefault =
    SupportsDontSerializeDefaultImpl<T> ||
    TStdOptionalTraits<T>::IsStdOptional &&
    SupportsDontSerializeDefaultImpl<typename TStdOptionalTraits<T>::TValueType>;

////////////////////////////////////////////////////////////////////////////////

template <class T>
void LoadFromNode(
    T& parameter,
    NYTree::INodePtr node,
    const NYPath::TYPath& path,
    EMergeStrategy /*mergeStrategy*/,
    bool /*keepUnrecognizedRecursively*/)
{
    try {
        Deserialize(parameter, node);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading parameter %v", path)
                << ex;
    }
}

// INodePtr
template <>
inline void LoadFromNode(
    NYTree::INodePtr& parameter,
    NYTree::INodePtr node,
    const NYPath::TYPath& /*path*/,
    EMergeStrategy mergeStrategy,
    bool /*keepUnrecognizedRecursively*/)
{
    switch (mergeStrategy) {
        case EMergeStrategy::Default:
        case EMergeStrategy::Overwrite: {
            parameter = node;
            break;
        }

        case EMergeStrategy::Combine: {
            if (!parameter) {
                parameter = node;
            } else {
                parameter = PatchNode(parameter, node);
            }
            break;
        }

        default:
            YT_UNIMPLEMENTED();
    }
}

// TYsonStruct or TYsonSerializable
template <IsYsonStructOrYsonSerializable T>
void LoadFromNode(
    TIntrusivePtr<T>& parameterValue,
    NYTree::INodePtr node,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively)
{
    if (!parameterValue || mergeStrategy == EMergeStrategy::Overwrite) {
        parameterValue = New<T>();
    }

    if (keepUnrecognizedRecursively) {
        parameterValue->SetUnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);
    }

    switch (mergeStrategy) {
        case EMergeStrategy::Default:
        case EMergeStrategy::Overwrite:
        case EMergeStrategy::Combine: {
            parameterValue->Load(node, false, false, path);
            break;
        }

        default:
            YT_UNIMPLEMENTED();
    }
}

// std::optional
template <class T>
void LoadFromNode(
    std::optional<T>& parameter,
    NYTree::INodePtr node,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively)
{
    switch (mergeStrategy) {
        case EMergeStrategy::Default:
        case EMergeStrategy::Overwrite: {
            if (node->GetType() == NYTree::ENodeType::Entity) {
                parameter = std::nullopt;
            } else {
                T value;
                LoadFromNode(value, node, path, EMergeStrategy::Overwrite, keepUnrecognizedRecursively);
                parameter = std::move(value);
            }
            break;
        }

        default:
            YT_UNIMPLEMENTED();
    }
}

// std::vector
template <class... T>
void LoadFromNode(
    std::vector<T...>& parameter,
    NYTree::INodePtr node,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively)
{
    switch (mergeStrategy) {
        case EMergeStrategy::Default:
        case EMergeStrategy::Overwrite: {
            auto listNode = node->AsList();
            auto size = listNode->GetChildCount();
            parameter.resize(size);
            for (int i = 0; i < size; ++i) {
                LoadFromNode(
                    parameter[i],
                    listNode->GetChildOrThrow(i),
                    path + "/" + NYPath::ToYPathLiteral(i),
                    EMergeStrategy::Overwrite,
                    keepUnrecognizedRecursively);
            }
            break;
        }

        default:
            YT_UNIMPLEMENTED();
    }
}

template <class T>
T DeserializeMapKey(TStringBuf value)
{
    if constexpr (TEnumTraits<T>::IsEnum) {
        return TEnumTraits<T>::FromString(DecodeEnumValue(value));
    } else {
        return FromString<T>(value);
    }
}

// For any map.
template <template <typename...> class Map, class... T, class M = typename Map<T...>::mapped_type>
void LoadFromNode(
    Map<T...>& parameter,
    NYTree::INodePtr node,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively)
{
    switch (mergeStrategy) {
        case EMergeStrategy::Default:
        case EMergeStrategy::Overwrite: {
            auto mapNode = node->AsMap();
            parameter.clear();
            for (const auto& [key, child] : mapNode->GetChildren()) {
                M value;
                LoadFromNode(
                    value,
                    child,
                    path + "/" + NYPath::ToYPathLiteral(key),
                    EMergeStrategy::Overwrite,
                    keepUnrecognizedRecursively);
                parameter.emplace(DeserializeMapKey<typename Map<T...>::key_type>(key), std::move(value));
            }
            break;
        }
        case EMergeStrategy::Combine: {
            auto mapNode = node->AsMap();
            for (const auto& [key, child] : mapNode->GetChildren()) {
                M value;
                LoadFromNode(
                    value,
                    child,
                    path + "/" + NYPath::ToYPathLiteral(key),
                    EMergeStrategy::Combine,
                    keepUnrecognizedRecursively);
                parameter[DeserializeMapKey<typename Map<T...>::key_type>(key)] = std::move(value);
            }
            break;
        }

        default:
            YT_UNIMPLEMENTED();
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void LoadFromCursor(
    T& parameter,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy /*mergeStrategy*/,
    bool /*keepUnrecognizedRecursively*/)
{
    try {
        Deserialize(parameter, cursor);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading parameter %v", path)
            << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <IsYsonStructOrYsonSerializable T>
void LoadFromCursor(
    TIntrusivePtr<T>& parameterValue,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively);

template <class... T>
void LoadFromCursor(
    std::vector<T...>& parameter,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively);

// std::optional
template <class T>
void LoadFromCursor(
    std::optional<T>& parameter,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively);

template <template <typename...> class Map, class... T, class M = typename Map<T...>::mapped_type>
void LoadFromCursor(
    Map<T...>& parameter,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively);

////////////////////////////////////////////////////////////////////////////////

// INodePtr
template <>
inline void LoadFromCursor(
    NYTree::INodePtr& parameter,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively)
{
    try {
        auto node = NYson::ExtractTo<INodePtr>(cursor);
        LoadFromNode(parameter, std::move(node), path, mergeStrategy, keepUnrecognizedRecursively);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error loading parameter %v", path)
            << ex;
    }
}

// TYsonStruct or TYsonSerializable
template <IsYsonStructOrYsonSerializable T>
void LoadFromCursor(
    TIntrusivePtr<T>& parameterValue,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively)
{
    if (!parameterValue || mergeStrategy == EMergeStrategy::Overwrite) {
        parameterValue = New<T>();
    }

    if (keepUnrecognizedRecursively) {
        parameterValue->SetUnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);
    }

    switch (mergeStrategy) {
        case EMergeStrategy::Default:
        case EMergeStrategy::Overwrite:
        case EMergeStrategy::Combine: {
            parameterValue->Load(cursor, false, false, path);
            break;
        }

        default:
            YT_UNIMPLEMENTED();
    }
}

// std::optional
template <class T>
void LoadFromCursor(
    std::optional<T>& parameter,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively)
{
    try {
        switch (mergeStrategy) {
            case EMergeStrategy::Default:
            case EMergeStrategy::Overwrite: {
                if ((*cursor)->GetType() == NYson::EYsonItemType::EntityValue) {
                    parameter = std::nullopt;
                    cursor->Next();
                } else {
                    T value;
                    LoadFromCursor(value, cursor, path, EMergeStrategy::Overwrite, keepUnrecognizedRecursively);
                    parameter = std::move(value);
                }
                break;
            }

            default:
                YT_UNIMPLEMENTED();
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error loading parameter %v", path)
            << ex;
    }
}

// std::vector
template <class... T>
void LoadFromCursor(
    std::vector<T...>& parameter,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively)
{
    try {
        switch (mergeStrategy) {
            case EMergeStrategy::Default:
            case EMergeStrategy::Overwrite: {
                parameter.clear();
                int index = 0;
                cursor->ParseList([&](NYson::TYsonPullParserCursor* cursor) {
                    LoadFromCursor(
                        parameter.emplace_back(),
                        cursor,
                        path + "/" + NYPath::ToYPathLiteral(index),
                        EMergeStrategy::Overwrite,
                        keepUnrecognizedRecursively);
                    ++index;
                });
                break;
            }

            default:
                YT_UNIMPLEMENTED();
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error loading parameter %v", path)
            << ex;
    }
}

// For any map.
template <template <typename...> class Map, class... T, class M>
void LoadFromCursor(
    Map<T...>& parameter,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively)
{
    try {
        auto doParse = [&] (const auto& setterOrEmplacer, EMergeStrategy mergeStrategy) {
            cursor->ParseMap([&] (NYson::TYsonPullParserCursor* cursor) {
                auto key = ExtractTo<TString>(cursor);
                M value;
                LoadFromCursor(
                    value,
                    cursor,
                    path + "/" + NYPath::ToYPathLiteral(key),
                    mergeStrategy,
                    keepUnrecognizedRecursively);
                setterOrEmplacer(key, std::move(value));
            });
        };

        switch (mergeStrategy) {
            case EMergeStrategy::Default:
            case EMergeStrategy::Overwrite: {
                parameter.clear();
                auto emplacer = [&] (auto key, M&& value) {
                    parameter.emplace(DeserializeMapKey<typename Map<T...>::key_type>(key), std::move(value));
                };
                doParse(emplacer, EMergeStrategy::Overwrite);
                break;
            }
            case EMergeStrategy::Combine: {
                auto setter = [&] (auto key, M&& value) {
                    parameter[DeserializeMapKey<typename Map<T...>::key_type>(key)] = std::move(value);
                };
                doParse(setter, EMergeStrategy::Combine);
                break;
            }
            default:
                YT_UNIMPLEMENTED();
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error loading parameter %v", path)
            << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

// For all classes except descendants of TYsonStructBase and their intrusive pointers
// we do not attempt to extract unrecognized members. C++ prohibits function template specialization
// so we have to deal with static struct members.
template <class T, class = void>
struct TGetRecursiveUnrecognized
{
    static IMapNodePtr Do(const T& /*parameter*/)
    {
        return nullptr;
    }
};

template <class T>
struct TGetRecursiveUnrecognized<T, std::enable_if_t<std::is_base_of<TYsonStructBase, T>::value>>
{
    static IMapNodePtr Do(const T& parameter)
    {
        return parameter.GetRecursiveUnrecognized();
    }
};

template <class T>
struct TGetRecursiveUnrecognized<T, std::enable_if_t<std::is_base_of<TYsonStructBase, typename T::TUnderlying>::value>>
{
    static IMapNodePtr Do(const T& parameter)
    {
        return parameter ? parameter->GetRecursiveUnrecognized() : nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

// all
template <class F>
void InvokeForComposites(
    const void* /*parameter*/,
    const NYPath::TYPath& /*path*/,
    const F& /*func*/)
{ }

// TYsonStruct or TYsonSerializable
template <IsYsonStructOrYsonSerializable T, class F>
inline void InvokeForComposites(
    const TIntrusivePtr<T>* parameterValue,
    const NYPath::TYPath& path,
    const F& func)
{
    func(*parameterValue, path);
}

// std::vector
template <class... T, class F>
inline void InvokeForComposites(
    const std::vector<T...>* parameter,
    const NYPath::TYPath& path,
    const F& func)
{
    for (size_t i = 0; i < parameter->size(); ++i) {
        InvokeForComposites(
            &(*parameter)[i],
            path + "/" + NYPath::ToYPathLiteral(i),
            func);
    }
}

// For any map.
template <template <typename...> class Map, class... T, class F, class M = typename Map<T...>::mapped_type>
inline void InvokeForComposites(
    const Map<T...>* parameter,
    const NYPath::TYPath& path,
    const F& func)
{
    for (const auto& [key, value] : *parameter) {
        InvokeForComposites(
            &value,
            path + "/" + NYPath::ToYPathLiteral(key),
            func);
    }
}

////////////////////////////////////////////////////////////////////////////////

// all
template <class F>
void InvokeForComposites(
    const void* /* parameter */,
    const F& /* func */)
{ }

// TYsonStruct or TYsonSerializable
template <IsYsonStructOrYsonSerializable T, class F>
inline void InvokeForComposites(const TIntrusivePtr<T>* parameter, const F& func)
{
    func(*parameter);
}

// std::vector
template <class... T, class F>
inline void InvokeForComposites(const std::vector<T...>* parameter, const F& func)
{
    for (const auto& item : *parameter) {
        InvokeForComposites(&item, func);
    }
}

// For any map.
template <template <typename...> class Map, class... T, class F, class M = typename Map<T...>::mapped_type>
inline void InvokeForComposites(const Map<T...>* parameter, const F& func)
{
    for (const auto& [key, value] : *parameter) {
        InvokeForComposites(&value, func);
    }
}

template <class T, class = void>
struct IsYsonStructPtr : std::false_type
{ };

template <class T>
struct IsYsonStructPtr<TIntrusivePtr<T>, typename std::enable_if<std::is_convertible<T&, TYsonStruct&>::value>::type> : std::true_type
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NYsonStructDetail

////////////////////////////////////////////////////////////////////////////////

template <class TStruct>
void TYsonStructMeta<TStruct>::SetDefaultsOfInitializedStruct(TYsonStructBase* target) const
{
    for (const auto& [_, parameter] : Parameters_) {
        parameter->SetDefaultsInitialized(target);
    }

    for (const auto& preprocessor : Preprocessors_) {
        preprocessor(target);
    }
}

template <class TStruct>
const THashSet<TString>& TYsonStructMeta<TStruct>::GetRegisteredKeys() const
{
    return RegisteredKeys_;
}

template <class TStruct>
const THashMap<TString, IYsonStructParameterPtr>& TYsonStructMeta<TStruct>::GetParameterMap() const
{
    return Parameters_;
}

template <class TStruct>
const std::vector<std::pair<TString, IYsonStructParameterPtr>>& TYsonStructMeta<TStruct>::GetParameterSortedList() const
{
    return SortedParameters_;
}

template <class TStruct>
IYsonStructParameterPtr TYsonStructMeta<TStruct>::GetParameter(const TString& keyOrAlias) const
{
    auto it = Parameters_.find(keyOrAlias);
    if (it != Parameters_.end()) {
        return it->second;
    }

    for (const auto& [_, parameter] : Parameters_) {
        if (Count(parameter->GetAliases(), keyOrAlias) > 0) {
            return parameter;
        }
    }
    THROW_ERROR_EXCEPTION("Key or alias %Qv not found in yson struct", keyOrAlias);
}

template <class TStruct>
void TYsonStructMeta<TStruct>::LoadParameter(TYsonStructBase* target, const TString& key, const NYTree::INodePtr& node, EMergeStrategy mergeStrategy) const
{
    const auto& parameter = GetParameter(key);
    auto validate = [&] () {
        parameter->Postprocess(target, "/" + key);
        try {
            for (const auto& postprocessor : Postprocessors_) {
                postprocessor(target);
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                "Postprocess failed while loading parameter %Qv from value %Qv",
                key,
                ConvertToYsonString(node, NYson::EYsonFormat::Text))
                    << ex;
        }
    };
    auto loadOptions = TLoadParameterOptions{
        .Path = "",
        .MergeStrategy = mergeStrategy
    };

    parameter->SafeLoad(target, node, loadOptions, validate);
}

template <class TStruct>
void TYsonStructMeta<TStruct>::Postprocess(TYsonStructBase* target, const TYPath& path) const
{
    for (const auto& [name, parameter] : Parameters_) {
        parameter->Postprocess(target, path + "/" + name);
    }

    try {
        for (const auto& postprocessor : Postprocessors_) {
            postprocessor(target);
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Postprocess failed at %v",
            path.empty() ? "root" : path)
                << ex;
    }
}

template <class TStruct>
void TYsonStructMeta<TStruct>::LoadStruct(
    TYsonStructBase* target,
    INodePtr node,
    bool postprocess,
    bool setDefaults,
    const TYPath& path) const
{
    YT_VERIFY(node);

    if (setDefaults) {
        SetDefaultsOfInitializedStruct(target);
    }

    auto mapNode = node->AsMap();
    auto unrecognizedStrategy = target->InstanceUnrecognizedStrategy_.template value_or(MetaUnrecognizedStrategy_);
    for (const auto& [name, parameter] : Parameters_) {
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
        auto loadOptions = TLoadParameterOptions{
            .Path = path + "/" + key,
            .KeepUnrecognizedRecursively = unrecognizedStrategy == EUnrecognizedStrategy::KeepRecursive
        };
        parameter->Load(target, child, loadOptions);
    }

    if (unrecognizedStrategy != EUnrecognizedStrategy::Drop) {
        const auto& registeredKeys = GetRegisteredKeys();
        if (!target->LocalUnrecognized_) {
            target->LocalUnrecognized_ = GetEphemeralNodeFactory()->CreateMap();
        }
        for (const auto& [key, child] : mapNode->GetChildren()) {
            if (!registeredKeys.contains(key)) {
                target->LocalUnrecognized_->RemoveChild(key);
                YT_VERIFY(target->LocalUnrecognized_->AddChild(key, ConvertToNode(child)));
            }
        }
    }

    if (postprocess) {
        Postprocess(target, path);
    }
}

template <class TStruct>
void TYsonStructMeta<TStruct>::LoadStruct(
    TYsonStructBase* target,
    NYson::TYsonPullParserCursor* cursor,
    bool postprocess,
    bool setDefaults,
    const TYPath& path) const
{
    YT_VERIFY(cursor);

    if (setDefaults) {
        SetDefaultsOfInitializedStruct(target);
    }

    auto unrecognizedStrategy = target->InstanceUnrecognizedStrategy_.template value_or(MetaUnrecognizedStrategy_);

    auto createLoadOptions = [&] (TStringBuf key) {
        return TLoadParameterOptions{
            .Path = path + "/" + key,
            .KeepUnrecognizedRecursively = unrecognizedStrategy == EUnrecognizedStrategy::KeepRecursive,
        };
    };

    THashMap<TStringBuf, IYsonStructParameter*> keyToParameter;
    THashSet<IYsonStructParameter*> pendingParameters;
    for (const auto& [key, parameter] : Parameters_) {
        EmplaceOrCrash(keyToParameter, key, parameter.Get());
        for (const auto& alias : parameter->GetAliases()) {
            EmplaceOrCrash(keyToParameter, alias, parameter.Get());
        }
        InsertOrCrash(pendingParameters, parameter.Get());
    }

    THashMap<TString, TString> aliasedData;

    auto processPossibleAlias = [&] (
        IYsonStructParameter* parameter,
        TStringBuf key,
        NYson::TYsonPullParserCursor* cursor)
    {
        TStringStream ss;
        {
            NYson::TCheckedInDebugYsonTokenWriter writer(&ss);
            cursor->TransferComplexValue(&writer);
        }
        auto data = std::move(ss.Str());
        const auto& canonicalKey = parameter->GetKey();
        auto aliasedDataIt = aliasedData.find(canonicalKey);
        if (aliasedDataIt != aliasedData.end()) {
            auto firstNode = ConvertTo<INodePtr>(NYson::TYsonStringBuf(aliasedDataIt->second));
            auto secondNode = ConvertTo<INodePtr>(NYson::TYsonStringBuf(data));
            if (!AreNodesEqual(firstNode, secondNode)) {
                THROW_ERROR_EXCEPTION("Different values for aliased parameters %Qv and %Qv", canonicalKey, key)
                    << TErrorAttribute("main_value", firstNode)
                    << TErrorAttribute("aliased_value", secondNode);
            }
            return;
        }
        {
            TStringInput input(data);
            NYson::TYsonPullParser parser(&input, NYson::EYsonType::Node);
            NYson::TYsonPullParserCursor newCursor(&parser);
            parameter->Load(target, &newCursor, createLoadOptions(key));
        }
        EmplaceOrCrash(aliasedData, canonicalKey, std::move(data));
    };

    auto processUnrecognized = [&] (const TString& key, NYson::TYsonPullParserCursor* cursor) {
        if (unrecognizedStrategy == EUnrecognizedStrategy::Drop) {
            cursor->SkipComplexValue();
            return;
        }
        if (!target->LocalUnrecognized_) {
            target->LocalUnrecognized_ = GetEphemeralNodeFactory()->CreateMap();
        }
        target->LocalUnrecognized_->RemoveChild(key);
        auto added = target->LocalUnrecognized_->AddChild(key, NYson::ExtractTo<INodePtr>(cursor));
        YT_VERIFY(added);
    };

    cursor->ParseMap([&] (NYson::TYsonPullParserCursor* cursor) {
        auto key = ExtractTo<TString>(cursor);
        auto it = keyToParameter.find(key);
        if (it == keyToParameter.end()) {
            processUnrecognized(key, cursor);
            return;
        }

        auto* parameter = it->second;
        if (parameter->GetAliases().empty()) {
            parameter->Load(target, cursor, createLoadOptions(key));
        } else {
            processPossibleAlias(parameter, key, cursor);
        }
        // NB: Key may be missing in case of aliasing.
        pendingParameters.erase(parameter);
    });

    for (const auto parameter : pendingParameters) {
        parameter->Load(target, /* cursor */ nullptr, createLoadOptions(parameter->GetKey()));
    }

    if (postprocess) {
        Postprocess(target, path);
    }
}

template <class TStruct>
IMapNodePtr TYsonStructMeta<TStruct>::GetRecursiveUnrecognized(const TYsonStructBase* target) const
{
    // Take a copy of `LocalUnrecognized` and add parameter->GetRecursiveUnrecognized()
    // for all parameters that are TYsonStruct's themselves.
    auto result = target->LocalUnrecognized_ ? ConvertTo<IMapNodePtr>(target->LocalUnrecognized_) : GetEphemeralNodeFactory()->CreateMap();
    for (const auto& [name, parameter] : Parameters_) {
        auto unrecognized = parameter->GetRecursiveUnrecognized(target);
        if (unrecognized && unrecognized->AsMap()->GetChildCount() > 0) {
            result->AddChild(name, unrecognized);
        }
    }
    return result;
}

template <class TStruct>
void TYsonStructMeta<TStruct>::RegisterParameter(TString key, IYsonStructParameterPtr parameter)
{
    YT_VERIFY(Parameters_.template emplace(std::move(key), std::move(parameter)).second);
}

template <class TStruct>
void TYsonStructMeta<TStruct>::RegisterPreprocessor(std::function<void(TYsonStructBase*)> preprocessor)
{
    Preprocessors_.push_back(std::move(preprocessor));
}

template <class TStruct>
void TYsonStructMeta<TStruct>::RegisterPostprocessor(std::function<void(TYsonStructBase*)> postprocessor)
{
    Postprocessors_.push_back(std::move(postprocessor));
}

template <class TStruct>
void TYsonStructMeta<TStruct>::SetUnrecognizedStrategy(EUnrecognizedStrategy strategy)
{
    MetaUnrecognizedStrategy_ = strategy;
}

template <class TStruct>
void TYsonStructMeta<TStruct>::FinishInitialization()
{
    RegisteredKeys_.reserve(Parameters_.size());
    for (const auto& [name, parameter] : Parameters_) {
        RegisteredKeys_.insert(name);
        for (const auto& alias : parameter->GetAliases()) {
            RegisteredKeys_.insert(alias);
        }
    }

    SortedParameters_ = std::vector<std::pair<TString, IYsonStructParameterPtr>>(Parameters_.begin(), Parameters_.end());
    std::sort(
        SortedParameters_.begin(),
        SortedParameters_.end(),
        [] (const auto& lhs, const auto& rhs) {
            return lhs.first < rhs.first;
        });
}

////////////////////////////////////////////////////////////////////////////////

template <class TStruct, class TValue>
TYsonFieldAccessor<TStruct, TValue>::TYsonFieldAccessor(TYsonStructField<TStruct, TValue> field)
    : Field_(field)
{ };

template <class TStruct, class TValue>
TValue& TYsonFieldAccessor<TStruct, TValue>::GetValue(const TYsonStructBase* source)
{
    return TYsonStructRegistry::Get()->template CachedDynamicCast<TStruct>(source)->*Field_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
TYsonStructParameter<TValue>::TYsonStructParameter(TString key, std::unique_ptr<IYsonFieldAccessor<TValue>> fieldAccessor)
    : Key_(std::move(key))
    , FieldAccessor_(std::move(fieldAccessor))
    , MergeStrategy_(EMergeStrategy::Default)
{ }

template <class TValue>
void TYsonStructParameter<TValue>::Load(
    TYsonStructBase* self,
    NYTree::INodePtr node,
    const TLoadParameterOptions& options)
{
    if (node) {
        NPrivate::LoadFromNode(
            FieldAccessor_->GetValue(self),
            std::move(node),
            options.Path,
            options.MergeStrategy.value_or(MergeStrategy_),
            options.KeepUnrecognizedRecursively);
    } else if (!DefaultConstructor_) {
        THROW_ERROR_EXCEPTION("Missing required parameter %v",
            options.Path);
    }
}

template <class TValue>
void TYsonStructParameter<TValue>::SafeLoad(
    TYsonStructBase* self,
    NYTree::INodePtr node,
    const TLoadParameterOptions& options,
    const std::function<void()>& validate)
{
    if (node) {
        TValue oldValue = FieldAccessor_->GetValue(self);
        try {
            NPrivate::LoadFromNode(FieldAccessor_->GetValue(self), node, options.Path, options.MergeStrategy.value_or(MergeStrategy_), false);
            validate();
        } catch (const std::exception ex) {
            FieldAccessor_->GetValue(self) = oldValue;
            throw;
        }
    }
}

template <class TValue>
void TYsonStructParameter<TValue>::Load(
    TYsonStructBase* self,
    NYson::TYsonPullParserCursor* cursor,
    const TLoadParameterOptions& options)
{
    if (cursor) {
        NPrivate::LoadFromCursor(
            FieldAccessor_->GetValue(self),
            cursor,
            options.Path,
            options.MergeStrategy.value_or(MergeStrategy_),
            options.KeepUnrecognizedRecursively);
    } else if (!DefaultConstructor_) {
        THROW_ERROR_EXCEPTION("Missing required parameter %v",
            options.Path);
    }
}

template <class TValue>
void TYsonStructParameter<TValue>::SafeLoad(
    TYsonStructBase* self,
    NYson::TYsonPullParserCursor* cursor,
    const TLoadParameterOptions& options,
    const std::function<void()>& validate)
{
    if (cursor) {
        TValue oldValue = FieldAccessor_->GetValue(self);
        try {
            NPrivate::LoadFromCursor(
                FieldAccessor_->GetValue(self),
                cursor,
                options.Path,
                options.MergeStrategy.value_or(MergeStrategy_),
                /*keepUnrecoginizedRecursively*/ false);
            validate();
        } catch (const std::exception ex) {
            FieldAccessor_->GetValue(self) = oldValue;
            throw;
        }
    }
}

template <class TValue>
void TYsonStructParameter<TValue>::Postprocess(const TYsonStructBase* self, const NYPath::TYPath& path) const
{
    const auto& value = FieldAccessor_->GetValue(self);
    for (const auto& postprocessor : Postprocessors_) {
        try {
            postprocessor(value);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Postprocess failed at %v",
                path.empty() ? "root" : path)
                    << ex;
        }
    }

    NPrivate::InvokeForComposites(
        &value,
        path,
        [] <NPrivate::IsYsonStructOrYsonSerializable T> (TIntrusivePtr<T> obj, const NYPath::TYPath& subpath) {
            if (obj) {
                obj->Postprocess(subpath);
            }
        });
}

template <class TValue>
void TYsonStructParameter<TValue>::SetDefaultsInitialized(TYsonStructBase* self)
{
    TValue& value = FieldAccessor_->GetValue(self);

    if (DefaultConstructor_) {
        value = (*DefaultConstructor_)();
    }

    NPrivate::InvokeForComposites(
        &value,
        [] <NPrivate::IsYsonStructOrYsonSerializable T> (TIntrusivePtr<T> obj) {
            if (obj) {
                obj->SetDefaults();
            }
        });
}

template <class TValue>
void TYsonStructParameter<TValue>::Save(const TYsonStructBase* self, NYson::IYsonConsumer* consumer) const
{
    using NYTree::Serialize;
    Serialize(FieldAccessor_->GetValue(self), consumer);
}

template <class TValue>
bool TYsonStructParameter<TValue>::CanOmitValue(const TYsonStructBase* self) const
{
    const auto& value = FieldAccessor_->GetValue(self);
    if constexpr (std::is_arithmetic_v<TValue> || std::is_same_v<TValue, TString>) {
        if (!SerializeDefault_ && value == (*DefaultConstructor_)()) {
            return true;
        }
    }

    if (!DefaultConstructor_) {
        return NYT::NYTree::NDetail::CanOmitValue(&value, nullptr);
    }

    if (IsTriviallyInitializedIntrusivePtr_) {
        return false;
    }

    auto defaultValue = (*DefaultConstructor_)();
    return NYT::NYTree::NDetail::CanOmitValue(&value, &defaultValue);
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::Alias(const TString& name)
{
    Aliases_.push_back(name);
    return *this;
}

template <class TValue>
const std::vector<TString>& TYsonStructParameter<TValue>::GetAliases() const
{
    return Aliases_;
}

template <class TValue>
const TString& TYsonStructParameter<TValue>::GetKey() const
{
    return Key_;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::Optional()
{
    DefaultConstructor_ = [] () { return TValue{}; };
    return *this;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::Default(TValue defaultValue)
{
    static_assert(!std::is_convertible_v<TValue, TIntrusivePtr<TYsonStruct>>, "Use DefaultCtor to register TYsonStruct default.");
    DefaultConstructor_ = [value = std::move(defaultValue)] () { return value; };
    return *this;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::Default()
{
    DefaultConstructor_ = [] () { return TValue{}; };
    return *this;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::DefaultCtor(std::function<TValue()> defaultCtor)
{
    DefaultConstructor_ = std::move(defaultCtor);
    return *this;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::DontSerializeDefault()
{
    // We should check for equality-comparability here but it is rather hard
    // to do the deep validation.
    static_assert(
        NPrivate::SupportsDontSerializeDefault<TValue>,
        "DontSerializeDefault requires |Parameter| to be TString, TDuration, an arithmetic type or an optional of those");

    SerializeDefault_ = false;
    return *this;
}

template <class TValue>
template <class... TArgs>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::DefaultNew(TArgs&&... args)
{
    IsTriviallyInitializedIntrusivePtr_ = true;
    return DefaultCtor([=] () { return New<typename TValue::TUnderlying>(std::forward<TArgs>(args)...); });
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::CheckThat(TPostprocessor postprocessor)
{
    Postprocessors_.push_back(std::move(postprocessor));
    return *this;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::MergeBy(EMergeStrategy strategy)
{
    MergeStrategy_ = strategy;
    return *this;
}

template <class TValue>
IMapNodePtr TYsonStructParameter<TValue>::GetRecursiveUnrecognized(const TYsonStructBase* self) const
{
    return NPrivate::TGetRecursiveUnrecognized<TValue>::Do(FieldAccessor_->GetValue(self));
}

////////////////////////////////////////////////////////////////////////////////
// Standard postprocessors

#define DEFINE_POSTPROCESSOR(method, condition, error) \
    template <class TValue> \
    TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::method \
    { \
        return CheckThat([=] (const TValue& parameter) { \
            using ::ToString; \
            std::optional<TValueType> nullableParameter(parameter); \
            if (nullableParameter) { \
                const auto& actual = *nullableParameter; \
                if (!(condition)) { \
                    THROW_ERROR error; \
                } \
            } \
        }); \
    }

DEFINE_POSTPROCESSOR(
    GreaterThan(TValueType expected),
    actual > expected,
    TError("Expected > %v, found %v", expected, actual)
)

DEFINE_POSTPROCESSOR(
    GreaterThanOrEqual(TValueType expected),
    actual >= expected,
    TError("Expected >= %v, found %v", expected, actual)
)

DEFINE_POSTPROCESSOR(
    LessThan(TValueType expected),
    actual < expected,
    TError("Expected < %v, found %v", expected, actual)
)

DEFINE_POSTPROCESSOR(
    LessThanOrEqual(TValueType expected),
    actual <= expected,
    TError("Expected <= %v, found %v", expected, actual)
)

DEFINE_POSTPROCESSOR(
    InRange(TValueType lowerBound, TValueType upperBound),
    lowerBound <= actual && actual <= upperBound,
    TError("Expected in range [%v,%v], found %v", lowerBound, upperBound, actual)
)

DEFINE_POSTPROCESSOR(
    NonEmpty(),
    actual.size() > 0,
    TError("Value must not be empty")
)

#undef DEFINE_POSTPROCESSOR

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
