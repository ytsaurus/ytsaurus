#pragma once
#ifndef YSON_SERIALIZABLE_INL_H_
#error "Direct inclusion of this file is not allowed, include yson_serializable.h"
#endif

#include "convert.h"
#include "serialize.h"
#include "tree_visitor.h"

#include <yt/core/yson/consumer.h>

#include <yt/core/misc/guid.h>
#include <yt/core/misc/string.h>
#include <yt/core/misc/nullable.h>
#include <yt/core/misc/enum.h>
#include <yt/core/misc/demangle.h>
#include <yt/core/misc/serialize.h>

#include <yt/core/ypath/token.h>

#include <yt/core/ytree/ypath_client.h>
#include <yt/core/ytree/convert.h>

#include <yt/core/actions/bind.h>

#include <util/datetime/base.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

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
            Y_UNIMPLEMENTED();
    }
}

// TYsonSerializable
template <class T, class E = typename std::enable_if<std::is_convertible<T&, TYsonSerializable&>::value>::type>
void LoadFromNode(
    TIntrusivePtr<T>& parameter,
    NYTree::INodePtr node,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively)
{
    if (!parameter || mergeStrategy == EMergeStrategy::Overwrite) {
        parameter = New<T>();
    }

    if (keepUnrecognizedRecursively) {
        parameter->SetUnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);
    }

    switch (mergeStrategy) {
        case EMergeStrategy::Default:
        case EMergeStrategy::Overwrite:
        case EMergeStrategy::Combine: {
            parameter->Load(node, false, false, path);
            break;
        }

        default:
            Y_UNIMPLEMENTED();
    }
}

// TNullable
template <class T>
void LoadFromNode(
    TNullable<T>& parameter,
    NYTree::INodePtr node,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively)
{
    switch (mergeStrategy) {
        case EMergeStrategy::Default:
        case EMergeStrategy::Overwrite: {
            if (node->GetType() == NYTree::ENodeType::Entity) {
                parameter = Null;
            } else {
                T value;
                LoadFromNode(value, node, path, EMergeStrategy::Overwrite, keepUnrecognizedRecursively);
                parameter = value;
            }
            break;
        }

        default:
            Y_UNIMPLEMENTED();
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
                    listNode->GetChild(i),
                    path + "/" + NYPath::ToYPathLiteral(i),
                    EMergeStrategy::Overwrite,
                    keepUnrecognizedRecursively);
            }
            break;
        }

        default:
            Y_UNIMPLEMENTED();
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
            for (const auto& pair : mapNode->GetChildren()) {
                const auto& key = pair.first;
                M value;
                LoadFromNode(
                    value,
                    pair.second,
                    path + "/" + NYPath::ToYPathLiteral(key),
                    EMergeStrategy::Overwrite,
                    keepUnrecognizedRecursively);
                parameter.emplace(FromString<typename Map<T...>::key_type>(key), std::move(value));
            }
            break;
        }
        case EMergeStrategy::Combine: {
            auto mapNode = node->AsMap();
            for (const auto& pair : mapNode->GetChildren()) {
                const auto& key = pair.first;
                M value;
                LoadFromNode(
                    value,
                    pair.second,
                    path + "/" + NYPath::ToYPathLiteral(key),
                    EMergeStrategy::Combine,
                    keepUnrecognizedRecursively);
                parameter[FromString<typename Map<T...>::key_type>(key)] = std::move(value);
            }
            break;
        }

        default:
            Y_UNIMPLEMENTED();
    }
}

// For all classes except descendants of TYsonSerializableLite and their intrusive pointers
// we do not attempt to extract unrecognzied members. C++ prohibits function template specialization
// so we have to deal with static struct members.
template <class T, class = void>
struct TGetUnrecognizedRecursively
{
    static IMapNodePtr Do(const T& /*parameter*/)
    {
        return nullptr;
    }
};

template <class T>
struct TGetUnrecognizedRecursively<T, std::enable_if_t<std::is_base_of<TYsonSerializableLite, T>::value>>
{
    static IMapNodePtr Do(const T& parameter)
    {
        return parameter.GetUnrecognizedRecursively();
    }
};

template <class T>
struct TGetUnrecognizedRecursively<T, std::enable_if_t<std::is_base_of<TYsonSerializableLite, typename T::TUnderlying>::value>>
{
    static IMapNodePtr Do(const T& parameter)
    {
        return parameter ? parameter->GetUnrecognizedRecursively() : nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

// all
template <class F>
void InvokeForComposites(
    const void* /* parameter */,
    const NYPath::TYPath& /* path */,
    const F& /* func */)
{ }

// TYsonSerializable
template <class T, class F, class E = typename std::enable_if<std::is_convertible<T*, TYsonSerializable*>::value>::type>
inline void InvokeForComposites(
    const TIntrusivePtr<T>* parameter,
    const NYPath::TYPath& path,
    const F& func)
{
    func(*parameter, path);
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
template <template<typename...> class Map, class... T, class F, class M = typename Map<T...>::mapped_type>
inline void InvokeForComposites(
    const Map<T...>* parameter,
    const NYPath::TYPath& path,
    const F& func)
{
    for (const auto& pair : *parameter) {
        InvokeForComposites(
            &pair.second,
            path + "/" + NYPath::ToYPathLiteral(pair.first),
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

// TYsonSerializable
template <class T, class F, class E = typename std::enable_if<std::is_convertible<T*, TYsonSerializable*>::value>::type>
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
template <template<typename...> class Map, class... T, class F, class M = typename Map<T...>::mapped_type>
inline void InvokeForComposites(const Map<T...>* parameter, const F& func)
{
    for (const auto& pair : *parameter) {
        InvokeForComposites(&pair.second, func);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

template <class T>
TYsonSerializableLite::TParameter<T>::TParameter(T& parameter)
    : Parameter(parameter)
    , MergeStrategy(EMergeStrategy::Default)
{ }

template <class T>
void TYsonSerializableLite::TParameter<T>::Load(NYTree::INodePtr node, const NYPath::TYPath& path)
{
    if (node) {
        NDetail::LoadFromNode(Parameter, node, path, MergeStrategy, KeepUnrecognizedRecursively);
    } else if (!DefaultValue) {
        THROW_ERROR_EXCEPTION("Missing required parameter %v",
            path);
    }
}

template <class T>
void TYsonSerializableLite::TParameter<T>::Postprocess(const NYPath::TYPath& path) const
{
    for (const auto& postprocessor : Postprocessors) {
        try {
            postprocessor(Parameter);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Postprocess failed at %v",
                path.empty() ? "root" : path)
                << ex;
        }
    }

    NYT::NYTree::NDetail::InvokeForComposites(
        &Parameter,
        path,
        [] (TIntrusivePtr<TYsonSerializable> obj, const NYPath::TYPath& subpath) {
            if (obj) {
                obj->Postprocess(subpath);
            }
        });
}

template <class T>
void TYsonSerializableLite::TParameter<T>::SetDefaults()
{
    if (DefaultValue) {
        Parameter = *DefaultValue;
    }

    NYT::NYTree::NDetail::InvokeForComposites(
        &Parameter,
        [] (TIntrusivePtr<TYsonSerializable> obj) {
            if (obj) {
                obj->SetDefaults();
            }
        });
}

template <class T>
void TYsonSerializableLite::TParameter<T>::Save(NYson::IYsonConsumer* consumer) const
{
    using NYTree::Serialize;
    Serialize(Parameter, consumer);
}

template <class T>
bool TYsonSerializableLite::TParameter<T>::CanOmitValue() const
{
    return NYT::NYTree::NDetail::CanOmitValue(&Parameter, DefaultValue.GetPtr());
}

template <class T>
TYsonSerializableLite::TParameter<T>& TYsonSerializableLite::TParameter<T>::Alias(const TString& name)
{
    Aliases.push_back(name);
    return *this;
}

template <class T>
const std::vector<TString>& TYsonSerializableLite::TParameter<T>::GetAliases() const
{
    return Aliases;
}

template <class T>
TYsonSerializableLite::TParameter<T>& TYsonSerializableLite::TParameter<T>::Optional()
{
    DefaultValue = Parameter;
    return *this;
}

template <class T>
TYsonSerializableLite::TParameter<T>& TYsonSerializableLite::TParameter<T>::Default(const T& defaultValue)
{
    DefaultValue = defaultValue;
    Parameter = defaultValue;
    return *this;
}

template <class T>
template <class... TArgs>
TYsonSerializableLite::TParameter<T>& TYsonSerializableLite::TParameter<T>::DefaultNew(TArgs&&... args)
{
    return Default(New<typename T::TUnderlying>(std::forward<TArgs>(args)...));
}

template <class T>
TYsonSerializableLite::TParameter<T>& TYsonSerializableLite::TParameter<T>::CheckThat(TPostprocessor postprocessor)
{
    Postprocessors.push_back(std::move(postprocessor));
    return *this;
}

template <class T>
TYsonSerializableLite::TParameter<T>& TYsonSerializableLite::TParameter<T>::MergeBy(EMergeStrategy strategy)
{
    MergeStrategy = strategy;
    return *this;
}

template <class T>
IMapNodePtr TYsonSerializableLite::TParameter<T>::GetUnrecognizedRecursively() const
{
    return NDetail::TGetUnrecognizedRecursively<T>::Do(Parameter);
}

template <class T>
void TYsonSerializableLite::TParameter<T>::SetKeepUnrecognizedRecursively()
{
    KeepUnrecognizedRecursively = true;
}

////////////////////////////////////////////////////////////////////////////////
// Standard postprocessors

#define DEFINE_POSTPROCESSOR(method, condition, error) \
    template <class T> \
    TYsonSerializableLite::TParameter<T>& TYsonSerializableLite::TParameter<T>::method \
    { \
        return CheckThat([=] (const T& parameter) { \
            using ::ToString; \
            TNullable<TValueType> nullableParameter(parameter); \
            if (nullableParameter) { \
                const auto& actual = nullableParameter.Get(); \
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

template <class T>
TYsonSerializableLite::TParameter<T>& TYsonSerializableLite::RegisterParameter(
    const TString& parameterName,
    T& value)
{
    auto parameter = New<TParameter<T>>(value);
    if (UnrecognizedStrategy == EUnrecognizedStrategy::KeepRecursive) {
        parameter->SetKeepUnrecognizedRecursively();
    }
    YCHECK(Parameters.insert(std::make_pair(parameterName, parameter)).second);
    return *parameter;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
inline TIntrusivePtr<T> CloneYsonSerializable(TIntrusivePtr<T> obj)
{
    return NYTree::ConvertTo<TIntrusivePtr<T>>(NYTree::ConvertToYsonString(*obj));
}

template <class T>
TIntrusivePtr<T> UpdateYsonSerializable(
    TIntrusivePtr<T> obj,
    NYTree::INodePtr patch)
{
    static_assert(
        NMpl::TIsConvertible<T*, TYsonSerializable*>::Value,
        "'obj' must be convertible to TYsonSerializable");

    using NYTree::INodePtr;
    using NYTree::ConvertTo;

    if (patch) {
        return ConvertTo<TIntrusivePtr<T>>(PatchNode(ConvertTo<INodePtr>(obj), patch));
    } else {
        return CloneYsonSerializable(obj);
    }
}

template <class T>
TIntrusivePtr<T> UpdateYsonSerializable(
    TIntrusivePtr<T> obj,
    const NYson::TYsonString& patch)
{
    if (!patch) {
        return obj;
    }

    return UpdateYsonSerializable(obj, ConvertToNode(patch));
}

template <class T>
bool ReconfigureYsonSerializable(
    TIntrusivePtr<T> config,
    const NYson::TYsonString& newConfigYson)
{
    auto newConfig = ConvertToNode(newConfigYson);
    return ReconfigureYsonSerializable(config, newConfig);
}

template <class T>
bool ReconfigureYsonSerializable(
    TIntrusivePtr<T> config,
    NYTree::INodePtr newConfigNode)
{
    auto configNode = NYTree::ConvertToNode(config);

    auto newConfig = NYTree::ConvertTo<TIntrusivePtr<T>>(newConfigNode);
    auto newCanonicalConfigNode = NYTree::ConvertToNode(newConfig);

    if (NYTree::AreNodesEqual(configNode, newCanonicalConfigNode)) {
        return false;
    }

    config->Load(newConfigNode);
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
