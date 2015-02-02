#ifndef YSON_SERIALIZABLE_INL_H_
#error "Direct inclusion of this file is not allowed, include yson_serializable.h"
#endif
#undef YSON_SERIALIZABLE_INL_H_

#include "convert.h"
#include "tree_visitor.h"

#include <core/yson/consumer.h>

#include <core/misc/guid.h>
#include <core/misc/string.h>
#include <core/misc/nullable.h>
#include <core/misc/enum.h>
#include <core/misc/demangle.h>

#include <core/ypath/token.h>

#include <core/ytree/ypath_client.h>
#include <core/ytree/convert.h>

#include <core/actions/bind.h>

#include <util/datetime/base.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T, class = void>
struct TLoadHelper
{
    static void Load(T& parameter, NYTree::INodePtr node, const NYPath::TYPath& path)
    {
        try {
            Deserialize(parameter, node);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error reading parameter %v",
                path)
                << ex;
        }
    }
};

// TYsonSerializable
template <class T>
struct TLoadHelper<
    TIntrusivePtr<T>,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T&, TYsonSerializable&>>::TType
>
{
    static void Load(TIntrusivePtr<T>& parameter, NYTree::INodePtr node, const NYPath::TYPath& path)
    {
        if (!parameter) {
            parameter = New<T>();
        }
        parameter->Load(node, false, false, path);
    }
};

// TNullable
template <class T>
struct TLoadHelper<TNullable<T>, void>
{
    static void Load(TNullable<T>& parameter, NYTree::INodePtr node, const NYPath::TYPath& path)
    {
        if (node->GetType() == NYTree::ENodeType::Entity) {
            parameter = Null;
        } else {
            T value;
            TLoadHelper<T>::Load(value, node, path);
            parameter = value;
        }
    }
};

// std::vector
template <class T>
struct TLoadHelper<std::vector<T>, void>
{
    static void Load(std::vector<T>& parameter, NYTree::INodePtr node, const NYPath::TYPath& path)
    {
        auto listNode = node->AsList();
        auto size = listNode->GetChildCount();
        parameter.resize(size);
        for (int i = 0; i < size; ++i) {
            TLoadHelper<T>::Load(
                parameter[i],
                listNode->GetChild(i),
                path + "/" + NYPath::ToYPathLiteral(i));
        }
    }
};

// yhash_set
template <class T>
struct TLoadHelper<yhash_set<T>, void>
{
    static void Load(yhash_set<T>& parameter, NYTree::INodePtr node, const NYPath::TYPath& path)
    {
        auto listNode = node->AsList();
        int size = listNode->GetChildCount();
        for (int i = 0; i < size; ++i) {
            T value;
            TLoadHelper<T>::Load(
                value,
                listNode->GetChild(i),
                path + "/" +  NYPath::ToYPathLiteral(i));
            parameter.insert(std::move(value));
        }
    }
};

// yhash_map
template <class T>
struct TLoadHelper<yhash_map<Stroka, T>, void>
{
    static void Load(yhash_map<Stroka, T>& parameter, NYTree::INodePtr node, const NYPath::TYPath& path)
    {
        auto mapNode = node->AsMap();
        for (const auto& pair : mapNode->GetChildren()) {
            const auto& key = pair.first;
            T value;
            TLoadHelper<T>::Load(
                value,
                pair.second,
                path + "/" + NYPath::ToYPathLiteral(key));
            parameter.insert(std::make_pair(key, std::move(value)));
        }
    }
};

// map
template <class T>
struct TLoadHelper<std::map<Stroka, T>, void>
{
    static void Load(std::map<Stroka, T>& parameter, NYTree::INodePtr node, const NYPath::TYPath& path)
    {
        auto mapNode = node->AsMap();
        for (const auto& pair : mapNode->GetChildren()) {
            const auto& key = pair.first;
            T value;
            TLoadHelper<T>::Load(
                value,
                pair.second,
                path + "/" + NYPath::ToYPathLiteral(key));
            parameter.insert(std::make_pair(key, std::move(value)));
        }
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
template <class T, class F>
inline void InvokeForComposites(
    const TIntrusivePtr<T>* parameter,
    const NYPath::TYPath& path,
    const F& func,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T*, TYsonSerializable*>, int>::TType = 0)
{
    func(*parameter, path);
}

// std::vector
template <class T, class F>
inline void InvokeForComposites(
    const std::vector<T>* parameter,
    const NYPath::TYPath& path,
    const F& func)
{
    for (int i = 0; i < static_cast<int>(parameter->size()); ++i) {
        InvokeForComposites(
            &(*parameter)[i],
            path + "/" + NYPath::ToYPathLiteral(i),
            func);
    }
}

// yhash_map
template <class T, class F>
inline void InvokeForComposites(
    const yhash_map<Stroka, T>* parameter,
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
template <class T, class F>
inline void InvokeForComposites(
    const TIntrusivePtr<T>* parameter,
    const F& func,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T*, TYsonSerializable*>, int>::TType = 0)
{
    func(*parameter);
}

// std::vector
template <class T, class F>
inline void InvokeForComposites(
    const std::vector<T>* parameter,
    const F& func)
{
    for (int i = 0; i < static_cast<int>(parameter->size()); ++i) {
        InvokeForComposites(
            &(*parameter)[i],
            func);
    }
}

// yhash_map
template <class T, class F>
inline void InvokeForComposites(
    const yhash_map<Stroka, T>* parameter,
    const F& func)
{
    for (const auto& pair : *parameter) {
        InvokeForComposites(
            &pair.second,
            func);
    }
}

////////////////////////////////////////////////////////////////////////////////

// all
inline bool HasValue(const void* /* parameter */)
{
    return true;
}

// TIntrusivePtr
template <class T>
inline bool HasValue(TIntrusivePtr<T>* parameter)
{
    return (bool) (*parameter);
}

// TNullable
template <class T>
inline bool HasValue(TNullable<T>* parameter)
{
    return parameter->HasValue();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

template <class T>
TYsonSerializableLite::TParameter<T>::TParameter(T& parameter)
    : Parameter(parameter)
    , Description(nullptr)
{ }

template <class T>
void TYsonSerializableLite::TParameter<T>::Load(NYTree::INodePtr node, const NYPath::TYPath& path)
{
    if (node) {
        NYT::NYTree::NDetail::TLoadHelper<T>::Load(Parameter, node, path);
    } else if (!DefaultValue) {
        THROW_ERROR_EXCEPTION("Missing required parameter %v",
            path);
    }
}

template <class T>
void TYsonSerializableLite::TParameter<T>::Validate(const NYPath::TYPath& path) const
{
    for (const auto& validator : Validators) {
        try {
            validator(Parameter);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Validation failed at %v",
                path.empty() ? "/" : path)
                << ex;
        }
    }

    NYT::NYTree::NDetail::InvokeForComposites(
        &Parameter,
        path,
        [] (TIntrusivePtr<TYsonSerializable> obj, const NYPath::TYPath& subpath) {
            if (obj) {
                obj->Validate(subpath);
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
bool TYsonSerializableLite::TParameter<T>::HasValue() const
{
    return NYT::NYTree::NDetail::HasValue(&Parameter);
}

template <class T>
TYsonSerializableLite::TParameter<T>& TYsonSerializableLite::TParameter<T>::Describe(const char* description)
{
    Description = description;
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
TYsonSerializableLite::TParameter<T>& TYsonSerializableLite::TParameter<T>::DefaultNew()
{
    return Default(New<typename T::TUnderlying>());
}

template <class T>
TYsonSerializableLite::TParameter<T>& TYsonSerializableLite::TParameter<T>::CheckThat(TValidator validator)
{
    Validators.push_back(std::move(validator));
    return *this;
}

////////////////////////////////////////////////////////////////////////////////
// Standard validators

#define DEFINE_VALIDATOR(method, condition, error) \
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

DEFINE_VALIDATOR(
    GreaterThan(TValueType expected),
    actual > expected,
    TError("Expected > %v, found %v", expected, actual)
)

DEFINE_VALIDATOR(
    GreaterThanOrEqual(TValueType expected),
    actual >= expected,
    TError("Expected >= %v, found %v", expected, actual)
)

DEFINE_VALIDATOR(
    LessThan(TValueType expected),
    actual < expected,
    TError("Expected < %v, found %v", expected, actual)
)

DEFINE_VALIDATOR(
    LessThanOrEqual(TValueType expected),
    actual <= expected,
    TError("Expected <= %v, found %v", expected, actual)
)

DEFINE_VALIDATOR(
    InRange(TValueType lowerBound, TValueType upperBound),
    lowerBound <= actual && actual <= upperBound,
    TError("Expected in range [%v,%v], found %v", lowerBound, upperBound, actual)
)

DEFINE_VALIDATOR(
    NonEmpty(),
    actual.size() > 0,
    TError("Expected non-empty collection")
)

#undef DEFINE_VALIDATOR

////////////////////////////////////////////////////////////////////////////////

template <class T>
TYsonSerializableLite::TParameter<T>& TYsonSerializableLite::RegisterParameter(
    const Stroka& parameterName,
    T& value)
{
    auto parameter = New<TParameter<T>>(value);
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
        return ConvertTo<TIntrusivePtr<T>>(UpdateNode(ConvertTo<INodePtr>(obj), patch));
    } else {
        return CloneYsonSerializable(obj);
    }
}

template <class T>
bool ReconfigureYsonSerializable(
    TIntrusivePtr<T> config,
    const NYTree::TYsonString& newConfigYson)
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

} // namespace NYT
} // namespace NYTree
