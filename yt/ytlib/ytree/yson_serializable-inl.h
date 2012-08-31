#ifndef YSON_SERIALIZABLE_INL_H_
#error "Direct inclusion of this file is not allowed, include yson_serializable.h"
#endif
#undef YSON_SERIALIZABLE_INL_H_

#include "convert.h"
#include "tree_visitor.h"
#include "yson_consumer.h"
#include "ypath_client.h"

#include <ytlib/misc/guid.h>
#include <ytlib/misc/string.h>
#include <ytlib/misc/nullable.h>
#include <ytlib/misc/enum.h>
#include <ytlib/misc/demangle.h>
#include <ytlib/misc/error.h>

#include <ytlib/actions/bind.h>

#include <util/datetime/base.h>

// Avoid circular references.
namespace NYT {
namespace NYTree {
    TYPath EscapeYPathToken(const Stroka& value);
    TYPath EscapeYPathToken(i64 value);
}
}

namespace NYT {
namespace NConfig {

////////////////////////////////////////////////////////////////////////////////

template <class T, class = void>
struct TLoadHelper
{
    static void Load(T& parameter, NYTree::INodePtr node, const NYTree::TYPath& path)
    {
        UNUSED(path);
        NYTree::Deserialize(parameter, node);
    }
};

// TYsonSerializable
template <class T>
struct TLoadHelper<
    TIntrusivePtr<T>,
    typename NMpl::TEnableIf< NMpl::TIsConvertible<T&, TYsonSerializable&> >::TType
>
{
    static void Load(TIntrusivePtr<T>& parameter, NYTree::INodePtr node, const NYTree::TYPath& path)
    {
        if (!parameter) {
            parameter = New<T>();
        }
        parameter->Load(node, false, path);
    }
};

// TNullable
template <class T>
struct TLoadHelper<TNullable<T>, void>
{
    static void Load(TNullable<T>& parameter, NYTree::INodePtr node, const NYTree::TYPath& path)
    {
        T value;
        TLoadHelper<T>::Load(value, node, path);
        parameter = value;
    }
};

// std::vector
template <class T>
struct TLoadHelper<std::vector<T>, void>
{
    static void Load(std::vector<T>& parameter, NYTree::INodePtr node, const NYTree::TYPath& path)
    {
        auto listNode = node->AsList();
        auto size = listNode->GetChildCount();
        parameter.resize(size);
        for (int i = 0; i < size; ++i) {
            TLoadHelper<T>::Load(
                parameter[i],
                listNode->GetChild(i),
                path + "/" + NYTree::EscapeYPathToken(i));
        }
    }
};

// yhash_set
template <class T>
struct TLoadHelper<yhash_set<T>, void>
{
    static void Load(yhash_set<T>& parameter, NYTree::INodePtr node, const NYTree::TYPath& path)
    {
        auto listNode = node->AsList();
        auto size = listNode->GetChildCount();
        for (int i = 0; i < size; ++i) {
            T value;
            TLoadHelper<T>::Load(
                value,
                listNode->GetChild(i),
                path + "/" +  NYTree::EscapeYPathToken(i));
            parameter.insert(MoveRV(value));
        }
    }
};

// yhash_map
template <class T>
struct TLoadHelper<yhash_map<Stroka, T>, void>
{
    static void Load(yhash_map<Stroka, T>& parameter, NYTree::INodePtr node, const NYTree::TYPath& path)
    {
        auto mapNode = node->AsMap();
        FOREACH (const auto& pair, mapNode->GetChildren()) {
            auto& key = pair.first;
            T value;
            TLoadHelper<T>::Load(
                value,
                pair.second,
                path + "/" + NYTree::YsonizeString(key, NYTree::EYsonFormat::Binary));
            parameter.insert(MakePair(key, MoveRV(value)));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

// all
inline void ValidateSubconfigs(
    const void* /* parameter */,
    const NYTree::TYPath& /* path */)
{ }

// TYsonSerializable
template <class T>
inline void ValidateSubconfigs(
    const TIntrusivePtr<T>* parameter,
    const NYTree::TYPath& path,
    typename NMpl::TEnableIf<NMpl::TIsConvertible< T*, TYsonSerializable* >, int>::TType = 0)
{
    if (*parameter) {
        (*parameter)->Validate(path);
    }
}

// std::vector
template <class T>
inline void ValidateSubconfigs(
    const std::vector<T>* parameter,
    const NYTree::TYPath& path)
{
    for (int i = 0; i < parameter->size(); ++i) {
        ValidateSubconfigs(
            &(*parameter)[i],
            path + "/" + NYTree::EscapeYPathToken(i));
    }
}

// yhash_map
template <class T>
inline void ValidateSubconfigs(
    const yhash_map<Stroka, T>* parameter,
    const NYTree::TYPath& path)
{
    FOREACH (const auto& pair, *parameter) {
        ValidateSubconfigs(
            &pair.second,
            path + "/" + NYTree::EscapeYPathToken(pair.first));
    }
}

////////////////////////////////////////////////////////////////////////////////

// all
inline bool IsPresent(const void* /* parameter */)
{
    return true;
}

// TIntrusivePtr
template <class T>
inline bool IsPresent(TIntrusivePtr<T>* parameter)
{
    return (bool) (*parameter);
}

// TNullable
template <class T>
inline bool IsPresent(TNullable<T>* parameter)
{
    return parameter->IsInitialized();
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TParameter<T>::TParameter(T& parameter)
    : Parameter(parameter)
    , HasDefaultValue(false)
{ }

template <class T>
void TParameter<T>::Load(NYTree::INodePtr node, const NYTree::TYPath& path)
{
    if (node) {
        try {
            TLoadHelper<T>::Load(Parameter, node, path);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error reading parameter %s", ~path)
                << ex;
        }
    } else if (!HasDefaultValue) {
        THROW_ERROR_EXCEPTION("Required parameter is missing: %s", ~path);
    }
}

template <class T>
void TParameter<T>::Validate(const NYTree::TYPath& path) const
{
    ValidateSubconfigs(&Parameter, path);
    FOREACH (const auto& validator, Validators) {
        try {
            validator.Run(Parameter);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Validation failed: %s", ~path)
                << ex;
        }
    }
}

template <class T>
void TParameter<T>::Save(NYTree::IYsonConsumer* consumer) const
{
    NYTree::Serialize(Parameter, consumer);
}

template <class T>
bool TParameter<T>::IsPresent() const
{
    return NConfig::IsPresent(&Parameter);
}

template <class T>
TParameter<T>& TParameter<T>::Default(const T& defaultValue)
{
    Parameter = defaultValue;
    HasDefaultValue = true;
    return *this;
}

template <class T>
TParameter<T>& TParameter<T>::Default(T&& defaultValue)
{
    Parameter = MoveRV(defaultValue);
    HasDefaultValue = true;
    return *this;
}

template <class T>
TParameter<T>& TParameter<T>::DefaultNew()
{
    return Default(New<typename T::TElementType>());
}

template <class T>
TParameter<T>& TParameter<T>::CheckThat(TValidator validator)
{
    Validators.push_back(MoveRV(validator));
    return *this;
}

////////////////////////////////////////////////////////////////////////////////
// Standard validators

#define DEFINE_VALIDATOR(method, condition, error) \
    template <class T> \
    TParameter<T>& TParameter<T>::method \
    { \
        return CheckThat(BIND([=] (const T& parameter) { \
            TNullable<TValueType> nullableParameter(parameter); \
            if (nullableParameter) { \
                const TValueType& actual = nullableParameter.Get(); \
                if (!(condition)) { \
                    THROW_ERROR error; \
                } \
            } \
        })); \
    }

DEFINE_VALIDATOR(
    GreaterThan(TValueType expected),
    actual > expected,
    TError("Validation failure: expected > %s, found %s", ~ToString(expected), ~ToString(actual))
)

DEFINE_VALIDATOR(
    GreaterThanOrEqual(TValueType expected),
    actual >= expected,
    TError("Validation failure: expected >= %s, found %s", ~ToString(expected), ~ToString(actual))
)

DEFINE_VALIDATOR(
    LessThan(TValueType expected),
    actual < expected,
    TError("Validation failure: expected < %s, found %s", ~ToString(expected), ~ToString(actual))
)

DEFINE_VALIDATOR(
    LessThanOrEqual(TValueType expected),
    actual <= expected,
    TError("Validation failure: expected <= %s, found %s", ~ToString(expected), ~ToString(actual))
)

DEFINE_VALIDATOR(
    InRange(TValueType lowerBound, TValueType upperBound),
    lowerBound <= actual && actual <= upperBound,
    TError("Validation failure: expected in range [%s, %s], found %s", ~ToString(lowerBound), ~ToString(upperBound), ~ToString(actual))
)

DEFINE_VALIDATOR(
    NonEmpty(),
    actual.size() > 0,
    TError("Validation failure: expected non-empty")
)

#undef DEFINE_VALIDATOR

////////////////////////////////////////////////////////////////////////////////

} // namespace NConfig

////////////////////////////////////////////////////////////////////////////////

template <class T>
NConfig::TParameter<T>& TYsonSerializable::Register(const Stroka& parameterName, T& value)
{
    auto parameter = New< NConfig::TParameter<T> >(value);
    YCHECK(Parameters.insert(
        TPair<Stroka, NConfig::IParameterPtr>(parameterName, parameter)).second);
    return *parameter;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
inline TIntrusivePtr<T> CloneYsonSerializable(TIntrusivePtr<T> obj)
{
    return NYTree::ConvertTo< TIntrusivePtr<T> >(NYTree::ConvertToYsonString(*obj));
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
