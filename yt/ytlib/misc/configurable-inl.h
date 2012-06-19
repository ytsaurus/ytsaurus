#ifndef CONFIGURABLE_INL_H_
#error "Direct inclusion of this file is not allowed, include configurable.h"
#endif
#undef CONFIGURABLE_INL_H_

#include "guid.h"
#include "string.h"
#include "nullable.h"
#include "enum.h"
#include "demangle.h"

#include <ytlib/actions/bind.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/tree_visitor.h>
#include <ytlib/ytree/yson_consumer.h>

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
        NYTree::Read(parameter, node);
    }
};

// TConfigurable
template <class T>
struct TLoadHelper<
    T,
    typename NMpl::TEnableIf< NMpl::TIsConvertible<T*, TConfigurable*> >::TType
>
{
    static void Load(T& parameter, NYTree::INodePtr node, const NYTree::TYPath& path)
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

// TODO(panin): kill this once we get rid of yvector
// yvector
template <class T>
struct TLoadHelper<yvector<T>, void>
{
    static void Load(yvector<T>& parameter, NYTree::INodePtr node, const NYTree::TYPath& path)
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
                path + "/" + NYTree::SerializeToYson(key));
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

// TConfigurable
template <class T>
inline void ValidateSubconfigs(
    const TIntrusivePtr<T>* parameter,
    const NYTree::TYPath& path,
    typename NMpl::TEnableIf<NMpl::TIsConvertible< T*, TConfigurable* >, int>::TType = 0)
{
    if (*parameter) {
        (*parameter)->Validate(path);
    }
}

// yvector
template <class T>
inline void ValidateSubconfigs(
    const yvector<T>* parameter,
    const NYTree::TYPath& path)
{
    for (int i = 0; i < parameter->ysize(); ++i) {
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
            ythrow yexception()
                << Sprintf("Could not read parameter (Path: %s)\n%s",
                    ~path,
                    ex.what());
        }
    } else if (!HasDefaultValue) {
        ythrow yexception()
            << Sprintf("Required parameter is missing (Path: %s)", ~path);
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
            ythrow yexception()
                << Sprintf("Validation failed (Path: %s)\n%s",
                    ~path,
                    ex.what());
        }
    }
}

template <class T>
void TParameter<T>::Save(NYTree::IYsonConsumer* consumer) const
{
    NYTree::Write(Parameter, consumer);
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

#define DEFINE_VALIDATOR(method, condition, ex) \
    template <class T> \
    TParameter<T>& TParameter<T>::method \
    { \
        return CheckThat(BIND([=] (const T& parameter) { \
            TNullable<TValueType> nullableParameter(parameter); \
            if (nullableParameter) { \
                const TValueType& actual = nullableParameter.Get(); \
                if (!(condition)) { \
                    ythrow (ex); \
                } \
            } \
        })); \
    }

DEFINE_VALIDATOR(
    GreaterThan(TValueType expected),
    actual > expected,
    yexception() << "Validation failure: expected >" << expected << ", found " << actual)

DEFINE_VALIDATOR(
    GreaterThanOrEqual(TValueType expected),
    actual >= expected,
    yexception() << "Validation failure: expected >=" << expected << ", found " << actual)

DEFINE_VALIDATOR(
    LessThan(TValueType expected),
    actual < expected,
    yexception() << "Validation failure: expected <" << expected << ", found " << actual)

DEFINE_VALIDATOR(
    LessThanOrEqual(TValueType expected),
    actual <= expected,
    yexception() << "Validation failure: expected <=" << expected << ", found " << actual)

DEFINE_VALIDATOR(
    InRange(TValueType lowerBound, TValueType upperBound),
    lowerBound <= actual && actual <= upperBound,
    yexception() << "Validation failure: expected in range ["<< lowerBound << ", " << upperBound << "], found " << actual)

DEFINE_VALIDATOR(
    NonEmpty(),
    actual.size() > 0,
    yexception() << "Validation failure: expected non-empty")

#undef DEFINE_VALIDATOR

////////////////////////////////////////////////////////////////////////////////

} // namespace NConfig

////////////////////////////////////////////////////////////////////////////////

template <class T>
NConfig::TParameter<T>& TConfigurable::Register(const Stroka& parameterName, T& value)
{
    auto parameter = New< NConfig::TParameter<T> >(value);
    YVERIFY(Parameters.insert(
        TPair<Stroka, NConfig::IParameterPtr>(parameterName, parameter)).second);
    return *parameter;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
inline TIntrusivePtr<T> CloneConfigurable(TIntrusivePtr<T> obj)
{
    return NYTree::DeserializeFromYson<T>(NYTree::SerializeToYson(obj));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
