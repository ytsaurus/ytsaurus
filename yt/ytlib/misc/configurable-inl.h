#ifndef CONFIGURABLE_INL_H_
#error "Direct inclusion of this file is not allowed, include configurable.h"
#endif
#undef CONFIGURABLE_INL_H_

#include "guid.h"
#include "string.h"

#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/tree_visitor.h>

#include <util/datetime/base.h>

namespace NYT {
namespace NConfig {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T CheckedStaticCast(i64 value)
{
    if (value < Min<T>() || value > Max<T>()) {
        ythrow yexception()
            << Sprintf("Argument is out of integral range (Value: %" PRId64 ")", value);
    }
    return static_cast<T>(value);
}

// TConfigurable::TPtr
template <class T>
inline void Read(
    TIntrusivePtr<T>& parameter,
    const NYTree::INode* node,
    const NYTree::TYPath& path,
    typename NMpl::TEnableIf<NMpl::TIsConvertible< T*, TConfigurable* >, int>::TType = 0)
{
    if (!parameter) {
        parameter = New<T>();
    }
    parameter->Load(node, path);
}

// i64
inline void Read(i64& parameter, const NYTree::INode* node, const NYTree::TYPath& /* path */)
{
    parameter = node->AsInt64()->GetValue();
}

// i32
inline void Read(i32& parameter, const NYTree::INode* node, const NYTree::TYPath& /* path */)
{
    parameter = CheckedStaticCast<i32>(node->AsInt64()->GetValue());
}

// ui32
inline void Read(ui32& parameter, const NYTree::INode* node, const NYTree::TYPath& /* path */)
{
    parameter = CheckedStaticCast<ui32>(node->AsInt64()->GetValue());
}

// ui16
inline void Read(ui16& parameter, const NYTree::INode* node, const NYTree::TYPath& /* path */)
{
    parameter = CheckedStaticCast<ui16>(node->AsInt64()->GetValue());
}

// double
inline void Read(double& parameter, const NYTree::INode* node, const NYTree::TYPath& /* path */)
{
    parameter = node->AsDouble()->GetValue();
}

// Stroka
inline void Read(Stroka& parameter, const NYTree::INode* node, const NYTree::TYPath& /* path */)
{
    parameter = node->AsString()->GetValue();
}

// bool
inline void Read(bool& parameter, const NYTree::INode* node, const NYTree::TYPath& /* path */)
{
    Stroka value = node->AsString()->GetValue();
    parameter = ParseBool(value);
}

// TDuration
inline void Read(TDuration& parameter, const NYTree::INode* node, const NYTree::TYPath& /* path */)
{
    parameter = TDuration::MilliSeconds(node->AsInt64()->GetValue());
}

// TGuid
inline void Read(TGuid& parameter, const NYTree::INode* node, const NYTree::TYPath& /* path */)
{
    parameter = TGuid::FromString(node->AsString()->GetValue());
}

// TEnumBase
template <class T>
inline void Read(
    T& parameter,
    const NYTree::INode* node, 
    const NYTree::TYPath& /* path */,
    typename NMpl::TEnableIf<NMpl::TIsConvertible< T, TEnumBase<T> >, int>::TType = 0)
{
    auto value = node->AsString()->GetValue();
    parameter = ParseEnum<T>(value);
}

// INode::TPtr
inline void Read(
    NYTree::INode::TPtr& parameter,
    const NYTree::INode* node,
    const NYTree::TYPath& /* path */)
{
    parameter = const_cast<NYTree::INode*>(node);
}

// yvector
template <class T>
inline void Read(yvector<T>& parameter, const NYTree::INode* node, const NYTree::TYPath& path)
{
    auto listNode = node->AsList();
    auto size = listNode->GetChildCount();
    parameter.resize(size);
    for (int i = 0; i < size; ++i) {
        Read(parameter[i], ~listNode->GetChild(i), NYTree::CombineYPaths(path, ToString(i)));
    }
}

// yhash_set
template <class T>
inline void Read(yhash_set<T>& parameter, const NYTree::INode* node, const NYTree::TYPath& path)
{
    auto listNode = node->AsList();
    auto size = listNode->GetChildCount();
    for (int i = 0; i < size; ++i) {
        T value;
        Read(value, ~listNode->GetChild(i), NYTree::CombineYPaths(path, ToString(i)));
        parameter.insert(MoveRV(value));
    }
}

// yhash_map
template <class T>
inline void Read(yhash_map<Stroka, T>& parameter, const NYTree::INode* node, const NYTree::TYPath& path)
{
    auto mapNode = node->AsMap();
    FOREACH (const auto& pair, mapNode->GetChildren()) {
        auto& key = pair.First();
        T value;
        Read(value, ~pair.Second(), NYTree::CombineYPaths(path, key));
        parameter.insert(MakePair(key, MoveRV(value)));
    }
}

////////////////////////////////////////////////////////////////////////////////

// TConfigurable::TPtr
template <class T>
inline void Write(
    const TIntrusivePtr<T>& parameter,
    NYTree::IYsonConsumer* consumer,
    typename NMpl::TEnableIf<NMpl::TIsConvertible< T*, TConfigurable* >, int>::TType = 0)
{
    if (parameter) {
        parameter->Save(consumer);
    }
}

// i64
inline void Write(i64 parameter, NYTree::IYsonConsumer* consumer)
{
    consumer->OnInt64Scalar(parameter);
}

// i32
inline void Write(i32 parameter, NYTree::IYsonConsumer* consumer)
{
    consumer->OnInt64Scalar(parameter);
}

// ui32
inline void Write(ui32 parameter, NYTree::IYsonConsumer* consumer)
{
    consumer->OnInt64Scalar(parameter);
}

// ui16
inline void Write(ui16 parameter, NYTree::IYsonConsumer* consumer)
{
    consumer->OnInt64Scalar(parameter);
}

// double
inline void Write(double parameter, NYTree::IYsonConsumer* consumer)
{
    consumer->OnDoubleScalar(parameter);
}

// Stroka
inline void Write(const Stroka& parameter, NYTree::IYsonConsumer* consumer)
{
    consumer->OnStringScalar(parameter);
}

// bool
inline void Write(bool parameter, NYTree::IYsonConsumer* consumer)
{
    consumer->OnStringScalar(FormatBool(parameter));
}

// TDuration
inline void Write(const TDuration& parameter, NYTree::IYsonConsumer* consumer)
{
    consumer->OnInt64Scalar(parameter.MilliSeconds());
}

// TGuid
inline void Write(const TGuid& parameter, NYTree::IYsonConsumer* consumer)
{
    consumer->OnStringScalar(parameter.ToString());
}

// TEnumBase
template <class T>
inline void Write(
    const T& parameter,
    NYTree::IYsonConsumer* consumer,
    typename NMpl::TEnableIf<NMpl::TIsConvertible< T, TEnumBase<T> >, int>::TType = 0)
{
    consumer->OnStringScalar(parameter.ToString());
}

// INode::TPtr
inline void Write(const NYTree::INode::TPtr& parameter, NYTree::IYsonConsumer* consumer)
{
    if (parameter) {
        NYTree::TTreeVisitor visitor(consumer, false);
        visitor.Visit(~parameter);
    }
}

// yvector
template <class T>
inline void Write(const yvector<T>& parameter, NYTree::IYsonConsumer* consumer)
{
    consumer->OnBeginList();
    FOREACH (const auto& value, parameter) {
        consumer->OnListItem();
        Write(value, consumer);
    }
    consumer->OnEndList();
}

// yhash_set
template <class T>
inline void Write(const yhash_set<T>& parameter, NYTree::IYsonConsumer* consumer)
{
    consumer->OnBeginList();
    auto sortedItems = GetSortedIterators(parameter);
    FOREACH (const auto& value, sortedItems) {
        consumer->OnListItem();
        Write(*value, consumer);
    }
    consumer->OnEndList();
}

// yhash_map
template <class T>
inline void Write(const yhash_map<Stroka, T>& parameter, NYTree::IYsonConsumer* consumer)
{
    consumer->OnBeginMap();
    auto sortedItems = GetSortedIterators(parameter);
    FOREACH (const auto& pair, sortedItems) {
        consumer->OnMapItem(pair->First());
        Write(pair->Second(), consumer);
    }
    consumer->OnEndMap();
}

////////////////////////////////////////////////////////////////////////////////

template<class T>
void TParameter<T>::Save(NYTree::IYsonConsumer *consumer) const
{
    Write(Parameter, consumer);
}

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
    if (parameter->Get()) {
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
            NYTree::CombineYPaths(path, ToString(i)));
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
            &pair.Second(),
            NYTree::CombineYPaths(path, pair.First()));
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TParameter<T>::TParameter(T& parameter)
    : Parameter(parameter)
    , HasDefaultValue(false)
{ }

template <class T>
void TParameter<T>::Load(const NYTree::INode* node, const NYTree::TYPath& path)
{
    if (node) {
        try {
            Read(Parameter, node, path);
        } catch (...) {
            ythrow yexception()
                << Sprintf("Could not read parameter (Path: %s)\n%s",
                    ~path,
                    ~CurrentExceptionMessage());
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
    FOREACH (auto validator, Validators) {
        try {
            validator->Do(Parameter);
        } catch (...) {
            ythrow yexception()
                << Sprintf("Validation failed (Path: %s)\n%s",
                    ~path,
                    ~CurrentExceptionMessage());
        }
    }
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
TParameter<T>& TParameter<T>::CheckThat(TValidator* validator)
{
    Validators.push_back(validator);
    return *this;
}

////////////////////////////////////////////////////////////////////////////////
// Standard validators

#define DEFINE_VALIDATOR(method, condition, ex) \
    template <class T> \
    TParameter<T>& TParameter<T>::method \
    { \
        return CheckThat(~FromFunctor([=] (const T& parameter) \
            { \
                if (!(condition)) { \
                    ythrow (ex); \
                } \
            })); \
    }

DEFINE_VALIDATOR(
    GreaterThan(T value),
    parameter > value,
    yexception()
        << "Validation failure (Expected: >"
        << value << ", Actual: " << parameter << ")")

DEFINE_VALIDATOR(
    GreaterThanOrEqual(T value),
    parameter >= value,
    yexception()
        << "Validation failure (Expected: >="
        << value << ", Actual: " << parameter << ")")

DEFINE_VALIDATOR(
    LessThan(T value),
    parameter < value,
    yexception()
        << "Validation failure (Expected: <"
        << value << ", Actual: " << parameter << ")")

DEFINE_VALIDATOR(
    LessThanOrEqual(T value),
    parameter <= value,
    yexception()
        << "Validation failure (Expected: <="
        << value << ", Actual: " << parameter << ")")

DEFINE_VALIDATOR(
    InRange(T lowerBound, T upperBound),
    lowerBound <= parameter && parameter <= upperBound,
    yexception()
        << "Validation failure (Expected: in range ["
        << lowerBound << ", " << upperBound << "], Actual: " << parameter << ")")

DEFINE_VALIDATOR(
    NonEmpty(),
    parameter.size() > 0,
    yexception()
        << "Validation failure (Expected: non-empty)")

#undef DEFINE_VALIDATOR

////////////////////////////////////////////////////////////////////////////////

} // namespace NConfig

////////////////////////////////////////////////////////////////////////////////

template <class T>
NConfig::TParameter<T>& TConfigurable::Register(const Stroka& parameterName, T& value)
{
    auto parameter = New< TParameter<T> >(value);
    YVERIFY(Parameters.insert(
        TPair<Stroka, IParameter::TPtr>(parameterName, parameter)).Second());
    return *parameter;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
