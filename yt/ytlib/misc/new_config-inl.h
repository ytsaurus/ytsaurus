#ifndef NEW_CONFIG_INL_H_
#error "Direct inclusion of this file is not allowed, include new_config.h"
#endif
#undef NEW_CONFIG_INL_H_

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

// TConfigBase
template <class T>
inline void Read(
    T* parameter,
    NYTree::INode* node,
    const NYTree::TYPath& path,
    typename NYT::NDetail::TEnableIfConvertible<T, TConfigBase>::TType =
        NYT::NDetail::TEmpty())
{
    parameter->Load(node, path);
}

// i64
inline void Read(i64* parameter, NYTree::INode* node, const NYTree::TYPath& /* path */)
{
    *parameter = node->AsInt64()->GetValue();
}

// i32
inline void Read(i32* parameter, NYTree::INode* node, const NYTree::TYPath& /* path */)
{
    *parameter = CheckedStaticCast<i32>(node->AsInt64()->GetValue());
}

// ui32
inline void Read(ui32* parameter, NYTree::INode* node, const NYTree::TYPath& /* path */)
{
    *parameter = CheckedStaticCast<ui32>(node->AsInt64()->GetValue());
}

// double
inline void Read(double* parameter, NYTree::INode* node, const NYTree::TYPath& /* path */)
{
    *parameter = node->AsDouble()->GetValue();
}

// Stroka
inline void Read(Stroka* parameter, NYTree::INode* node, const NYTree::TYPath& /* path */)
{
    *parameter = node->AsString()->GetValue();
}

// bool
inline void Read(bool* parameter, NYTree::INode* node, const NYTree::TYPath& /* path */)
{
    Stroka value = node->AsString()->GetValue();
    if (value == "True") {
        *parameter = true;
    } else if (value == "False") {
        *parameter = false;
    } else {
        ythrow yexception()
            << Sprintf("Could not load boolean parameter (Value: %s)",
                value.length() <= 10
                    ? ~value
                    : ~(value.substr(0, 10) + "..."));
    }
}

// TDuration
inline void Read(TDuration* parameter, NYTree::INode* node, const NYTree::TYPath& /* path */)
{
    *parameter = TDuration::MilliSeconds(node->AsInt64()->GetValue());
}

// TEnumBase
template <class T>
inline void Read(
    T* parameter,
    NYTree::INode* node, 
    const NYTree::TYPath& /* path */,
    typename NYT::NDetail::TEnableIfConvertible<T, TEnumBase<T> >::TType = 
        NYT::NDetail::TEmpty())
{
    Stroka value = node->AsString()->GetValue();
    *parameter = T::FromString(value);
}

// yvector
template <class T>
inline void Read(yvector<T>* parameter, NYTree::INode* node, const NYTree::TYPath& path)
{
    auto listNode = node->AsList();
    auto size = listNode->GetChildCount();
    parameter->resize(size);
    for (int i = 0; i < size; ++i) {
        Read(&(*parameter)[i], ~listNode->GetChild(i), path + "/" + ToString(i));
    }
}

// yhash_set
template <class T>
inline void Read(yhash_set<T>* parameter, NYTree::INode* node, const NYTree::TYPath& path)
{
    auto listNode = node->AsList();
    auto size = listNode->GetChildCount();
    for (int i = 0; i < size; ++i) {
        T value;
        Read(&value, ~listNode->GetChild(i), path + "/" + ToString(i));
        parameter->insert(MoveRV(value));
    }
}

// yhash_map
template <class T>
inline void Read(yhash_map<Stroka, T>* parameter, NYTree::INode* node, const NYTree::TYPath& path)
{
    auto mapNode = node->AsMap();
    FOREACH (const auto& pair, mapNode->GetChildren()) {
        auto& key = pair.First();
        T value;
        Read(&value, ~pair.Second(), path + "/" + key);
        parameter->insert(MakePair(key, MoveRV(value)));
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TParameter<T, true>::TParameter(T* parameter)
    : Parameter(parameter)
{ }

template <class T>
void TParameter<T, true>::Load(NYTree::INode* node, const NYTree::TYPath& path)
{
    if (node != NULL) {
        Parameter->Load(node, path);
    } else {
        Parameter->SetDefaults(path);
    }
}

template <class T>
void TParameter<T, true>::Validate(const NYTree::TYPath& path) const
{
    Parameter->Validate(path);   
}

template <class T>
void TParameter<T, true>::SetDefaults(const NYTree::TYPath& path)
{
    Parameter->SetDefaults(path);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TParameter<T, false>::TParameter(T* parameter)
    : Parameter(parameter)
    , HasDefaultValue(false)
{ }

template <class T>
void TParameter<T, false>::Load(NYTree::INode* node, const NYTree::TYPath& path)
{
    if (node != NULL) {
        try {
            Read(Parameter, node, path);
        } catch (...) {
            ythrow yexception()
                << Sprintf("Could not read parameter (Path: %s)\n%s",
                    ~path,
                    ~CurrentExceptionMessage());
        }
    } else if (HasDefaultValue) { // same as SetDefaults(false, path), but another error message
        *Parameter = DefaultValue;
    } else {
        ythrow yexception()
            << Sprintf("Required parameter is missing (Path: %s)", ~path);
    }
}

template <class T>
void TParameter<T, false>::Validate(const NYTree::TYPath& path) const
{
    FOREACH (auto validator, Validators) {
        try {
            validator->Do(*Parameter);
        } catch (...) {
            ythrow yexception()
                << Sprintf("Config validation failed (Path: %s)\n%s",
                    ~path,
                    ~CurrentExceptionMessage());
        }
    }
}

template <class T>
void TParameter<T, false>::SetDefaults(const NYTree::TYPath& path)
{
    if (HasDefaultValue) {
        *Parameter = DefaultValue;
    } else {
        ythrow yexception()
            << Sprintf("Parameter does not have default value (Path: %s)",
                ~path);
    }
}

template <class T>
TParameter<T, false>& TParameter<T, false>::Default(const T& defaultValue)
{
    DefaultValue = defaultValue;
    HasDefaultValue = true;
    *Parameter = DefaultValue;
    return *this;
}

template <class T>
TParameter<T, false>& TParameter<T, false>::Default(T&& defaultValue)
{
    DefaultValue = MoveRV(defaultValue);
    HasDefaultValue = true;
    *Parameter = DefaultValue;
    return *this;
}

template <class T>
TParameter<T, false>& TParameter<T, false>::CheckThat(TValidator* validator)
{
    Validators.push_back(validator);
    return *this;
}

////////////////////////////////////////////////////////////////////////////////
// Standard validators

#define DEFINE_VALIDATOR(method, condition, ex) \
    template <class T> \
    TParameter<T, false>& TParameter<T, false>::method \
    { \
        CheckThat(~FromFunctor([=] (const T& parameter) \
            { \
                if (!(condition)) { \
                    ythrow (ex); \
                } \
            })); \
        return *this; \
    }

DEFINE_VALIDATOR(
    GreaterThan(T value),
    parameter > value,
    yexception()
        << "Validation failure (Expected: to be greater than "
        << value << ", Actual: " << parameter << ")")

DEFINE_VALIDATOR(
    GreaterThanOrEqual(T value),
    parameter >= value,
    yexception()
        << "Validation failure (Expected: to be greater than or equal to "
        << value << ", Actual: " << parameter << ")")

DEFINE_VALIDATOR(
    LessThan(T value),
    parameter < value,
    yexception()
        << "Validation failure (Expected: to be less than "
        << value << ", Actual: " << parameter << ")")

DEFINE_VALIDATOR(
    LessThanOrEqual(T value),
    parameter <= value,
    yexception()
        << "Validation failure (Expected: to be less than or equal to "
        << value << ", Actual: " << parameter << ")")

DEFINE_VALIDATOR(
    InRange(T lowerBound, T upperBound),
    lowerBound <= parameter && parameter <= upperBound,
    yexception()
        << "Validation failure (Expected: to be in range ["
        << lowerBound << ", " << upperBound << "], Actual: " << parameter << ")")

DEFINE_VALIDATOR(
    NonEmpty(),
    parameter.size() > 0,
    yexception()
        << "Validation failure (Expected: to be non-empty)")

#undef DEFINE_VALIDATOR

////////////////////////////////////////////////////////////////////////////////

} // namespace NConfig

////////////////////////////////////////////////////////////////////////////////

template <class T>
NConfig::TParameter<T>& TConfigBase::Register(const Stroka& parameterName, T& value)
{
    auto parameter = New< NConfig::TParameter<T> >(&value);
    YVERIFY(Parameters.insert(
        TPair<Stroka, NConfig::IParameter::TPtr>(parameterName, parameter)).Second());
    return *parameter;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
