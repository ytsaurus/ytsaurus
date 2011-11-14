#ifndef NEW_CONFIG_INL_H_
#error "Direct inclusion of this file is not allowed, include new_config.h"
#endif
#undef NEW_CONFIG_INL_H_

#include <util/datetime/base.h>

namespace NYT {
namespace NConfig {

////////////////////////////////////////////////////////////////////////////////

inline void Read(i64* parameter, NYTree::INode* node)
{
    *parameter = node->AsInt64()->GetValue();
}

inline void Read(i32* parameter, NYTree::INode* node)
{
    *parameter = static_cast<i32>(node->AsInt64()->GetValue());
}

inline void Read(ui32* parameter, NYTree::INode* node)
{
    *parameter = static_cast<ui32>(node->AsInt64()->GetValue());
}

inline void Read(double* parameter, NYTree::INode* node)
{
    *parameter = node->AsDouble()->GetValue();
}

inline void Read(Stroka* parameter, NYTree::INode* node)
{
    *parameter = node->AsString()->GetValue();
}

inline void Read(bool* parameter, NYTree::INode* node)
{
    Stroka value = node->AsString()->GetValue();
    if (value == "True") {
        *parameter = true;
    } else if (value == "False") {
        *parameter = false;
    } else {
        ythrow yexception()
            << "Could not load bool parameter (Value: "
            << (value.length() <= 10
                ? value
                : value.substr(0, 10) + "...")
            << ")";
    }
}

inline void Read(TDuration* parameter, NYTree::INode* node)
{
    *parameter = TDuration::MilliSeconds(node->AsInt64()->GetValue());
}

template <class T>
inline void Read(yvector<T>* parameter, NYTree::INode* node)
{
    auto listNode = node->AsList();
    auto size = listNode->GetChildCount();
    parameter->resize(size);
    for (int i = 0; i < size; ++i) {
        Read(&(*parameter)[i], ~listNode->GetChild(i));
    }
}

template <class T>
inline void Read(
    T* parameter,
    NYTree::INode* node,
    typename NYT::NDetail::TEnableIfConvertible<T, TEnumBase<T> >::TType = 
        NYT::NDetail::TEmpty())
{
    Stroka value = node->AsString()->GetValue();
    *parameter = T::FromString(value);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TParameter<T, true>::TParameter(T* parameter)
    : Parameter(parameter)
{ }

template <class T>
void TParameter<T, true>::Load(NYTree::INode* node, Stroka path)
{
    Parameter->Load(node, path);
}

template <class T>
void TParameter<T, true>::Validate(Stroka path) const
{
    Parameter->Validate(path);   
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TParameter<T, false>::TParameter(T* parameter)
    : Parameter(parameter)
    , HasDefaultValue(false)
{ }

template <class T>
void TParameter<T, false>::Load(NYTree::INode* node, Stroka path)
{
    if (node != NULL) {
        try {
            Read(Parameter, node);
        } catch (...) {
            ythrow yexception()
                << Sprintf("Could not read parameter (Path: %s, InnerException: %s)",
                    ~path, ~CurrentExceptionMessage());
        }
    } else if (HasDefaultValue) {
        *Parameter = DefaultValue;
    } else {
        ythrow yexception()
            << "Required parameter is missing (Path: " << path << ")";
    }
}

template <class T>
void TParameter<T, false>::Validate(Stroka path) const
{
    FOREACH (auto validator, Validators) {
        try {
            validator->Do(*Parameter);
        } catch (...) {
            ythrow yexception()
                << Sprintf("Config validation failed (Path: %s, InnerException: %s)",
                    ~path, ~CurrentExceptionMessage());
        }
    }
}

template <class T>
TParameter<T, false>& TParameter<T, false>::Default(T defaultValue)
{
    DefaultValue = defaultValue;
    HasDefaultValue = true;
    return *this;
}

template <class T>
TParameter<T, false>& TParameter<T, false>::Check(typename TValidator::TPtr validator)
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
        Check(FromFunctor([=] (T parameter) \
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
        << "Validation failure (Expected: > "
        << value << ", Actual: " << parameter << ")")

DEFINE_VALIDATOR(
    GreaterThanOrEqual(T value),
    parameter >= value,
    yexception()
        << "Validation failure (Expected: >= "
        << value << ", Actual: " << parameter << ")")

DEFINE_VALIDATOR(
    LessThan(T value),
    parameter < value,
    yexception()
        << "Validation failure (Expected: < "
        << value << ", Actual: " << parameter << ")")

DEFINE_VALIDATOR(
    LessThanOrEqual(T value),
    parameter <= value,
    yexception()
        << "Validation failure (Expected: <= "
        << value << ", Actual: " << parameter << ")")

DEFINE_VALIDATOR(
    InRange(T lowerBound, T upperBound),
    lowerBound <= parameter && parameter <= upperBound,
    yexception()
        << "Validation failure, parameter is not in range "
        << "(Range: [" << lowerBound << ", " << upperBound << "], Actual: " << parameter << ")")

DEFINE_VALIDATOR(
    NonEmpty(),
    parameter.size() > 0,
    yexception()
        << "Validation failure, parameter is empty")

#undef DEFINE_VALIDATOR

////////////////////////////////////////////////////////////////////////////////

} // namespace NConfig

////////////////////////////////////////////////////////////////////////////////

template <class T>
NConfig::TParameter<T>& TConfigBase::Register(Stroka parameterName, T& value)
{
    auto parameter = New< NConfig::TParameter<T> >(&value);
    YVERIFY(Parameters.insert(
        TPair<Stroka, NConfig::IParameter::TPtr>(parameterName, parameter)).Second());
    return *parameter;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
