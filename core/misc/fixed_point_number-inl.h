#pragma once
#ifndef FIXED_POINT_NUMBER_INL_H_
#error "Direct inclusion of this file is not allowed, include fixed_point_number.h"
// For the sake of sane code completion.
#include "fixed_point_number.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
constexpr T ComputePower(T base, int exponent)
{
    return exponent == 0 ? 1 : base * ComputePower(base, exponent - 1);
}

////////////////////////////////////////////////////////////////////////////////

template <typename U, int P>
TFixedPointNumber<U, P>::TFixedPointNumber()
    : Value_()
{ }

template <typename U, int P>
template <typename T>
TFixedPointNumber<U, P>::TFixedPointNumber(const T& value)
    : Value_(std::round(value * ScalingFactor))
{ }

template <typename U, int P>
TFixedPointNumber<U, P>::operator i64 () const
{
    return Value_ / ScalingFactor;
}

template <typename U, int P>
TFixedPointNumber<U, P>::operator double () const
{
    return static_cast<double>(Value_) / ScalingFactor;
}

template <typename U, int P>
TFixedPointNumber<U, P>& TFixedPointNumber<U, P>::operator += (const TFixedPointNumber& rhs)
{
    Value_ += rhs.Value_;
    return *this;
}

template <typename U, int P>
TFixedPointNumber<U, P>& TFixedPointNumber<U, P>::operator -= (const TFixedPointNumber<U, P>& rhs)
{
    Value_ -= rhs.Value_;
    return *this;
}

template <typename U, int P>
template <typename T>
TFixedPointNumber<U, P>& TFixedPointNumber<U, P>::operator *= (const T& value)
{
    Value_ *= value;
    return *this;
}

template <typename U, int P>
template <typename T>
TFixedPointNumber<U, P>& TFixedPointNumber<U, P>::operator /= (const T& value)
{
    Value_ /= value;
    return *this;
}

template <typename U, int P>
void TFixedPointNumber<U, P>::Persist(const NYT::TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Value_);
}

template <typename U, int P>
TFixedPointNumber<U, P>& TFixedPointNumber<U, P>::operator *= (const double& value)
{
    Value_ = std::round(Value_ * value);
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

template <typename U, int P>
void Serialize(const TFixedPointNumber<U, P>& number, NYson::IYsonConsumer* consumer)
{
    NYTree::Serialize(static_cast<double>(number), consumer);
}

template <typename U, int P>
void Deserialize(TFixedPointNumber<U, P>& number, NYTree::INodePtr node)
{
    double doubleValue;
    Deserialize(doubleValue, std::move(node));
    number = doubleValue;
}

template <typename U, int P>
TString ToString(const TFixedPointNumber<U, P>& number)
{
    return ToString(static_cast<double>(number));
}

template <typename U, int P>
NYT::TFixedPointNumber<U, P> round(const NYT::TFixedPointNumber<U, P>& number)
{
    return number;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
