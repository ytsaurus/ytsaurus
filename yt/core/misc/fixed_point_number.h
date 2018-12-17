#pragma once

#include <yt/core/ytree/serialize.h>

#include <type_traits>
#include <cmath>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
constexpr T ComputePower(T base, int exponent);

////////////////////////////////////////////////////////////////////////////////

//! Stores fixed point number of form X.YYY with DecimalPrecision decimal digits after the point
//! using one object of Underlying integer type as a storage.
//! Details can be found at https://en.wikipedia.org/wiki/Fixed-point_arithmetic
template <typename Underlying, int DecimalPrecision>
class TFixedPointNumber
{
    static_assert(std::is_integral<Underlying>::value, "Underlying type should be integral");
    static_assert(DecimalPrecision >= 0 && DecimalPrecision <= std::numeric_limits<Underlying>::digits10,
        "Underlying type should be able to represent specified number of decimal places");

public:
    static constexpr Underlying ScalingFactor = ComputePower<Underlying>(10, DecimalPrecision);

    TFixedPointNumber();

    template <typename T>
    TFixedPointNumber(const T& value);

    explicit operator i64 () const;
    explicit operator double () const;

    TFixedPointNumber& operator += (const TFixedPointNumber& rhs);
    TFixedPointNumber& operator -= (const TFixedPointNumber& rhs);

    template <typename T>
    TFixedPointNumber& operator *= (const T& value);

    TFixedPointNumber& operator *= (const double& value);

    template <typename T>
    TFixedPointNumber& operator /= (const T& value);

    friend TFixedPointNumber operator + (TFixedPointNumber lhs, const TFixedPointNumber& rhs)
    {
        lhs += rhs;
        return lhs;
    }

    friend TFixedPointNumber operator - (TFixedPointNumber lhs, const TFixedPointNumber& rhs)
    {
        lhs -= rhs;
        return lhs;
    }

    template <typename T>
    friend TFixedPointNumber operator * (TFixedPointNumber lhs, T value)
    {
        lhs *= value;
        return lhs;
    }

    template <typename T>
    friend TFixedPointNumber operator / (TFixedPointNumber lhs, T value)
    {
        lhs /= value;
        return lhs;
    }

    friend TFixedPointNumber operator - (TFixedPointNumber lhs)
    {
        lhs.Value_ = -lhs.Value_;
        return lhs;
    }

    friend bool operator == (const TFixedPointNumber& lhs, const TFixedPointNumber& rhs)
    {
        return lhs.Value_ == rhs.Value_;
    }

    friend bool operator != (const TFixedPointNumber& lhs, const TFixedPointNumber& rhs)
    {
        return lhs.Value_ != rhs.Value_;
    }

    friend bool operator < (const TFixedPointNumber& lhs, const TFixedPointNumber& rhs)
    {
        return lhs.Value_ < rhs.Value_;
    }

    friend bool operator <= (const TFixedPointNumber& lhs, const TFixedPointNumber& rhs)
    {
        return lhs.Value_ <= rhs.Value_;
    }

    friend bool operator > (const TFixedPointNumber& lhs, const TFixedPointNumber& rhs)
    {
        return lhs.Value_ > rhs.Value_;
    }

    friend bool operator >= (const TFixedPointNumber& lhs, const TFixedPointNumber& rhs)
    {
        return lhs.Value_ >= rhs.Value_;
    }

    void Persist(const NYT::TStreamPersistenceContext& context);

private:
    Underlying Value_;

};

////////////////////////////////////////////////////////////////////////////////

template <typename U, int P>
void Serialize(const TFixedPointNumber<U, P>& number, NYson::IYsonConsumer* consumer);

template <typename U, int P>
void Deserialize(TFixedPointNumber<U, P>& number, NYTree::INodePtr node);

template <typename U, int P>
TString ToString(const TFixedPointNumber<U, P>& number);

template <typename U, int P>
NYT::TFixedPointNumber<U, P> round(const NYT::TFixedPointNumber<U, P>& number);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

namespace std {

template <typename U, int P>
class numeric_limits<NYT::TFixedPointNumber<U, P>>
{
public:
   static NYT::TFixedPointNumber<U, P> max()
   {
       return numeric_limits<U>::max() / NYT::TFixedPointNumber<U, P>::ScalingFactor;
   };
};

////////////////////////////////////////////////////////////////////////////////

} // namespace std

#define FIXED_POINT_NUMBER_INL_H_
#include "fixed_point_number-inl.h"
#undef FIXED_POINT_NUMBER_INL_H_
