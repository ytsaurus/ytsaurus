#ifndef METRIC_INL_H_
#error "Direct inclusion of this file is not allowed, include metric.h"
// For the sake of sane code completion.
#include "metric.h"
#endif

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

template <int Size>
Y_FORCE_INLINE TGenericMetric<Size>::TGenericMetric(const std::array<double, Size>& values)
    : Values_(values)
{ }

template <int Size>
Y_FORCE_INLINE TGenericMetric<Size> TGenericMetric<Size>::operator+(TGenericMetric other) const
{
    TGenericMetric result;
    for (int index = 0; index < Size; ++index) {
        result.Values_[index] = Values_[index] + other.Values_[index];
    }
    return result;
}

template <int Size>
Y_FORCE_INLINE TGenericMetric<Size> TGenericMetric<Size>::operator-(TGenericMetric other) const
{
    TGenericMetric result;
    for (int index = 0; index < Size; ++index) {
        result.Values_[index] = Values_[index] - other.Values_[index];
    }
    return result;
}

template <int Size>
Y_FORCE_INLINE TGenericMetric<Size> TGenericMetric<Size>::operator*(TGenericMetric other) const
{
    TGenericMetric result;
    for (int index = 0; index < Size; ++index) {
        result.Values_[index] = Values_[index] * other.Values_[index];
    }
    return result;
}

template <int Size>
Y_FORCE_INLINE TGenericMetric<Size> TGenericMetric<Size>::operator/(TGenericMetric other) const
{
    TGenericMetric result;
    for (int index = 0; index < Size; ++index) {
        result.Values_[index] = Values_[index] / other.Values_[index];
    }
    return result;
}

template <int Size>
Y_FORCE_INLINE TGenericMetric<Size> TGenericMetric<Size>::operator+(double scalar) const
{
    TGenericMetric result;
    for (int index = 0; index < Size; ++index) {
        result.Values_[index] = Values_[index] + scalar;
    }
    return result;
}

template <int Size>
Y_FORCE_INLINE TGenericMetric<Size> TGenericMetric<Size>::operator-(double scalar) const
{
    TGenericMetric result;
    for (int index = 0; index < Size; ++index) {
        result.Values_[index] = Values_[index] - scalar;
    }
    return result;
}

template <int Size>
Y_FORCE_INLINE TGenericMetric<Size> TGenericMetric<Size>::operator*(double scalar) const
{
    TGenericMetric result;
    for (int index = 0; index < Size; ++index) {
        result.Values_[index] = Values_[index] * scalar;
    }
    return result;
}

template <int Size>
Y_FORCE_INLINE TGenericMetric<Size> TGenericMetric<Size>::operator/(double scalar) const
{
    TGenericMetric result;
    for (int index = 0; index < Size; ++index) {
        result.Values_[index] = Values_[index] / scalar;
    }
    return result;
}

template <int Size>
Y_FORCE_INLINE TGenericMetric<Size>& TGenericMetric<Size>::operator+=(TGenericMetric other)
{
    for (int index = 0; index < Size; ++index) {
        Values_[index] += other.Values_[index];
    }
    return *this;
}

template <int Size>
Y_FORCE_INLINE TGenericMetric<Size>& TGenericMetric<Size>::operator-=(TGenericMetric other)
{
    for (int index = 0; index < Size; ++index) {
        Values_[index] -= other.Values_[index];
    }
    return *this;
}

template <int Size>
Y_FORCE_INLINE TGenericMetric<Size>& TGenericMetric<Size>::operator*=(TGenericMetric other)
{
    for (int index = 0; index < Size; ++index) {
        Values_[index] *= other.Values_[index];
    }
    return *this;
}

template <int Size>
Y_FORCE_INLINE TGenericMetric<Size>& TGenericMetric<Size>::operator/=(TGenericMetric other)
{
    for (int index = 0; index < Size; ++index) {
        Values_[index] /= other.Values_[index];
    }
    return *this;
}

template <int Size>
Y_FORCE_INLINE TGenericMetric<Size>& TGenericMetric<Size>::operator+=(double scalar)
{
    for (int index = 0; index < Size; ++index) {
        Values_[index] += scalar;
    }
    return *this;
}

template <int Size>
Y_FORCE_INLINE TGenericMetric<Size>& TGenericMetric<Size>::operator-=(double scalar)
{
    for (int index = 0; index < Size; ++index) {
        Values_[index] -= scalar;
    }
    return *this;
}

template <int Size>
Y_FORCE_INLINE TGenericMetric<Size>& TGenericMetric<Size>::operator*=(double scalar)
{
    for (int index = 0; index < Size; ++index) {
        Values_[index] *= scalar;
    }
    return *this;
}

template <int Size>
Y_FORCE_INLINE TGenericMetric<Size>& TGenericMetric<Size>::operator/=(double scalar)
{
    for (int index = 0; index < Size; ++index) {
        Values_[index] /= scalar;
    }
    return *this;
}

template <int Size>
Y_FORCE_INLINE double TGenericMetric<Size>::GetTotalValue() const
{
    double result = 0.0;
    for (int index = 0; index < Size; ++index) {
        result += Values_[index];
    }
    return result;
}

template <int Size>
Y_FORCE_INLINE TGenericMetric<Size> TGenericMetric<Size>::GetNormalizedMetric(double scalar) const
{
    TGenericMetric result;
    for (int index = 0; index < Size; ++index) {
        if constexpr (Size <= 2) {
            result.Values_[index] = Values_[index] < MinimumAcceptableMetricValue
                ? 1.0
                : scalar / Values_[index];
        } else {
            // Select safe operands so vectorization does not need predicated division.
            bool belowThreshold = Values_[index] < MinimumAcceptableMetricValue;
            double numerator = belowThreshold ? 1.0 : scalar;
            double denominator = belowThreshold ? 1.0 : Values_[index];
            result.Values_[index] = numerator / denominator;
        }
    }
    return result;
}

template <int Size>
const std::array<double, Size>& TGenericMetric<Size>::ToArray() const
{
    return Values_;
}

////////////////////////////////////////////////////////////////////////////////

template <int Size>
void FormatValue(TStringBuilderBase* builder, TGenericMetric<Size> metric, TStringBuf /*spec*/)
{
    if constexpr (Size == 1) {
        builder->AppendString(ToString(metric.GetTotalValue()));
    } else {
        builder->AppendFormat("{%v, TotalValue: %v}",
            metric.ToArray(),
            metric.GetTotalValue());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
