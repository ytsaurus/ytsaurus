#pragma once

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/library/numeric/binary_search.h>
#include <yt/library/numeric/double_array.h>
#include <yt/library/numeric/piecewise_linear_function.h>

#include <cmath>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

static constexpr double RatioComputationPrecision = 1e-9;  // std::numeric_limits<double>::epsilon();
static constexpr double RatioComparisonPrecision = 1e-4;

////////////////////////////////////////////////////////////////////////////////

inline constexpr int GetResourceCount() noexcept
{
    int res = 0;
    #define XX(name, Name) do { res += 1; } while(false);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return res;
}

static constexpr int ResourceCount = GetResourceCount();
static_assert(TEnumTraits<EJobResourceType>::DomainSize == ResourceCount);

class TResourceVector
    : public TDoubleArrayBase<ResourceCount, TResourceVector>
{
private:
    using TBase = TDoubleArrayBase<ResourceCount, TResourceVector>;

public:
    using TBase::TDoubleArrayBase;
    using TBase::operator[];

    Y_FORCE_INLINE double& operator[](EJobResourceType resourceType)
    {
        static_assert(TEnumTraits<EJobResourceType>::DomainSize == ResourceCount);
        return (*this)[GetIdByResourceType(resourceType)];
    }

    Y_FORCE_INLINE const double& operator[](EJobResourceType resourceType) const
    {
        static_assert(TEnumTraits<EJobResourceType>::DomainSize == ResourceCount);
        return (*this)[GetIdByResourceType(resourceType)];
    }

    static TResourceVector FromJobResources(
        const TJobResources& resources,
        const TJobResources& totalLimits,
        double zeroDivByZero,
        double oneDivByZero)
    {
        TResourceVector result = {};
        int resourceId = 0;
        auto update = [&](auto resourceValue, auto resourceLimit) {
            if (resourceLimit == 0) {
                if (resourceValue == 0) {
                    result[resourceId] = zeroDivByZero;
                } else {
                    result[resourceId] = oneDivByZero;
                }
            } else {
                result[resourceId] = static_cast<double>(resourceValue) / static_cast<double>(resourceLimit);
            }
            ++resourceId;
        };
        #define XX(name, Name) update(resources.Get##Name(), totalLimits.Get##Name());
        ITERATE_JOB_RESOURCES(XX)
        #undef XX
        return result;
    }

    static constexpr TResourceVector SmallEpsilon()
    {
        return FromDouble(RatioComputationPrecision);
    }

    static constexpr TResourceVector Epsilon()
    {
        return FromDouble(RatioComparisonPrecision);
    }

    static constexpr TResourceVector Infinity()
    {
        return FromDouble(1e10);
    }

    Y_FORCE_INLINE static constexpr int GetIdByResourceType(EJobResourceType resourceType)
    {
        return static_cast<int>(resourceType);
    }

    Y_FORCE_INLINE static constexpr EJobResourceType GetResourceTypeById(int resourceId)
    {
        return static_cast<EJobResourceType>(resourceId);
    }
};

inline TJobResources operator*(const TJobResources& lhs, const TResourceVector& rhs)
{
    TJobResources result;
    int resourceId = 0;
    #define XX(name, Name) do { \
        double newValue = std::round(static_cast<double>(lhs.Get##Name() * rhs[resourceId])); \
        result.Set##Name(static_cast<decltype(lhs.Get##Name())>(newValue)); \
        ++resourceId; \
    } while (false);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

////////////////////////////////////////////////////////////////////////////////

using TVectorPiecewiseSegment = TPiecewiseSegment<TResourceVector>;
using TScalarPiecewiseSegment = TPiecewiseSegment<double>;
using TVectorPiecewiseLinearFunction = TPiecewiseLinearFunction<TResourceVector>;
using TScalarPiecewiseLinearFunction = TPiecewiseLinearFunction<double>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

