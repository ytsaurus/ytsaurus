#pragma once

#include "property.h"

// TODO(ignat): migrate to enum class
#include <library/cpp/ytalloc/core/misc/enum.h>

#include <yt/yt/library/numeric/fixed_point_number.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

// Uses precision of 2 decimal digits.
using TCpuResource = TFixedPointNumber<i64, 2>;

////////////////////////////////////////////////////////////////////////////////

// Implementation detail.
class TEmptyJobResourcesBase
{ };

class TJobResources
    : public TEmptyJobResourcesBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(i64, UserSlots);
    DEFINE_BYVAL_RW_PROPERTY(TCpuResource, Cpu);
    DEFINE_BYVAL_RW_PROPERTY(int, Gpu);
    DEFINE_BYVAL_RW_PROPERTY(i64, Memory);
    DEFINE_BYVAL_RW_PROPERTY(i64, Network);
    
public:
    inline void SetCpu(double cpu)
    {
        Cpu_ = TCpuResource(cpu);
    }
    
    TJobResources() = default;

    static TJobResources Infinite();
};

#define ITERATE_JOB_RESOURCES(XX) \
    XX(user_slots,            UserSlots) \
    XX(cpu,                   Cpu) \
    XX(gpu,                   Gpu) \
    XX(user_memory,           Memory) \
    XX(network,               Network)

// NB(antonkikh): Resource types must be numbered from 0 to N - 1.
DEFINE_ENUM(EJobResourceType,
    (UserSlots)
    (Cpu)
    (Gpu)
    (Memory)
    (Network)
);

EJobResourceType GetDominantResource(
    const TJobResources& demand,
    const TJobResources& limits);

double GetDominantResourceUsage(
    const TJobResources& usage,
    const TJobResources& limits);

double GetResource(
    const TJobResources& resources,
    EJobResourceType type);

void SetResource(
    TJobResources& resources,
    EJobResourceType type,
    double value);

double GetMinResourceRatio(
    const TJobResources& nominator,
    const TJobResources& denominator);

double GetMaxResourceRatio(
    const TJobResources& nominator,
    const TJobResources& denominator);

TJobResources  operator +  (const TJobResources& lhs, const TJobResources& rhs);
TJobResources& operator += (TJobResources& lhs, const TJobResources& rhs);

TJobResources  operator -  (const TJobResources& lhs, const TJobResources& rhs);
TJobResources& operator -= (TJobResources& lhs, const TJobResources& rhs);

TJobResources  operator *  (const TJobResources& lhs, i64 rhs);
TJobResources  operator *  (const TJobResources& lhs, double rhs);
TJobResources& operator *= (TJobResources& lhs, i64 rhs);
TJobResources& operator *= (TJobResources& lhs, double rhs);

TJobResources  operator -  (const TJobResources& resources);

bool operator == (const TJobResources& lhs, const TJobResources& rhs);
bool operator != (const TJobResources& lhs, const TJobResources& rhs);

bool Dominates(const TJobResources& lhs, const TJobResources& rhs);

TJobResources Max(const TJobResources& lhs, const TJobResources& rhs);
TJobResources Min(const TJobResources& lhs, const TJobResources& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
