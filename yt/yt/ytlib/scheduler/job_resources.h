#pragma once

// TODO(ignat): migrate to enum class
#include <library/cpp/ytalloc/core/misc/enum.h>

#include <yt/yt/library/numeric/fixed_point_number.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

// Uses precision of 2 decimal digits.
using TCpuResource = TFixedPointNumber<i64, 2>;

void PersistCpuResource(const TStreamPersistenceContext& context, TCpuResource& cpu);

////////////////////////////////////////////////////////////////////////////////

// Implementation detail.
class TEmptyJobResourcesBase
{ };

class TJobResources
    : public TEmptyJobResourcesBase
{
public:
    inline i64 GetUserSlots() const
    {
        return UserSlots_;
    }
    inline void SetUserSlots(i64 slots)
    {
        UserSlots_ = slots;
    }
    
    inline TCpuResource GetCpu() const
    {
        return Cpu_;
    }
    inline void SetCpu(TCpuResource cpu)
    {
        Cpu_ = cpu;
    }
    inline void SetCpu(double cpu)
    {
        Cpu_ = TCpuResource(cpu);
    }
    
    inline int GetGpu() const
    {
        return Gpu_;
    }
    void SetGpu(int gpu)
    {
        Gpu_ = gpu;
    }
    
    inline i64 GetMemory() const
    {
        return Memory_;
    }
    void SetMemory(i64 memory)
    {
        Memory_ = memory;
    }
    
    inline i64 GetNetwork() const
    {
        return Network_;
    }
    void SetNetwork(i64 network)
    {
        Network_ = network;
    }
    
public:
    TJobResources() = default;

    static TJobResources Infinite();

    void Persist(const TStreamPersistenceContext& context);

private:
    i64 UserSlots_{};
    TCpuResource Cpu_{};
    int Gpu_{};
    i64 Memory_{};
    i64 Network_{};
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
