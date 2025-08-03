#pragma once

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/library/numeric/fixed_point_number.h>

// TODO(ignat): migrate to enum class
#include <library/cpp/yt/misc/enum.h>
#include <library/cpp/yt/misc/property.h>

#include <optional>

namespace NYT::NVectorHdrf {

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
    TJobResources(const TJobResources&) = default;
    TJobResources& operator=(const TJobResources& other) = default;

    static TJobResources Infinite();
};

#define ITERATE_JOB_RESOURCES(XX) \
    XX(user_slots,            UserSlots) \
    XX(cpu,                   Cpu) \
    XX(gpu,                   Gpu) \
    XX(user_memory,           Memory) \
    XX(network,               Network)

DEFINE_ENUM(EJobResourceType,
    ((UserSlots)    (0))
    ((Cpu)          (1))
    ((Gpu)          (2))
    ((Memory)       (3))
    ((Network)      (4))
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

std::ostream& operator << (std::ostream& os, const TJobResources& jobResources);

bool Dominates(const TJobResources& lhs, const TJobResources& rhs);
bool StrictlyDominates(const TJobResources& lhs, const TJobResources& rhs);

TJobResources Max(const TJobResources& lhs, const TJobResources& rhs);
TJobResources Min(const TJobResources& lhs, const TJobResources& rhs);

void Serialize(const TJobResources& resources, NYson::IYsonConsumer* consumer);
void Deserialize(TJobResources& resources, NYTree::INodePtr node);

void FormatValue(TStringBuilderBase* builder, const TJobResources& resources, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

struct TJobResourcesConfig
{
    std::optional<int> UserSlots;
    std::optional<double> Cpu;
    std::optional<int> Network;
    std::optional<i64> Memory;
    std::optional<int> Gpu;

    template <class T>
    static void ForEachResource(T processResource)
    {
        processResource(&TJobResourcesConfig::UserSlots, EJobResourceType::UserSlots);
        processResource(&TJobResourcesConfig::Cpu, EJobResourceType::Cpu);
        processResource(&TJobResourcesConfig::Network, EJobResourceType::Network);
        processResource(&TJobResourcesConfig::Memory, EJobResourceType::Memory);
        processResource(&TJobResourcesConfig::Gpu, EJobResourceType::Gpu);
    }

    bool IsNonTrivial();
    bool IsEqualTo(const TJobResourcesConfig& other);

    TJobResourcesConfig& operator+=(const TJobResourcesConfig& addend);
};

////////////////////////////////////////////////////////////////////////////////

TJobResources ToJobResources(const TJobResourcesConfig& config, TJobResources defaultValue);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf
