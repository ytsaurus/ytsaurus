#pragma once

#include "public.h"

#include <yt/ytlib/node_tracker_client/helpers.h>
#include <yt/ytlib/node_tracker_client/node.pb.h>

#include <yt/core/misc/fixed_point_number.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

// For each memory capacity gives the number of nodes with this much memory.
using TMemoryDistribution = yhash<i64, int>;

// Uses precision of 2 decimal digits.
using TCpuResource = TFixedPointNumber<int, 2>;

// Implementation detail.
class TEmptyJobResourcesBase
{ };

class TExtendedJobResources
{
public:
    DEFINE_BYVAL_RW_PROPERTY(int, UserSlots);
    DEFINE_BYVAL_RW_PROPERTY(TCpuResource, Cpu);
    DEFINE_BYVAL_RW_PROPERTY(i64, JobProxyMemory);
    DEFINE_BYVAL_RW_PROPERTY(i64, UserJobMemory);
    DEFINE_BYVAL_RW_PROPERTY(i64, FootprintMemory);
    DEFINE_BYVAL_RW_PROPERTY(int, Network);

public:
    i64 GetMemory() const;

    void Persist(const TStreamPersistenceContext& context);
};

class TJobResources
    : public TEmptyJobResourcesBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(int, UserSlots);
    DEFINE_BYVAL_RW_PROPERTY(TCpuResource, Cpu);
    DEFINE_BYVAL_RW_PROPERTY(i64, Memory);
    DEFINE_BYVAL_RW_PROPERTY(int, Network);

public:
    TJobResources() = default;
    TJobResources(const NNodeTrackerClient::NProto::TNodeResources& nodeResources);

    NNodeTrackerClient::NProto::TNodeResources ToNodeResources() const;

    void Persist(const TStreamPersistenceContext& context);
};

#define ITERATE_JOB_RESOURCES(XX) \
    XX(user_slots,            UserSlots) \
    XX(cpu,                   Cpu) \
    XX(user_memory,           Memory) \
    XX(network,               Network)

#define MAKE_JOB_METHODS(Base, Field) \
    using Base::Get ## Field; \
    using Base::Set ## Field;

template <class TResourceType>
class TResourcesWithQuota
{
public:
    i64 GetDiskQuota() const
    {
        return DiskQuota_;
    }

    void SetDiskQuota(i64 DiskQuota)
    {
        DiskQuota_ = DiskQuota;
    }

protected:
    i64 DiskQuota_ = 0;
};

template <>
struct TResourcesWithQuota<TJobResources>
    : private TJobResources
    , private TResourcesWithQuota<void>
{
public:
    MAKE_JOB_METHODS(TJobResources, UserSlots)
    MAKE_JOB_METHODS(TJobResources, Cpu)
    MAKE_JOB_METHODS(TJobResources, Memory)
    MAKE_JOB_METHODS(TJobResources, Network)
    MAKE_JOB_METHODS(TResourcesWithQuota<void>, DiskQuota);

    TJobResources ToJobResources() const
    {
        return *this;
    }

    void Persist(const TStreamPersistenceContext& context)
    {
        using NYT::Persist;
        TJobResources::Persist(context);
        Persist(context, DiskQuota_);
    }

    TResourcesWithQuota() = default;

    TResourcesWithQuota(const TJobResources& jobResources)
        : TJobResources(jobResources)
    {
        SetDiskQuota(0);
    }
};

using TJobResourcesWithQuota = TResourcesWithQuota<TJobResources>;

TString FormatResourceUsage(const TJobResources& usage, const TJobResources& limits);
TString FormatResourceUsage(
    const TJobResources& usage,
    const TJobResources& limits,
    const NNodeTrackerClient::NProto::TDiskResources& diskInfo);
TString FormatResources(const TJobResources& resources);
TString FormatResources(const TJobResourcesWithQuota& resources);
TString FormatResources(const TExtendedJobResources& resources);

void ProfileResources(
    const NProfiling::TProfiler& profiler,
    const TJobResources& resources,
    const TString& prefix = TString(),
    const NProfiling::TTagIdList& tagIds = NProfiling::EmptyTagIds);

NNodeTrackerClient::EResourceType GetDominantResource(
    const TJobResources& demand,
    const TJobResources& limits);

double GetDominantResourceUsage(
    const TJobResources& usage,
    const TJobResources& limits);

double GetResource(
    const TJobResources& resources,
    NNodeTrackerClient::EResourceType type);

double GetMinResourceRatio(
    const TJobResources& nominator,
    const TJobResources& denominator);

double GetMaxResourceRatio(
    const TJobResources& nominator,
    const TJobResources& denominator);

TJobResources GetAdjustedResourceLimits(
    const TJobResources& demand,
    const TJobResources& limits,
    const TMemoryDistribution& execNodeMemoryDistribution);

const TJobResources& ZeroJobResources();
const TJobResourcesWithQuota& ZeroJobResourcesWithQuota();
const TJobResources& InfiniteJobResources();
const TJobResourcesWithQuota& InfiniteJobResourcesWithQuota();

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
TJobResourcesWithQuota Min(const TJobResourcesWithQuota& lhs, const TJobResourcesWithQuota& rhs);

void Serialize(const TExtendedJobResources& resources, NYson::IYsonConsumer* consumer);
void Serialize(const TJobResources& resources, NYson::IYsonConsumer* consumer);

const TJobResources& MinSpareNodeResources();

bool CanSatisfyDiskRequest(
    const NNodeTrackerClient::NProto::TDiskResources& diskInfo,
    i64 diskRequest);

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(NScheduler::NProto::TJobResources* protoResources, const NScheduler::TJobResources& resources);
void FromProto(NScheduler::TJobResources* resources, const NScheduler::NProto::TJobResources& protoResources);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
