#pragma once

#include "public.h"

#include <yt/ytlib/node_tracker_client/helpers.h>
#include <yt/ytlib/node_tracker_client/node.pb.h>

#include <yt/core/misc/fixed_point_number.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

// For each memory capacity gives the number of nodes with this much memory.
using TMemoryDistribution = THashMap<i64, int>;

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
    TExtendedJobResources();

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
    TJobResources();
    TJobResources(const NNodeTrackerClient::NProto::TNodeResources& nodeResources);

    NNodeTrackerClient::NProto::TNodeResources ToNodeResources() const;

    void Persist(const TStreamPersistenceContext& context);
};

#define ITERATE_JOB_RESOURCES(XX) \
    XX(user_slots,            UserSlots) \
    XX(cpu,                   Cpu) \
    XX(memory,                Memory) \
    XX(network,               Network)

TString FormatResourceUsage(const TJobResources& usage, const TJobResources& limits);
TString FormatResources(const TJobResources& resources);
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

void SetResource(
    TJobResources& resources,
    NNodeTrackerClient::EResourceType type,
    i64 value);

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
const TJobResources& InfiniteJobResources();

TJobResources  operator +  (const TJobResources& lhs, const TJobResources& rhs);
TJobResources& operator += (TJobResources& lhs, const TJobResources& rhs);

TJobResources  operator -  (const TJobResources& lhs, const TJobResources& rhs);
TJobResources& operator -= (TJobResources& lhs, const TJobResources& rhs);

TJobResources  operator *  (const TJobResources& lhs, i64 rhs);
TJobResources  operator *  (const TJobResources& lhs, double rhs);
TJobResources& operator *= (TJobResources& lhs, i64 rhs);
TJobResources& operator *= (TJobResources& lhs, double rhs);

TJobResources  operator -  (const TJobResources& resources);

bool operator == (const TJobResources& a, const TJobResources& b);
bool operator != (const TJobResources& a, const TJobResources& b);

bool Dominates(const TJobResources& lhs, const TJobResources& rhs);

TJobResources Max(const TJobResources& a, const TJobResources& b);
TJobResources Min(const TJobResources& a, const TJobResources& b);

void Serialize(const TExtendedJobResources& resources, NYson::IYsonConsumer* consumer);
void Serialize(const TJobResources& resources, NYson::IYsonConsumer* consumer);

const TJobResources& MinSpareNodeResources();

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
