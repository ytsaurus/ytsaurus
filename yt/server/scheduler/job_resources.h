#pragma once

#include "public.h"

#include <yt/core/misc/phoenix.h>

#include <yt/ytlib/node_tracker_client/helpers.h>
#include <yt/ytlib/node_tracker_client/node.pb.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TJobResources
{
public:
    DEFINE_BYVAL_RW_PROPERTY(int, UserSlots);
    DEFINE_BYVAL_RW_PROPERTY(int, Cpu);
    DEFINE_BYVAL_RW_PROPERTY(i64, Memory);
    DEFINE_BYVAL_RW_PROPERTY(int, Network);

public:
    TJobResources();
    TJobResources(const NNodeTrackerClient::NProto::TNodeResources& resources);

    NNodeTrackerClient::NProto::TNodeResources ToNodeResources() const;

    void Persist(NPhoenix::TPersistenceContext& context);
};

#define ITERATE_JOB_RESOURCES(XX) \
    XX(user_slots,            UserSlots) \
    XX(cpu,                   Cpu) \
    XX(memory,                Memory) \
    XX(network,               Network)

Stroka FormatResourceUsage(const TJobResources& usage, const TJobResources& limits);
Stroka FormatResources(const TJobResources& resources);

void ProfileResources(NProfiling::TProfiler& profiler, const TJobResources& resources);

NNodeTrackerClient::EResourceType GetDominantResource(
    const TJobResources& demand,
    const TJobResources& limits);

i64 GetResource(
    const TJobResources& resources,
    NNodeTrackerClient::EResourceType type);

void SetResource(
    TJobResources& resources,
    NNodeTrackerClient::EResourceType type,
    i64 value);

double GetMinResourceRatio(
    const TJobResources& nominator,
    const TJobResources& denominator);

TJobResources GetAdjustedResourceLimits(
    const TJobResources& demand,
    const TJobResources& limits,
    int nodeCount);

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

void Serialize(const TJobResources& resources, NYson::IYsonConsumer* consumer);

const TJobResources& MinSpareNodeResources();

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
