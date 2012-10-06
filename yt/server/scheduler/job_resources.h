#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>

#include <ytlib/scheduler/scheduler_service.pb.h>

#include <ytlib/profiling/profiler.h>

#include <server/job_proxy/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EResourceType,
    (Slots)
    (Cpu)
    (Memory)
    (Network)
);

Stroka FormatResourceUtilization(const NProto::TNodeResources& utilization, const NProto::TNodeResources& limits);
Stroka FormatResources(const NProto::TNodeResources& resources);

void ProfileResources(NProfiling::TProfiler& profiler, const NProto::TNodeResources& resources);

NProto::TNodeResources  operator +  (const NProto::TNodeResources& lhs, const NProto::TNodeResources& rhs);
NProto::TNodeResources& operator += (NProto::TNodeResources& lhs, const NProto::TNodeResources& rhs);
NProto::TNodeResources& operator -= (NProto::TNodeResources& lhs, const NProto::TNodeResources& rhs);

NProto::TNodeResources  operator *  (const NProto::TNodeResources& lhs, i64 rhs);
NProto::TNodeResources  operator *  (const NProto::TNodeResources& lhs, double rhs);
NProto::TNodeResources& operator *= (NProto::TNodeResources& lhs, i64 rhs);
NProto::TNodeResources& operator *= (NProto::TNodeResources& lhs, double rhs);

bool Dominates(const NProto::TNodeResources& lhs, const NProto::TNodeResources& rhs);

NProto::TNodeResources Max(const NProto::TNodeResources& a, const NProto::TNodeResources& b);
NProto::TNodeResources Min(const NProto::TNodeResources& a, const NProto::TNodeResources& b);

EResourceType GetDominantResource(
    const NProto::TNodeResources& demand,
    const NProto::TNodeResources& limits);

NProto::TNodeResources ComputeEffectiveLimits(
    const NProto::TNodeResources& limits,
    const NProto::TNodeResources& quantum);

i64 GetResource(
    const NProto::TNodeResources& resources,
    EResourceType type);

bool HasEnoughResources(
    const NProto::TNodeResources& currentUtilization,
    const NProto::TNodeResources& requestedUtilization,
    const NProto::TNodeResources& limits);

bool HasSpareResources(
    const NProto::TNodeResources& utilization,
    const NProto::TNodeResources& limits);

void BuildNodeResourcesYson(
    const NProto::TNodeResources& resources,
    NYTree::IYsonConsumer* consumer);

NProto::TNodeResources ZeroResources();
NProto::TNodeResources InfiniteResources();

i64 GetFootprintMemorySize();

i64 GetIOMemorySize(
    NJobProxy::TJobIOConfigPtr ioConfig,
    int inputStreamCount,
    int outputStreamCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
