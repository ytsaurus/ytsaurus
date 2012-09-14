#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>

#include <ytlib/scheduler/scheduler_service.pb.h>

#include <server/job_proxy/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EResourceType,
    (Slots)
    (Cpu)
    (Memory)
);

Stroka FormatResourceUtilization(
    const NProto::TNodeResources& utilization,
    const NProto::TNodeResources& limits);

Stroka FormatResources(
    const NProto::TNodeResources& resources);

void AddResources(
    NProto::TNodeResources* lhs,
    const NProto::TNodeResources& rhs);

void SubtractResources(
    NProto::TNodeResources* lhs,
    const NProto::TNodeResources& rhs);

void MultiplyResources(
    NProto::TNodeResources* lhs,
    int rhs);

void MultiplyResources(
    NProto::TNodeResources* lhs,
    double rhs);

EResourceType GetDominantResource(
    const NProto::TNodeResources& demand,
    const NProto::TNodeResources& limits);

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
