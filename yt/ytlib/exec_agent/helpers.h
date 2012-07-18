#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>

#include <ytlib/scheduler/scheduler_service.pb.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

Stroka FormatResourceUtilization(
    const NScheduler::NProto::TNodeResources& utilization,
    const NScheduler::NProto::TNodeResources& limits);

void IncreaseResourceUtilization(
    NScheduler::NProto::TNodeResources* utilization,
    const NScheduler::NProto::TNodeResources& delta);

bool HasEnoughResources(
    const NScheduler::NProto::TNodeResources& currentUtilization,
    const NScheduler::NProto::TNodeResources& requestedUtilization,
    const NScheduler::NProto::TNodeResources& limits);

bool HasSpareResources(
    const NScheduler::NProto::TNodeResources& utilization,
    const NScheduler::NProto::TNodeResources& limits);

void BuildNodeResourcesYson(
    const NScheduler::NProto::TNodeResources& resources,
    NYTree::IYsonConsumer* consumer);

NScheduler::NProto::TNodeResources ZeroResources();
NScheduler::NProto::TNodeResources InfiniteResources();

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
