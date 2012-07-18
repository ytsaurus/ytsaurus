#include "stdafx.h"
#include "helpers.h"

#include <ytlib/ytree/fluent.h>

namespace NYT {
namespace NExecAgent {

using namespace NYTree;
using namespace NScheduler::NProto;

////////////////////////////////////////////////////////////////////

Stroka FormatResourceUtilization(
    const TNodeResources& utilization,
    const TNodeResources& limits)
{
    return Sprintf("Slots: %d/%d, Cores: %d/%d, Memory: %d/%d",
        // Slots
        utilization.slots(),
        limits.slots(),
        // Cores
        utilization.cores(),
        limits.cores(),
        // Memory (in MB)
        utilization.memory() / (1024 * 1024),
        limits.memory() / (1024 * 1024));
}

void IncreaseResourceUtilization(
    TNodeResources* utilization,
    const TNodeResources& delta)
{
    utilization->set_slots(utilization->slots() + delta.slots());
    utilization->set_cores(utilization->cores() + delta.cores());
    utilization->set_memory(utilization->memory() + delta.memory());
}

bool HasEnoughResources(
    const TNodeResources& currentUtilization,
    const TNodeResources& requestedUtilization,
    const TNodeResources& limits)
{
    return
        currentUtilization.slots() + requestedUtilization.slots() <= limits.slots() &&
        currentUtilization.cores() + requestedUtilization.cores() <= limits.cores() &&
        currentUtilization.memory() + requestedUtilization.memory() <= limits.memory();
}

bool HasSpareResources(
    const TNodeResources& utilization,
    const TNodeResources& limits)
{
    return
        utilization.slots() < limits.slots() &&
        utilization.cores() < limits.cores() &&
        // TODO(babenko): remove magic number
        utilization.memory() + (i64) 1024 * 1024 * 1024 < limits.memory();
}

void BuildNodeResourcesYson(
    const TNodeResources& resources,
    IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("slots").Scalar(resources.slots())
        .Item("cores").Scalar(resources.cores())
        .Item("memory").Scalar(resources.memory());
}

////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

