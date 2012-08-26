#include "stdafx.h"
#include "job_resources.h"
#include "config.h"

#include <ytlib/ytree/fluent.h>

#include <server/job_proxy/config.h>

namespace NYT {
namespace NScheduler {

using namespace NScheduler::NProto;
using namespace NYTree;
using namespace NJobProxy;

////////////////////////////////////////////////////////////////////

//! Additive term for each job memory usage.
//! Accounts for job proxy process and other lightweight stuff.
static const i64 FootprintMemorySize = (i64) 256 * 1024 * 1024;

//! Nodes having less free memory are considered fully occupied.
static const i64 LowWatermarkMemorySize = (i64) 512 * 1024 * 1024;

////////////////////////////////////////////////////////////////////

Stroka FormatResourceUtilization(
    const TNodeResources& utilization,
    const TNodeResources& limits)
{
    return Sprintf("Slots: %d/%d, Cores: %d/%d, Memory: %d/%d, Network: %d/%d",
        // Slots
        utilization.slots(),
        limits.slots(),
        // Cores
        utilization.cores(),
        limits.cores(),
        // Memory (in MB)
        static_cast<int>(utilization.memory() / (1024 * 1024)),
        static_cast<int>(limits.memory() / (1024 * 1024)),
        utilization.network(),
        limits.network());
}

Stroka FormatResources(const TNodeResources& resources)
{
    return Sprintf("Slots: %d, Cores: %d, Memory: %d, Network: %d",
        resources.slots(),
        resources.cores(),
        static_cast<int>(resources.memory() / (1024 * 1024)),
        resources.network());
}

void IncreaseResourceUtilization(
    TNodeResources* utilization,
    const TNodeResources& delta)
{
    utilization->set_slots(utilization->slots() + delta.slots());
    utilization->set_cores(utilization->cores() + delta.cores());
    utilization->set_memory(utilization->memory() + delta.memory());
    utilization->set_network(utilization->network() + delta.network());
}

bool HasEnoughResources(
    const TNodeResources& currentUtilization,
    const TNodeResources& requestedUtilization,
    const TNodeResources& limits)
{
    return
        currentUtilization.slots() + requestedUtilization.slots() <= limits.slots() &&
        currentUtilization.cores() + requestedUtilization.cores() <= limits.cores() &&
        currentUtilization.memory() + requestedUtilization.memory() <= limits.memory() &&
        currentUtilization.network() + requestedUtilization.network() <= limits.network();
}

bool HasSpareResources(
    const TNodeResources& utilization,
    const TNodeResources& limits)
{
    return
        utilization.slots() < limits.slots() &&
        utilization.cores() < limits.cores() &&
        utilization.memory() + LowWatermarkMemorySize < limits.memory();
}

void BuildNodeResourcesYson(
    const TNodeResources& resources,
    IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("slots").Scalar(resources.slots())
        .Item("cores").Scalar(resources.cores())
        .Item("memory").Scalar(resources.memory())
        .Item("network").Scalar(resources.network());
}

TNodeResources ZeroResources()
{
    TNodeResources result;
    result.set_slots(0);
    result.set_cores(0);
    result.set_memory(0);
    result.set_network(0);
    return result;
}

TNodeResources InfiniteResources()
{
    TNodeResources result;
    result.set_slots(1000);
    result.set_cores(1000);
    result.set_memory((i64) 1024 * 1024 * 1024 * 1024);
    result.set_network(1000);
    return result;
}

i64 GetFootprintMemorySize()
{
    return FootprintMemorySize;
}

i64 GetIOMemorySize(
    TJobIOConfigPtr ioConfig,
    int inputStreamCount,
    int outputStreamCount)
{
    return
        ioConfig->TableReader->WindowSize * ioConfig->TableReader->PrefetchWindow * inputStreamCount +
        (ioConfig->TableWriter->WindowSize + 
        ioConfig->TableWriter->EncodeWindowSize) * outputStreamCount * 2;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

