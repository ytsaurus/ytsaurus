#include "stdafx.h"
#include "job_resources.h"
#include "config.h"

#include <ytlib/ytree/fluent.h>

#include <ytlib/job_proxy/config.h>

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

i64 GetIOMemorySize(
    TJobIOConfigPtr ioConfig,
    int inputStreamCount,
    int outputStreamCount)
{
    return
        ioConfig->TableReader->WindowSize * ioConfig->TableReader->PrefetchWindow * inputStreamCount +
        ioConfig->TableWriter->WindowSize * outputStreamCount +
        ioConfig->TableWriter->WindowSize * outputStreamCount;
}

TNodeResources GetMapResources(
    TJobIOConfigPtr ioConfig,
    TMapOperationSpecPtr spec)
{
    TNodeResources result;
    result.set_slots(1);
    result.set_cores(spec->Mapper->CoresLimit);
    result.set_memory(
        GetIOMemorySize(
            ioConfig,
            spec->InputTablePaths.size(),
            spec->OutputTablePaths.size()) +
        FootprintMemorySize);
    return result;
}

TNodeResources GetMapDuringMapReduceResources(
    TJobIOConfigPtr ioConfig,
    TMapReduceOperationSpecPtr spec,
    int partitionCount)
{
    TNodeResources result;
    result.set_slots(1);
    result.set_cores(spec->Mapper->CoresLimit);
    result.set_memory(
        GetIOMemorySize(ioConfig, 1, 1) +
        ioConfig->TableWriter->BlockSize * partitionCount +
        spec->Mapper->MemoryLimit +
        FootprintMemorySize);
    return result;
}

TNodeResources GetMergeJobResources(
    TJobIOConfigPtr ioConfig,
    TMergeOperationSpecPtr spec)
{
    TNodeResources result;
    result.set_slots(1);
    result.set_cores(1);
    result.set_memory(
        GetIOMemorySize(ioConfig, spec->InputTablePaths.size(), 1) +
        FootprintMemorySize);
    return result;
}

TNodeResources GetPartitionReduceDuringMapReduceResources(
    TJobIOConfigPtr ioConfig,
    TMapReduceOperationSpecPtr spec,
    i64 dataSize,
    i64 rowCount)
{
    TNodeResources result;
    result.set_slots(1);
    result.set_cores(spec->Reducer->CoresLimit);
    result.set_memory(
        GetIOMemorySize(ioConfig, 0, spec->OutputTablePaths.size()) +
        dataSize +
        (i64) 16 * spec->SortBy.size() * rowCount +
        (i64) 16 * rowCount +
        spec->Reducer->MemoryLimit +
        FootprintMemorySize);
    result.set_network(spec->ShuffleNetworkLimit);
    return result;
}

TNodeResources GetSortedReduceDuringMapReduceResources(
    TJobIOConfigPtr ioConfig,
    TMapReduceOperationSpecPtr spec,
    int stripeCount)
{
    TNodeResources result;
    result.set_slots(1);
    result.set_cores(spec->Reducer->CoresLimit);
    result.set_memory(
        GetIOMemorySize(ioConfig, stripeCount, spec->OutputTablePaths.size()) +
        spec->Reducer->MemoryLimit +
        FootprintMemorySize);
    return result;
}

TNodeResources GetEraseResources(
    TJobIOConfigPtr ioConfig,
    TEraseOperationSpecPtr spec)
{
    TNodeResources result;
    result.set_slots(1);
    result.set_cores(1);
    result.set_memory(
        GetIOMemorySize(ioConfig, 1, 1) +
        FootprintMemorySize);
    return result;
}

NProto::TNodeResources GetSortedReduceResources(
    NJobProxy::TJobIOConfigPtr ioConfig,
    TReduceOperationSpecPtr spec)
{
    TNodeResources result;
    result.set_slots(1);
    result.set_cores(spec->Reducer->CoresLimit);
    result.set_memory(
        GetIOMemorySize(ioConfig, spec->InputTablePaths.size(), spec->OutputTablePaths.size()) +
        spec->Reducer->MemoryLimit +
        FootprintMemorySize);
    return result;
}

TNodeResources GetPartitionResources(
    TJobIOConfigPtr ioConfig,
    i64 dataSize,
    int partitionCount)
{
    TNodeResources result;
    result.set_slots(1);
    result.set_cores(1);
    result.set_memory(
        GetIOMemorySize(ioConfig, 1, 1) +
        std::min(ioConfig->TableWriter->BlockSize * partitionCount, dataSize) +
        FootprintMemorySize);
    return result;
}

TNodeResources GetSimpleSortResources(
    TJobIOConfigPtr ioConfig,
    TSortOperationSpecPtr spec,
    i64 dataSize,
    i64 rowCount,
    i64 valueCount)
{
    TNodeResources result;
    result.set_slots(1);
    result.set_cores(1);
    result.set_memory(
        // NB: Sort jobs typically have large prefetch window, which would
        // drastically increase the estimated consumption returned by GetIOMemorySize.
        // Setting input count to zero to eliminates this term.
        GetIOMemorySize(ioConfig, 0, 1) +
        dataSize +
        // TODO(babenko): *2 are due to lack of reserve, remove this once simple sort
        // starts reserving arrays of appropriate sizes.
        (i64) 16 * spec->SortBy.size() * rowCount * 2 +
        (i64) 16 * rowCount * 2 +
        (i64) 32 * valueCount * 2 +
        FootprintMemorySize);
    return result;
}

TNodeResources GetPartitionSortDuringSortResources(
    TJobIOConfigPtr ioConfig,
    TSortOperationSpecPtr spec,
    i64 dataSize,
    i64 rowCount)
{
    TNodeResources result;
    result.set_slots(1);
    result.set_cores(1);
    result.set_memory(
        // NB: See comment above for GetSimpleSortJobResources.
        GetIOMemorySize(ioConfig, 0, 1) +
        dataSize +
        (i64) 16 * spec->SortBy.size() * rowCount +
        (i64) 12 * rowCount +
        FootprintMemorySize);
    result.set_network(spec->ShuffleNetworkLimit);
    return result;
}

TNodeResources GetPartitionSortDuringMapReduceResources(
    TJobIOConfigPtr ioConfig,
    TMapReduceOperationSpecPtr spec,
    i64 dataSize,
    i64 rowCount)
{
    TNodeResources result;
    result.set_slots(1);
    result.set_cores(1);
    result.set_memory(
        // NB: See comment above for GetSimpleSortJobResources.
        GetIOMemorySize(ioConfig, 0, 1) +
        dataSize +
        (i64) 16 * spec->SortBy.size() * rowCount +
        (i64) 12 * rowCount +
        FootprintMemorySize);
    result.set_network(spec->ShuffleNetworkLimit);
    return result;
}

TNodeResources GetSortedMergeDuringSortResources(
    TJobIOConfigPtr ioConfig,
    TSortOperationSpecPtr spec,
    int stripeCount)
{
    UNUSED(spec);
    NProto::TNodeResources result;
    result.set_slots(1);
    result.set_cores(1);
    result.set_memory(
        GetIOMemorySize(ioConfig, stripeCount, 1) +
        FootprintMemorySize);
    return result;
}

TNodeResources GetUnorderedMergeDuringSortResources(
    TJobIOConfigPtr ioConfig,
    TSortOperationSpecPtr spec)
{
    UNUSED(spec);
    TNodeResources result;
    result.set_slots(1);
    result.set_cores(1);
    result.set_memory(
        GetIOMemorySize(ioConfig, 1, 1) +
        FootprintMemorySize);
    return result;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

