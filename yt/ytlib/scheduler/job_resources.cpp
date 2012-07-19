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
    return Sprintf("Slots: %d/%d, Cores: %d/%d, Memory: %d/%d",
        // Slots
        utilization.slots(),
        limits.slots(),
        // Cores
        utilization.cores(),
        limits.cores(),
        // Memory (in MB)
        static_cast<int>(utilization.memory() / (1024 * 1024)),
        static_cast<int>(limits.memory() / (1024 * 1024)));
}

Stroka FormatResources(const TNodeResources& resources)
{
    return Sprintf("Slots: %d, Cores: %d, Memory: %d",
        resources.slots(),
        resources.cores(),
        static_cast<int>(resources.memory() / (1024 * 1024)));
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
        utilization.memory() + LowWatermarkMemorySize < limits.memory();
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

TNodeResources ZeroResources()
{
    TNodeResources result;
    result.set_slots(0);
    result.set_cores(0);
    result.set_memory(0);
    return result;
}

TNodeResources InfiniteResources()
{
    TNodeResources result;
    result.set_slots(1000);
    result.set_cores(1000);
    result.set_memory((i64) 1024 * 1024 * 1024 * 1024);
    return result;
}

i64 GetIOMemorySize(
    TJobIOConfigPtr ioConfig,
    int inputStreamCount,
    int outputStreamCount)
{
    return
        ioConfig->ChunkSequenceReader->SequentialReader->WindowSize * ioConfig->ChunkSequenceReader->PrefetchWindow * inputStreamCount +
        ioConfig->ChunkSequenceWriter->RemoteWriter->WindowSize * outputStreamCount +
        ioConfig->ChunkSequenceWriter->ChunkWriter->EncodingWriter->WindowSize * outputStreamCount;
}

TNodeResources GetMapJobResources(
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

TNodeResources GetReduceJobResources(
    TJobIOConfigPtr ioConfig,
    TReduceOperationSpecPtr spec)
{
    TNodeResources result;
    result.set_slots(1);
    result.set_cores(spec->Reducer->CoresLimit);
    result.set_memory(
        GetIOMemorySize(ioConfig, spec->InputTablePaths.size(), 1) +
        spec->Reducer->MemoryLimit +
        FootprintMemorySize);
    return result;
}

TNodeResources GetEraseJobResources(
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

TNodeResources GetPartitionJobResources(
    TJobIOConfigPtr ioConfig,
    i64 dataWeight,
    int partitionCount)
{
    TNodeResources result;
    result.set_slots(1);
    result.set_cores(1);
    result.set_memory(
        GetIOMemorySize(ioConfig, 1, 1) +
        std::min(ioConfig->ChunkSequenceWriter->ChunkWriter->BlockSize * partitionCount, dataWeight) +
        FootprintMemorySize);
    return result;
}

TNodeResources GetSimpleSortJobResources(
    TJobIOConfigPtr ioConfig,
    TSortOperationSpecPtr spec,
    i64 dataWeight,
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
        dataWeight +
        // TODO(babenko): *2 are due to lack of reserve, remove this once simple sort
        // starts reserving arrays of appropriate sizes.
        (i64) 16 * spec->KeyColumns.size() * rowCount * 2 +
        (i64) 16 * rowCount * 2 +
        (i64) 32 * valueCount * 2 +
        FootprintMemorySize);
    return result;
}

TNodeResources GetPartitionSortJobResources(
    TJobIOConfigPtr ioConfig,
    TSortOperationSpecPtr spec,
    i64 dataWeight,
    i64 rowCount)
{
    TNodeResources result;
    result.set_slots(1);
    result.set_cores(1);
    result.set_memory(
        // NB: See comment above for GetSimpleSortJobResources.
        GetIOMemorySize(ioConfig, 0, 1) +
        dataWeight +
        (i64) 16 * spec->KeyColumns.size() * rowCount +
        (i64) 8 * rowCount +
        FootprintMemorySize);
    return result;
}

TNodeResources GetSortedMergeDuringSortJobResources(
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

TNodeResources GetUnorderedMergeDuringSortJobResources(
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

