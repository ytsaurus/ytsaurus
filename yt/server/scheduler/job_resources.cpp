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
    return Sprintf("Slots: %d/%d, Cpu: %d/%d, Memory: %d/%d, Network: %d/%d",
        // Slots
        utilization.slots(),
        limits.slots(),
        // Cpu
        utilization.cpu(),
        limits.cpu(),
        // Memory (in MB)
        static_cast<int>(utilization.memory() / (1024 * 1024)),
        static_cast<int>(limits.memory() / (1024 * 1024)),
        utilization.network(),
        limits.network());
}

Stroka FormatResources(const TNodeResources& resources)
{
    return Sprintf("Slots: %d, Cpu: %d, Memory: %d, Network: %d",
        resources.slots(),
        resources.cpu(),
        static_cast<int>(resources.memory() / (1024 * 1024)),
        resources.network());
}

TNodeResources operator + (const TNodeResources& lhs, const TNodeResources& rhs)
{
    TNodeResources result;
    result.set_slots(lhs.slots() + rhs.slots());
    result.set_cpu(lhs.cpu() + rhs.cpu());
    result.set_memory(lhs.memory() + rhs.memory());
    result.set_network(lhs.network() + rhs.network());
    return result;
}

TNodeResources& operator += (TNodeResources& lhs, const TNodeResources& rhs)
{
    lhs.set_slots(lhs.slots() + rhs.slots());
    lhs.set_cpu(lhs.cpu() + rhs.cpu());
    lhs.set_memory(lhs.memory() + rhs.memory());
    lhs.set_network(lhs.network() + rhs.network());
    return lhs;
}

TNodeResources& operator -= (TNodeResources& lhs, const TNodeResources& rhs)
{
    lhs.set_slots(lhs.slots() - rhs.slots());
    lhs.set_cpu(lhs.cpu() - rhs.cpu());
    lhs.set_memory(lhs.memory() - rhs.memory());
    lhs.set_network(lhs.network() - rhs.network());
    return lhs;
}

TNodeResources operator * (const TNodeResources& lhs, i64 rhs)
{
    TNodeResources result;
    result.set_slots(lhs.slots() * rhs);
    result.set_cpu(lhs.cpu() * rhs);
    result.set_memory(lhs.memory() * rhs);
    result.set_network(lhs.network() * rhs);
    return result;
}

TNodeResources operator * (const TNodeResources& lhs, double rhs)
{
    TNodeResources result;
    result.set_slots(static_cast<int>(lhs.slots() * rhs));
    result.set_cpu(static_cast<int>(lhs.cpu() * rhs));
    result.set_memory(static_cast<i64>(lhs.memory() * rhs));
    result.set_network(static_cast<int>(lhs.network() * rhs));
    return result;
}

TNodeResources& operator *= (TNodeResources& lhs, i64 rhs)
{
    lhs.set_slots(lhs.slots() * rhs);
    lhs.set_cpu(lhs.cpu() * rhs);
    lhs.set_memory(lhs.memory() * rhs);
    lhs.set_network(lhs.network() * rhs);
    return lhs;
}

TNodeResources& operator *= (TNodeResources& lhs, double rhs)
{
    lhs.set_slots(static_cast<int>(lhs.slots() * rhs));
    lhs.set_cpu(static_cast<int>(lhs.cpu() * rhs));
    lhs.set_memory(static_cast<i64>(lhs.memory() * rhs));
    lhs.set_network(static_cast<int>(lhs.network() * rhs));
    return lhs;
}

bool Dominates(const NProto::TNodeResources& lhs, const NProto::TNodeResources& rhs)
{
    return lhs.slots() >= rhs.slots() &&
           lhs.cpu() >= rhs.cpu() &&
           lhs.memory() >= rhs.memory() &&
           lhs.network() >= rhs.network();
}

TNodeResources Max(const TNodeResources& a, const TNodeResources& b)
{
    TNodeResources result;
    result.set_slots(std::max(a.slots(), b.slots()));
    result.set_cpu(std::max(a.cpu(), b.cpu()));
    result.set_memory(std::max(a.memory(), b.memory()));
    result.set_network(std::max(a.network(), b.network()));
    return result;
}

TNodeResources Min(const TNodeResources& a, const TNodeResources& b)
{
    TNodeResources result;
    result.set_slots(std::min(a.slots(), b.slots()));
    result.set_cpu(std::min(a.cpu(), b.cpu()));
    result.set_memory(std::min(a.memory(), b.memory()));
    result.set_network(std::min(a.network(), b.network()));
    return result;
}

EResourceType GetDominantResource(
    const NProto::TNodeResources& demand,
    const NProto::TNodeResources& limits)
{
    auto result = EResourceType::Cpu;
    double minRatio = -1.0;

    if (limits.cpu() > 0) {
        double newRatio = (double) demand.cpu() / limits.cpu();
        if (newRatio > minRatio) {
            result = EResourceType::Cpu;
            minRatio = newRatio;
        }
    }

    if (limits.memory() > 0) {
        double newRatio = (double) demand.memory() / limits.memory();
        if (newRatio > minRatio) {
            result = EResourceType::Memory;
            minRatio = newRatio;
        }
    }

    if (limits.network() > 0) {
        double newRatio = (double) demand.network() / limits.network();
        if (newRatio > minRatio) {
            result = EResourceType::Network;
            minRatio = newRatio;
        }
    }

    return result;
}

NProto::TNodeResources ComputeEffectiveLimits(
    const NProto::TNodeResources& limits,
    const NProto::TNodeResources& quantum)
{
    auto effectiveLimits = limits;

    if (quantum.slots() > 0) {
        effectiveLimits.set_slots(limits.slots() - limits.slots() % quantum.slots());
    }

    if (quantum.cpu() > 0) {
        effectiveLimits.set_cpu(limits.cpu() - limits.cpu() % quantum.cpu());
    }

    if (quantum.memory() > 0) {
        effectiveLimits.set_memory(limits.memory() - limits.memory() % quantum.memory());
    }

    if (quantum.network() > 0) {
        effectiveLimits.set_memory(limits.network() - limits.network() % quantum.network());
    }

    return effectiveLimits;
}

i64 GetResource(
    const NProto::TNodeResources& resources,
    EResourceType type)
{
    switch (type) {
        case EResourceType::Slots:
            return resources.slots();
        case EResourceType::Cpu:
            return resources.cpu();
        case EResourceType::Memory:
            return resources.memory();
        case EResourceType::Network:
            return resources.network();
        default:
            YUNREACHABLE();
    }
}

bool HasEnoughResources(
    const TNodeResources& currentUtilization,
    const TNodeResources& requestedUtilization,
    const TNodeResources& limits)
{
    return
        currentUtilization.slots() + requestedUtilization.slots() <= limits.slots() &&
        currentUtilization.cpu() + requestedUtilization.cpu() <= limits.cpu() &&
        currentUtilization.memory() + requestedUtilization.memory() <= limits.memory() &&
        currentUtilization.network() + requestedUtilization.network() <= limits.network();
}

bool HasSpareResources(
    const TNodeResources& utilization,
    const TNodeResources& limits)
{
    return
        utilization.slots() < limits.slots() &&
        utilization.cpu() < limits.cpu() &&
        utilization.memory() + LowWatermarkMemorySize < limits.memory();
}

void BuildNodeResourcesYson(
    const TNodeResources& resources,
    IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("slots").Scalar(resources.slots())
            .Item("cpu").Scalar(resources.cpu())
            .Item("memory").Scalar(resources.memory())
            .Item("network").Scalar(resources.network())
        .EndMap();
}

TNodeResources ZeroResources()
{
    TNodeResources result;
    result.set_slots(0);
    result.set_cpu(0);
    result.set_memory(0);
    result.set_network(0);
    return result;
}

TNodeResources InfiniteResources()
{
    TNodeResources result;
    result.set_slots(1000);
    result.set_cpu(1000);
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
        (ioConfig->TableWriter->WindowSize + // remote chunk writer window
        ioConfig->TableWriter->EncodeWindowSize + // codec window
        ioConfig->TableWriter->MaxBufferSize) * 
        outputStreamCount * 2; // possibly writing two chunks at the time at chunk change
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

