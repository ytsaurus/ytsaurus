#include "stdafx.h"
#include "helpers.h"

#include <core/ytree/fluent.h>

#include <ytlib/object_client/helpers.h>

#include <limits>

namespace NYT {
namespace NNodeTrackerClient {

using namespace NYson;
using namespace NYTree;
using namespace NObjectClient;
using namespace NNodeTrackerClient::NProto;

////////////////////////////////////////////////////////////////////

Stroka FormatResourceUsage(
    const TNodeResources& usage,
    const TNodeResources& limits)
{
    return Format(
        "UserSlots: %v/%v, Cpu: %v/%v, Memory: %v/%v, Network: %v/%v, "
        "ReplicationSlots: %v/%v, RemovalSlots: %v/%v, RepairSlots: %v/%v, SealSlots: %v/%v",
        // User slots
        usage.user_slots(),
        limits.user_slots(),
        // Cpu
        usage.cpu(),
        limits.cpu(),
        // Memory (in MB)
        usage.memory() / (1024 * 1024),
        limits.memory() / (1024 * 1024),
        // Network
        usage.network(),
        limits.network(),
        // Replication slots
        usage.replication_slots(),
        limits.replication_slots(),
        // Removal slots
        usage.removal_slots(),
        limits.removal_slots(),
        // Repair slots
        usage.repair_slots(),
        limits.repair_slots(),
        // Seal slots
        usage.seal_slots(),
        limits.seal_slots());
}

Stroka FormatResources(const TNodeResources& resources)
{
    return Format(
        "UserSlots: %v, Cpu: %v, Memory: %v, Network: %v, "
        "ReplicationSlots: %v, RemovalSlots: %v, RepairSlots: %v, SealSlots: %v",
        resources.user_slots(),
        resources.cpu(),
        resources.memory() / (1024 * 1024),
        resources.network(),
        resources.replication_slots(),
        resources.removal_slots(),
        resources.repair_slots(),
        resources.seal_slots());
}

void ProfileResources(NProfiling::TProfiler& profiler, const TNodeResources& resources)
{
    profiler.Enqueue("/user_slots", resources.user_slots());
    profiler.Enqueue("/cpu", resources.cpu());
    profiler.Enqueue("/memory", resources.memory());
    profiler.Enqueue("/replication_slots", resources.replication_slots());
    profiler.Enqueue("/removal_slots", resources.removal_slots());
    profiler.Enqueue("/repair_slots", resources.repair_slots());
    profiler.Enqueue("/seal_slots", resources.seal_slots());
}

EResourceType GetDominantResource(
    const TNodeResources& demand,
    const TNodeResources& limits)
{
    auto maxType = EResourceType::Cpu;
    double maxRatio = 0.0;
    auto update = [&] (i64 a, i64 b, EResourceType type) {
        if (b > 0) {
            double ratio = (double) a / b;
            if (ratio > maxRatio) {
                maxRatio = ratio;
                maxType = type;
            }
        }
    };
    update(demand.user_slots(), limits.user_slots(), EResourceType::UserSlots);    
    update(demand.cpu(), limits.cpu(), EResourceType::Cpu);
    update(demand.memory(), limits.memory(), EResourceType::Memory);
    update(demand.network(), limits.network(), EResourceType::Network);
    return maxType;
}

i64 GetResource(const TNodeResources& resources, EResourceType type)
{
    switch (type) {
        case EResourceType::UserSlots:
            return resources.user_slots();
        case EResourceType::Cpu:
            return resources.cpu();
        case EResourceType::Memory:
            return resources.memory();
        case EResourceType::Network:
            return resources.network();
        case EResourceType::ReplicationSlots:
            return resources.replication_slots();
        case EResourceType::RemovalSlots:
            return resources.removal_slots();
        case EResourceType::RepairSlots:
            return resources.repair_slots();
        case EResourceType::SealSlots:
            return resources.seal_slots();
        default:
            YUNREACHABLE();
    }
}

void SetResource(TNodeResources& resources, EResourceType type, i64 value)
{
    switch (type) {
        case EResourceType::UserSlots:
            resources.set_user_slots(static_cast<i32>(value));
            break;
        case EResourceType::Cpu:
            resources.set_cpu(static_cast<i32>(value));
            break;
        case EResourceType::Memory:
            resources.set_memory(value);
            break;
        case EResourceType::Network:
            resources.set_network(static_cast<i32>(value));
            break;
        case EResourceType::ReplicationSlots:
            resources.set_replication_slots(static_cast<i32>(value));
            break;
        case EResourceType::RemovalSlots:
            resources.set_removal_slots(static_cast<i32>(value));
            break;
        case EResourceType::RepairSlots:
            resources.set_repair_slots(static_cast<i32>(value));
            break;
        case EResourceType::SealSlots:
            resources.set_seal_slots(static_cast<i32>(value));
            break;
        default:
            YUNREACHABLE();
    }
}

double GetMinResourceRatio(
    const TNodeResources& nominator,
    const TNodeResources& denominator)
{
    double result = std::numeric_limits<double>::infinity();
    auto update = [&] (i64 a, i64 b) {
        if (b > 0) {
            result = std::min(result, (double) a / b);
        }
    };
    update(nominator.user_slots(), denominator.user_slots());
    update(nominator.cpu(), denominator.cpu());
    update(nominator.memory(), denominator.memory());
    update(nominator.network(), denominator.network());
    return result;
}

TNodeResources GetAdjustedResourceLimits(
    const TNodeResources& demand,
    const TNodeResources& limits,
    int nodeCount)
{
    auto adjustedLimits = limits;

    // Take memory granularity into account.
    if (demand.user_slots() > 0 && nodeCount > 0) {
        i64 memoryDemandPerJob = demand.memory() / demand.user_slots();
        i64 memoryLimitPerNode = limits.memory() / nodeCount;
        int slotsPerNode = memoryLimitPerNode / memoryDemandPerJob;
        i64 adjustedMemoryLimit = slotsPerNode * memoryDemandPerJob * nodeCount;
        adjustedLimits.set_memory(adjustedMemoryLimit);
    }

    return adjustedLimits;
}

TNodeResources GetZeroNodeResources()
{
    TNodeResources result;
    result.set_user_slots(0);
    result.set_cpu(0);
    result.set_memory(0);
    result.set_network(0);
    result.set_replication_slots(0);
    result.set_removal_slots(0);
    result.set_repair_slots(0);
    result.set_seal_slots(0);
    return result;
}

const TNodeResources& ZeroNodeResources()
{
    static auto value = GetZeroNodeResources();
    return value;
}

TNodeResources GetInfiniteResources()
{
    TNodeResources result;
    result.set_user_slots(1000000);
    result.set_cpu(1000000);
    result.set_memory((i64) 1000000000000000000);
    result.set_network(1000000);
    result.set_replication_slots(1000000);
    result.set_removal_slots(1000000);
    result.set_repair_slots(1000000);
    result.set_seal_slots(1000000);
    return result;
}

const TNodeResources& InfiniteNodeResources()
{
    static auto result = GetInfiniteResources();
    return result;
}

TObjectId ObjectIdFromNodeId(TNodeId nodeId, TCellTag cellTag)
{
    return MakeId(EObjectType::ClusterNode, cellTag, nodeId, 0);
}

TNodeId NodeIdFromObjectId(const TObjectId& objectId)
{
    return CounterFromId(objectId);
}

namespace NProto {

TNodeResources operator + (const TNodeResources& lhs, const TNodeResources& rhs)
{
    TNodeResources result;
    result.set_user_slots(lhs.user_slots() + rhs.user_slots());
    result.set_cpu(lhs.cpu() + rhs.cpu());
    result.set_memory(lhs.memory() + rhs.memory());
    result.set_network(lhs.network() + rhs.network());
    result.set_replication_slots(lhs.replication_slots() + rhs.replication_slots());
    result.set_removal_slots(lhs.removal_slots() + rhs.removal_slots());
    result.set_repair_slots(lhs.repair_slots() + rhs.repair_slots());
    result.set_seal_slots(lhs.seal_slots() + rhs.seal_slots());
    return result;
}

TNodeResources& operator += (TNodeResources& lhs, const TNodeResources& rhs)
{
    lhs.set_user_slots(lhs.user_slots() + rhs.user_slots());
    lhs.set_cpu(lhs.cpu() + rhs.cpu());
    lhs.set_memory(lhs.memory() + rhs.memory());
    lhs.set_network(lhs.network() + rhs.network());
    lhs.set_replication_slots(lhs.replication_slots() + rhs.replication_slots());
    lhs.set_removal_slots(lhs.removal_slots() + rhs.removal_slots());
    lhs.set_repair_slots(lhs.repair_slots() + rhs.repair_slots());
    lhs.set_seal_slots(lhs.seal_slots() + rhs.seal_slots());
    return lhs;
}

TNodeResources operator - (const TNodeResources& lhs, const TNodeResources& rhs)
{
    TNodeResources result;
    result.set_user_slots(lhs.user_slots() - rhs.user_slots());
    result.set_cpu(lhs.cpu() - rhs.cpu());
    result.set_memory(lhs.memory() - rhs.memory());
    result.set_network(lhs.network() - rhs.network());
    result.set_replication_slots(lhs.replication_slots() - rhs.replication_slots());
    result.set_removal_slots(lhs.removal_slots() - rhs.removal_slots());
    result.set_repair_slots(lhs.repair_slots() - rhs.repair_slots());
    result.set_seal_slots(lhs.seal_slots() - rhs.seal_slots());
    return result;
}

TNodeResources& operator -= (TNodeResources& lhs, const TNodeResources& rhs)
{
    lhs.set_user_slots(lhs.user_slots() - rhs.user_slots());
    lhs.set_cpu(lhs.cpu() - rhs.cpu());
    lhs.set_memory(lhs.memory() - rhs.memory());
    lhs.set_network(lhs.network() - rhs.network());
    lhs.set_replication_slots(lhs.replication_slots() - rhs.replication_slots());
    lhs.set_removal_slots(lhs.removal_slots() - rhs.removal_slots());
    lhs.set_repair_slots(lhs.repair_slots() - rhs.repair_slots());
    lhs.set_seal_slots(lhs.seal_slots() - rhs.seal_slots());
    return lhs;
}

TNodeResources operator * (const TNodeResources& lhs, i64 rhs)
{
    TNodeResources result;
    result.set_user_slots(lhs.user_slots() * rhs);
    result.set_cpu(lhs.cpu() * rhs);
    result.set_memory(lhs.memory() * rhs);
    result.set_network(lhs.network() * rhs);
    result.set_replication_slots(lhs.replication_slots() * rhs);
    result.set_removal_slots(lhs.removal_slots() * rhs);
    result.set_repair_slots(lhs.repair_slots() * rhs);
    result.set_seal_slots(lhs.seal_slots() * rhs);
    return result;
}

TNodeResources operator * (const TNodeResources& lhs, double rhs)
{
    TNodeResources result;
    result.set_user_slots(static_cast<int>(lhs.user_slots() * rhs + 0.5));
    result.set_cpu(static_cast<int>(lhs.cpu() * rhs + 0.5));
    result.set_memory(static_cast<i64>(lhs.memory() * rhs + 0.5));
    result.set_network(static_cast<int>(lhs.network() * rhs + 0.5));
    result.set_replication_slots(static_cast<int>(lhs.replication_slots() * rhs + 0.5));
    result.set_removal_slots(static_cast<int>(lhs.removal_slots() * rhs + 0.5));
    result.set_repair_slots(static_cast<int>(lhs.repair_slots() * rhs + 0.5));
    result.set_seal_slots(static_cast<int>(lhs.seal_slots() * rhs + 0.5));
    return result;
}

TNodeResources& operator *= (TNodeResources& lhs, i64 rhs)
{
    lhs.set_user_slots(lhs.user_slots() * rhs);
    lhs.set_cpu(lhs.cpu() * rhs);
    lhs.set_memory(lhs.memory() * rhs);
    lhs.set_network(lhs.network() * rhs);
    lhs.set_replication_slots(lhs.replication_slots() * rhs);
    lhs.set_removal_slots(lhs.removal_slots() * rhs);
    lhs.set_repair_slots(lhs.repair_slots() * rhs);
    lhs.set_seal_slots(lhs.seal_slots() * rhs);
    return lhs;
}

TNodeResources& operator *= (TNodeResources& lhs, double rhs)
{
    lhs.set_user_slots(static_cast<int>(lhs.user_slots() * rhs + 0.5));
    lhs.set_cpu(static_cast<int>(lhs.cpu() * rhs + 0.5));
    lhs.set_memory(static_cast<i64>(lhs.memory() * rhs + 0.5));
    lhs.set_network(static_cast<int>(lhs.network() * rhs + 0.5));
    lhs.set_replication_slots(static_cast<int>(lhs.replication_slots() * rhs + 0.5));
    lhs.set_removal_slots(static_cast<int>(lhs.removal_slots() * rhs + 0.5));
    lhs.set_repair_slots(static_cast<int>(lhs.repair_slots() * rhs + 0.5));
    lhs.set_seal_slots(static_cast<int>(lhs.seal_slots() * rhs + 0.5));
    return lhs;
}

TNodeResources  operator - (const TNodeResources& resources)
{
    TNodeResources result;
    result.set_user_slots(-resources.user_slots());
    result.set_cpu(-resources.cpu());
    result.set_memory(-resources.memory());
    result.set_network(-resources.network());
    result.set_replication_slots(-resources.replication_slots());
    result.set_removal_slots(-resources.removal_slots());
    result.set_repair_slots(-resources.repair_slots());
    result.set_seal_slots(-resources.seal_slots());
    return result;
}

bool operator == (const TNodeResources& lhs, const TNodeResources& rhs)
{
    return lhs.user_slots() == rhs.user_slots() &&
           lhs.cpu() == rhs.cpu() &&
           lhs.memory() == rhs.memory() &&
           lhs.network() == rhs.network() &&
           lhs.replication_slots() == rhs.replication_slots() &&
           lhs.removal_slots() == rhs.removal_slots() &&
           lhs.repair_slots() == rhs.repair_slots() &&
           lhs.seal_slots() == rhs.seal_slots();
}

bool operator != (const TNodeResources& lhs, const TNodeResources& rhs)
{
    return !(lhs == rhs);
}

TNodeResources MakeNonnegative(const TNodeResources& resources)
{
    TNodeResources result;
    result.set_user_slots(std::max(i32(0), resources.user_slots()));
    result.set_cpu(std::max(i32(0), resources.cpu()));
    result.set_memory(std::max(i64(0), resources.memory()));
    result.set_network(std::max(i32(0), resources.network()));
    result.set_replication_slots(std::max(i32(0), resources.replication_slots()));
    result.set_removal_slots(std::max(i32(0), resources.removal_slots()));
    result.set_repair_slots(std::max(i32(0), resources.repair_slots()));
    result.set_seal_slots(std::max(i32(0), resources.seal_slots()));
    return result;
}

bool Dominates(const TNodeResources& lhs, const TNodeResources& rhs)
{
    return lhs.user_slots() >= rhs.user_slots() &&
           lhs.cpu() >= rhs.cpu() &&
           lhs.memory() >= rhs.memory() &&
           lhs.network() >= rhs.network() &&
           lhs.replication_slots() >= rhs.replication_slots() &&
           lhs.removal_slots() >= rhs.removal_slots() &&
           lhs.repair_slots() >= rhs.repair_slots() &&
           lhs.seal_slots() >= rhs.seal_slots();
}

bool DominatesNonnegative(const TNodeResources& lhs, const TNodeResources& rhs)
{
    auto nonnegLhs = MakeNonnegative(lhs);
    auto nonnegRhs = MakeNonnegative(rhs);
    return Dominates(nonnegLhs, nonnegRhs);
}

TNodeResources Max(const TNodeResources& a, const TNodeResources& b)
{
    TNodeResources result;
    result.set_user_slots(std::max(a.user_slots(), b.user_slots()));
    result.set_cpu(std::max(a.cpu(), b.cpu()));
    result.set_memory(std::max(a.memory(), b.memory()));
    result.set_network(std::max(a.network(), b.network()));
    result.set_replication_slots(std::max(a.replication_slots(), b.replication_slots()));
    result.set_removal_slots(std::max(a.removal_slots(), b.removal_slots()));
    result.set_repair_slots(std::max(a.repair_slots(), b.repair_slots()));
    result.set_seal_slots(std::max(a.seal_slots(), b.seal_slots()));
    return result;
}

TNodeResources Min(const TNodeResources& a, const TNodeResources& b)
{
    TNodeResources result;
    result.set_user_slots(std::min(a.user_slots(), b.user_slots()));
    result.set_cpu(std::min(a.cpu(), b.cpu()));
    result.set_memory(std::min(a.memory(), b.memory()));
    result.set_network(std::min(a.network(), b.network()));
    result.set_replication_slots(std::min(a.replication_slots(), b.replication_slots()));
    result.set_removal_slots(std::min(a.removal_slots(), b.removal_slots()));
    result.set_repair_slots(std::min(a.repair_slots(), b.repair_slots()));
    result.set_seal_slots(std::min(a.seal_slots(), b.seal_slots()));
    return result;
}

void Serialize(const TNodeResources& resources, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("user_slots").Value(resources.user_slots())
            .Item("cpu").Value(resources.cpu())
            .Item("memory").Value(resources.memory())
            .Item("network").Value(resources.network())
            .Item("replication_slots").Value(resources.replication_slots())
            .Item("removal_slots").Value(resources.removal_slots())
            .Item("repair_slots").Value(resources.repair_slots())
            .Item("seal_slots").Value(resources.seal_slots())
        .EndMap();
}

} // namespace NProto

////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerClient
} // namespace NYT

