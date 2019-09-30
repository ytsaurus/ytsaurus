#include "resource_capacities.h"

#include <yt/core/misc/error.h>

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

TResourceCapacities& operator += (
    TResourceCapacities& lhs,
    const TResourceCapacities& rhs)
{
    for (size_t index = 0; index < MaxResourceDimensions; ++index) {
        lhs[index] += rhs[index];
    }
    return lhs;
}

TResourceCapacities operator + (
    const TResourceCapacities& lhs,
    const TResourceCapacities& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

bool Dominates(
    const TResourceCapacities& lhs,
    const TResourceCapacities& rhs)
{
    for (size_t index = 0; index < MaxResourceDimensions; ++index) {
        if (lhs[index] < rhs[index]) {
            return false;
        }
    }
    return true;
}

TResourceCapacities Max(
    const TResourceCapacities& a,
    const TResourceCapacities& b)
{
    TResourceCapacities result = {};
    for (size_t index = 0; index < MaxResourceDimensions; ++index) {
        result[index] = std::max(a[index], b[index]);
    }
    return result;
}

TResourceCapacities SubtractWithClamp(
    const TResourceCapacities& lhs,
    const TResourceCapacities& rhs)
{
    TResourceCapacities result = {};
    for (size_t index = 0; index < MaxResourceDimensions; ++index) {
        result[index] = lhs[index] < rhs[index] ? 0 : lhs[index] - rhs[index];
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TResourceCapacities MakeCpuCapacities(ui64 capacity)
{
    return {{capacity, 0, 0}};
}

TResourceCapacities MakeMemoryCapacities(ui64 capacity)
{
    return {{capacity, 0, 0}};
}

TResourceCapacities MakeNetworkCapacities(ui64 bandwidth)
{
    return {{bandwidth, 0, 0}};
}

TResourceCapacities MakeSlotCapacities(ui64 capacity)
{
    return {{capacity, 0, 0}};
}

TResourceCapacities MakeDiskCapacities(ui64 capacity, ui64 volumeSlots, ui64 bandwidth)
{
    return {{capacity, volumeSlots, bandwidth}};
}

TResourceCapacities MakeGpuCapacities(ui64 capacity)
{
    return {{capacity, 0, 0}};
}

////////////////////////////////////////////////////////////////////////////////

ui64 GetHomogeneousCapacity(const TResourceCapacities& capacities)
{
    return capacities[0];
}

ui64 GetCpuCapacity(const TResourceCapacities& capacities)
{
    return GetHomogeneousCapacity(capacities);
}

ui64 GetGpuCapacity(const TResourceCapacities& capacities)
{
    return GetHomogeneousCapacity(capacities);
}

ui64 GetMemoryCapacity(const TResourceCapacities& capacities)
{
    return GetHomogeneousCapacity(capacities);
}

ui64 GetNetworkBandwidth(const TResourceCapacities& capacities)
{
    return GetHomogeneousCapacity(capacities);
}

ui64 GetSlotCapacity(const TResourceCapacities& capacities)
{
    return GetHomogeneousCapacity(capacities);
}

ui64 GetDiskCapacity(const TResourceCapacities& capacities)
{
    return capacities[0];
}

ui64 GetDiskBandwidth(const TResourceCapacities& capacities)
{
    return capacities[2];
}

////////////////////////////////////////////////////////////////////////////////

ui64 GetDiskVolumeRequestBandwidthGuarantee(
    const NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest& request)
{
    if (request.has_quota_policy()) {
        return request.quota_policy().bandwidth_guarantee();
    } else if (request.has_exclusive_policy()) {
        return request.exclusive_policy().min_bandwidth();
    } else {
        THROW_ERROR_EXCEPTION("Malformed disk volume request");
    }
}

std::optional<ui64> GetDiskVolumeRequestOptionalBandwidthLimit(
    const NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest& request)
{
    if (request.has_quota_policy()) {
        if (request.quota_policy().has_bandwidth_limit()) {
            return request.quota_policy().bandwidth_limit();
        }
        return std::nullopt;
    } else if (request.has_exclusive_policy()) {
        return std::nullopt;
    } else {
        THROW_ERROR_EXCEPTION("Malformed disk volume request");
    }
}

TResourceCapacities GetDiskVolumeRequestCapacities(
    const NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest& request)
{
    if (request.has_quota_policy()) {
        return MakeDiskCapacities(
            request.quota_policy().capacity(),
            /*volumeSlots*/ 1,
            request.quota_policy().bandwidth_guarantee());
    } else if (request.has_exclusive_policy()) {
        return MakeDiskCapacities(
            request.exclusive_policy().min_capacity(),
            /*volumeSlots*/ 1,
            request.exclusive_policy().min_bandwidth());
    } else {
        THROW_ERROR_EXCEPTION("Malformed disk volume request");
    }
}

bool GetDiskVolumeRequestExclusive(
    const NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest& request)
{
    return request.has_exclusive_policy();
}

NClient::NApi::NProto::EDiskVolumePolicy GetDiskVolumeRequestPolicy(
    const NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest& request)
{
    if (request.has_quota_policy()) {
        return NClient::NApi::NProto::DVP_QUOTA;
    } else if (request.has_exclusive_policy()) {
        return NClient::NApi::NProto::DVP_EXCLUSIVE;
    } else {
        THROW_ERROR_EXCEPTION("Malformed disk volume request");
    }
}

////////////////////////////////////////////////////////////////////////////////

TResourceCapacities GetGpuRequestCapacities(
    const NClient::NApi::NProto::TPodSpec_TGpuRequest& /*request*/)
{
    return MakeGpuCapacities(1);
}

////////////////////////////////////////////////////////////////////////////////

TResourceCapacities GetResourceCapacities(const NClient::NApi::NProto::TResourceSpec& spec)
{
    if (spec.has_cpu()) {
        return MakeCpuCapacities(spec.cpu().total_capacity());
    } else if (spec.has_memory()) {
        return MakeMemoryCapacities(spec.memory().total_capacity());
    } else if (spec.has_network()) {
        return MakeNetworkCapacities(spec.network().total_bandwidth());
    } else if (spec.has_slot()) {
        return MakeSlotCapacities(spec.slot().total_capacity());
    } else if (spec.has_disk()) {
        return MakeDiskCapacities(
            spec.disk().total_capacity(),
            spec.disk().total_volume_slots(),
            spec.disk().total_bandwidth());
    } else if (spec.has_gpu()) {
        return MakeGpuCapacities(1);
    } else {
        THROW_ERROR_EXCEPTION("Malformed resource spec");
    }
}

TResourceCapacities GetAllocationCapacities(const NClient::NApi::NProto::TResourceStatus_TAllocation& allocation)
{
    if (allocation.has_cpu()) {
        return MakeCpuCapacities(allocation.cpu().capacity());
    } else if (allocation.has_memory()) {
        return MakeMemoryCapacities(allocation.memory().capacity());
    } else if (allocation.has_network()) {
        return MakeNetworkCapacities(allocation.network().bandwidth());
    } else if (allocation.has_slot()) {
        return MakeSlotCapacities(allocation.slot().capacity());
    } else if (allocation.has_disk()) {
        return MakeDiskCapacities(
            allocation.disk().capacity(),
            /*volumeSlots*/ 1,
            allocation.disk().bandwidth());
    } else if (allocation.has_gpu()) {
        return MakeGpuCapacities(allocation.gpu().capacity());
    } else {
        THROW_ERROR_EXCEPTION("Malformed resource allocation");
    }
}

bool GetAllocationExclusive(const NClient::NApi::NProto::TResourceStatus_TAllocation& allocation)
{
    return allocation.has_disk() && allocation.disk().exclusive();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
