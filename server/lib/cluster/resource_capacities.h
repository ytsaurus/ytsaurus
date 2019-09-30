#pragma once

#include "public.h"

#include <yp/client/api/proto/data_model.pb.h>

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

TResourceCapacities& operator += (
    TResourceCapacities& lhs,
    const TResourceCapacities& rhs);

TResourceCapacities operator + (
    const TResourceCapacities& lhs,
    const TResourceCapacities& rhs);

bool Dominates(
    const TResourceCapacities& lhs,
    const TResourceCapacities& rhs);

TResourceCapacities Max(
    const TResourceCapacities& a,
    const TResourceCapacities& b);

TResourceCapacities SubtractWithClamp(
    const TResourceCapacities& lhs,
    const TResourceCapacities& rhs);

////////////////////////////////////////////////////////////////////////////////

TResourceCapacities MakeCpuCapacities(ui64 capacity);
TResourceCapacities MakeMemoryCapacities(ui64 capacity);
TResourceCapacities MakeNetworkCapacities(ui64 bandwidth);
TResourceCapacities MakeSlotCapacities(ui64 capacity);
TResourceCapacities MakeDiskCapacities(ui64 capacity, ui64 volumeSlots, ui64 bandwidth);
TResourceCapacities MakeGpuCapacities(ui64 capacity);

////////////////////////////////////////////////////////////////////////////////

ui64 GetHomogeneousCapacity(const TResourceCapacities& capacities);
ui64 GetCpuCapacity(const TResourceCapacities& capacities);
ui64 GetGpuCapacity(const TResourceCapacities& capacities);
ui64 GetMemoryCapacity(const TResourceCapacities& capacities);
ui64 GetNetworkBandwidth(const TResourceCapacities& capacities);
ui64 GetSlotCapacity(const TResourceCapacities& capacities);
ui64 GetDiskCapacity(const TResourceCapacities& capacities);
ui64 GetDiskBandwidth(const TResourceCapacities& capacities);

////////////////////////////////////////////////////////////////////////////////

ui64 GetDiskVolumeRequestBandwidthGuarantee(
    const NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest& request);

std::optional<ui64> GetDiskVolumeRequestOptionalBandwidthLimit(
    const NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest& request);

TResourceCapacities GetDiskVolumeRequestCapacities(
    const NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest& request);

bool GetDiskVolumeRequestExclusive(
    const NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest& request);

NClient::NApi::NProto::EDiskVolumePolicy GetDiskVolumeRequestPolicy(
    const NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest& request);

////////////////////////////////////////////////////////////////////////////////

TResourceCapacities GetGpuRequestCapacities(
    const NClient::NApi::NProto::TPodSpec_TGpuRequest& request);

////////////////////////////////////////////////////////////////////////////////

TResourceCapacities GetResourceCapacities(
    const NClient::NApi::NProto::TResourceSpec& spec);

TResourceCapacities GetAllocationCapacities(
    const NClient::NApi::NProto::TResourceStatus_TAllocation& allocation);

bool GetAllocationExclusive(
    const NClient::NApi::NProto::TResourceStatus_TAllocation& allocation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
