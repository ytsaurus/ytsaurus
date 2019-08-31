#pragma once

#include "public.h"

#include <yp/client/api/proto/data_model.pb.h>

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

struct TAllocationStatistics
{
    TResourceCapacities Capacities = {};
    bool Used = false;
    bool UsedExclusively = false;
};

////////////////////////////////////////////////////////////////////////////////

TAllocationStatistics Max(
    const TAllocationStatistics& lhs,
    const TAllocationStatistics& rhs);

TAllocationStatistics& operator += (
    TAllocationStatistics& lhs,
    const TAllocationStatistics& rhs);

TAllocationStatistics operator + (
    const TAllocationStatistics& lhs,
    const TAllocationStatistics& rhs);

////////////////////////////////////////////////////////////////////////////////

NClient::NApi::NProto::TResourceStatus_TAllocationStatistics ResourceCapacitiesToStatistics(
    const TResourceCapacities& capacities,
    EResourceKind kind);

TAllocationStatistics ComputeTotalAllocationStatistics(
    const std::vector<NYP::NClient::NApi::NProto::TResourceStatus_TAllocation>& scheduledAllocations,
    const std::vector<NYP::NClient::NApi::NProto::TResourceStatus_TAllocation>& actualAllocations);

void Accumulate(
    TAllocationStatistics& statistics,
    const NClient::NApi::NProto::TResourceStatus_TAllocation& allocation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
