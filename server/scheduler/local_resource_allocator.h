#pragma once

#include "public.h"

#include <yp/server/lib/cluster/allocation_statistics.h>
#include <yp/server/lib/cluster/resource_capacities.h>

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/small_vector.h>

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TLocalResourceAllocator
{
public:
    struct TResource;
    struct TRequest;
    struct TAllocation;

    struct TAllocation
    {
        // Globally unique identifier.
        // Null for anonymous allocations (e.g. cpu, memory).
        TObjectId Id;
        TObjectId PodId;
        NCluster::TResourceCapacities Capacities = {};
        bool Exclusive = false;

        // Opaque
        const NClient::NApi::NProto::TResourceStatus_TAllocation* ProtoAllocation = nullptr;
    };

    struct TResource
    {
        TObjectId Id;
        EResourceKind Kind;
        NCluster::TResourceCapacities Capacities = {};
        SmallVector<TAllocation, 8> ScheduledAllocations;
        SmallVector<TAllocation, 8> ActualAllocations;

        // Opaque
        const NClient::NApi::NProto::TResourceSpec_TDiskSpec* ProtoDiskSpec = nullptr;
        const NClient::NApi::NProto::TResourceSpec_TGpuSpec* ProtoGpuSpec = nullptr;
    };

    struct TRequest
    {
        // Identifies a request within pod spec.
        // Null for anonymous allocations.
        TObjectId Id;

        // Identifier of the existing allocation matching this request
        // or null if there is no such allocation.
        // Always null for anonymous allocations (e.g. cpu, memory).
        TObjectId AllocationId;

        EResourceKind Kind;
        bool Exclusive = false;
        NCluster::TResourceCapacities Capacities = {};
        SmallVector<const TResource*, 9> MatchingResources;

        // Opaque
        const NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest* ProtoVolumeRequest = nullptr;
        const NClient::NApi::NProto::TPodSpec_TGpuRequest* ProtoGpuRequest = nullptr;
    };

    struct TResponse
    {
        // Always non-null after successful call to #TryAllocate.
        const TResource* Resource = nullptr;
        // Non-null if the request must be served with an existing allocation.
        const TAllocation* ExistingAllocation = nullptr;
    };

    //! Attempts to make allocations.
    /*!
     *  Returns |true| on success; |false| on failure; in the latter case \param errors (if not null)
     *  is populated with the errors.
     *
     *  On success, fills \param responses.
     */
    bool TryAllocate(
        const TObjectId& podId,
        const std::vector<TRequest>& requests,
        const std::vector<TResource>& resources,
        std::vector<TResponse>* responses,
        std::vector<TError>* errors);

private:
    std::vector<NCluster::TAllocationStatistics> ResourceStatistics_;

    static TString FormatRequest(const TRequest& request);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
