#pragma once

#include "private.h"

#include <yp/server/objects/public.h>

#include <yt/core/misc/small_vector.h>

#include <yp/client/api/proto/data_model.pb.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TLocalResourceAllocator
{
public:
    struct TResource;
    struct TRequest;
    struct TAllocation;

    struct TAllocation
    {
        // Could be null for anonymous allocations (e.g. cpu, memory).
        NObjects::TObjectId RequestId;
        NObjects::TObjectId PodId;
        TResourceCapacities Capacities = {};
        bool Exclusive = false;

        // Opaque
        const NClient::NApi::NProto::TResourceStatus_TAllocation* ProtoAllocation = nullptr;
    };

    struct TResource
    {
        NObjects::TObjectId Id;
        EResourceKind Kind;
        TResourceCapacities Capacities = {};
        SmallVector<TAllocation, 8> ScheduledAllocations;
        SmallVector<TAllocation, 8> ActualAllocations;

        // Opaque
        const NClient::NApi::NProto::TResourceSpec_TDiskSpec* ProtoDiskSpec = nullptr;
    };

    struct TRequest
    {
        // Identifies a request within pod spec.
        // Null for anonymous allocations.
        NObjects::TObjectId Id;

        // Globally unique; identifies a specific allocation.
        // Null for anonymous allocations.
        NObjects::TObjectId AllocationId;

        EResourceKind Kind;
        bool Exclusive = false;
        TResourceCapacities Capacities = {};
        SmallVector<const TResource*, 9> MatchingResources;

        // Opaque
        const NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest* ProtoVolumeRequest = nullptr;
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
        const NObjects::TObjectId& podId,
        const std::vector<TRequest>& requests,
        const std::vector<TResource>& resources,
        std::vector<TResponse>* responses,
        std::vector<TError>* errors);

private:
    std::vector<TAllocationStatistics> ResourceStatistics_;

    static TString FormatRequest(const TRequest& request);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
