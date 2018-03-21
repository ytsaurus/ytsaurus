#pragma once

#include "private.h"

#include <yp/server/objects/public.h>

#include <yp/client/api/proto/data_model.pb.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

bool IsHomogeneous(EResourceKind kind);

////////////////////////////////////////////////////////////////////////////////

class TLocalResourceAllocator
{
public:
    struct TRequest
    {
        NServer::NObjects::EResourceKind Kind;
        ui64 Capacity;
    };

    struct TResource
    {
        NObjects::TObjectId Id;
        const NClient::NApi::NProto::TResourceSpec* Spec;
        const std::vector<NClient::NApi::NProto::TResourceStatus_TAllocation>* ScheduledAllocations;
        const std::vector<NClient::NApi::NProto::TResourceStatus_TAllocation>* ActualAllocations;
    };

    struct TAllocation
    {
        std::vector<NClient::NApi::NProto::TResourceStatus_TAllocation> NewAllocations;
    };

    //! Converts protobuf-encoded requests into a local representation.
    std::vector<TRequest> BuildRequests(
        const NClient::NApi::NProto::TPodSpec_TResourceRequests& protoRequests);

    //! Attemps to make allocations.
    /*!
     *  Returns |true| on success; |false| on failure; in the latter case \param errors is populated
     *  with the errors.
     *
     *  On success, populates \param allocations. The elements of the latter match the elements of \param resources.
     *  For each resource, the corresponding item in \param allocations describes the allocations
     *  of this resource that are needed to satisfy the requests.
     */
    bool ComputeAllocations(
        const NObjects::TObjectId& podId,
        const std::vector<TRequest>& requests,
        const std::vector<TResource>& resources,
        std::vector<TAllocation>* allocations,
        std::vector<TError>* errors);
};

class TGlobalResourceAllocator
{
public:
    explicit TGlobalResourceAllocator(const TClusterPtr& cluster);

    TErrorOr<TNode*> ComputeAllocation(TPod* pod);

private:
    const TClusterPtr Cluster_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
