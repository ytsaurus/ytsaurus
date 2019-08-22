#include "resource.h"

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TResource::TResource(
    TObjectId id,
    NYT::NYson::TYsonString labels,
    TObjectId nodeId,
    EResourceKind kind,
    NClient::NApi::NProto::TResourceSpec spec,
    std::vector<NClient::NApi::NProto::TResourceStatus_TAllocation> scheduledAllocations,
    std::vector<NClient::NApi::NProto::TResourceStatus_TAllocation> actualAllocations)
    : TObject(std::move(id), std::move(labels))
    , NodeId_(std::move(nodeId))
    , Kind_(kind)
    , Spec_(std::move(spec))
    , ScheduledAllocations_(std::move(scheduledAllocations))
    , ActualAllocations_(std::move(actualAllocations))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
