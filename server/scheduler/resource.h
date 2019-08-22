#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TResource
    : public TObject
    , public NYT::TRefTracked<TResource>
{
public:
    TResource(
        TObjectId id,
        NYT::NYson::TYsonString labels,
        TObjectId nodeId,
        EResourceKind kind,
        NClient::NApi::NProto::TResourceSpec spec,
        std::vector<NClient::NApi::NProto::TResourceStatus_TAllocation> scheduledAllocations,
        std::vector<NClient::NApi::NProto::TResourceStatus_TAllocation> actualAllocations);

    DEFINE_BYREF_RO_PROPERTY(TObjectId, NodeId);

    // Meta.
    DEFINE_BYVAL_RO_PROPERTY(EResourceKind, Kind);

    // Spec.
    DEFINE_BYREF_RO_PROPERTY(NClient::NApi::NProto::TResourceSpec, Spec);

    // Status.
    DEFINE_BYREF_RO_PROPERTY(std::vector<NClient::NApi::NProto::TResourceStatus_TAllocation>, ScheduledAllocations);
    DEFINE_BYREF_RO_PROPERTY(std::vector<NClient::NApi::NProto::TResourceStatus_TAllocation>, ActualAllocations);

    DEFINE_BYVAL_RW_PROPERTY(TNode*, Node);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
