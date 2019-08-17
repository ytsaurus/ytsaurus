#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TPod
    : public TObject
    , public NYT::TRefTracked<TPod>
{
public:
    TPod(
        const TObjectId& id,
        NYT::NYson::TYsonString labels,
        TPodSet* podSet,
        TNode* node,
        TAccount* account,
        TObjectId uuid,
        NObjects::TPodResourceRequests resourceRequests,
        // NB! Pass some arguments by const reference due to lack of move semantics support.
        const NObjects::TPodDiskVolumeRequests& diskVolumeRequests,
        const NObjects::TPodIP6AddressRequests& ip6AddressRequests,
        const NObjects::TPodIP6SubnetRequests& ip6SubnetRequests,
        TString nodeFilter,
        NClient::NApi::NProto::TPodStatus_TEviction eviction);

    DEFINE_BYVAL_RO_PROPERTY(TPodSet*, PodSet);
    DEFINE_BYVAL_RO_PROPERTY(TNode*, Node);
    DEFINE_BYVAL_RO_PROPERTY(TAccount*, Account);

    // Meta.
    DEFINE_BYREF_RO_PROPERTY(TObjectId, Uuid);

    // Spec.
    DEFINE_BYREF_RO_PROPERTY(NObjects::TPodResourceRequests, ResourceRequests);
    DEFINE_BYREF_RO_PROPERTY(NObjects::TPodDiskVolumeRequests, DiskVolumeRequests);
    DEFINE_BYREF_RO_PROPERTY(NObjects::TPodIP6AddressRequests, IP6AddressRequests);
    DEFINE_BYREF_RO_PROPERTY(NObjects::TPodIP6SubnetRequests, IP6SubnetRequests);
    DEFINE_BYREF_RO_PROPERTY(TString, NodeFilter);

    // Status.
    DEFINE_BYREF_RO_PROPERTY(NClient::NApi::NProto::TPodStatus_TEviction, Eviction);

    TAccount* GetEffectiveAccount() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
