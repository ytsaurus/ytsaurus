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
        TObjectId id,
        NYT::NYson::TYsonString labels,
        TObjectId podSetId,
        TObjectId nodeId,
        TObjectId accountId,
        TObjectId uuid,
        NObjects::TPodResourceRequests resourceRequests,
        // NB! Pass some arguments by const reference due to lack of move semantics support.
        const NObjects::TPodDiskVolumeRequests& diskVolumeRequests,
        const NObjects::TPodIP6AddressRequests& ip6AddressRequests,
        const NObjects::TPodIP6SubnetRequests& ip6SubnetRequests,
        TString nodeFilter,
        NClient::NApi::NProto::TPodStatus_TEviction eviction);

    DEFINE_BYREF_RO_PROPERTY(TObjectId, PodSetId);
    DEFINE_BYREF_RO_PROPERTY(TObjectId, NodeId);
    DEFINE_BYREF_RO_PROPERTY(TObjectId, AccountId);

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

    DEFINE_BYVAL_RW_PROPERTY(TPodSet*, PodSet);
    DEFINE_BYVAL_RW_PROPERTY(TNode*, Node);
    DEFINE_BYVAL_RW_PROPERTY(TAccount*, Account);

    TAccount* GetEffectiveAccount() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
