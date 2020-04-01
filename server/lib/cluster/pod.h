#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/proto/error.pb.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

class TPodIP6AddressRequests
{
public:
    TPodIP6AddressRequests() = default;
    explicit TPodIP6AddressRequests(
        const NObjects::TPodIP6AddressRequests& ip6AddressRequests);

    DEFINE_BYREF_RO_PROPERTY(NObjects::TPodIP6AddressRequests, ProtoRequests);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TIP4AddressPool*>, IP4AddressPools);

    size_t GetSize() const;
    void SetPool(size_t pos, TIP4AddressPool* pool);
};

////////////////////////////////////////////////////////////////////////////////

class TPod
    : public TObject
    , public NYT::TRefTracked<TPod>
{
public:
    static constexpr EObjectType Type = EObjectType::Pod;

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
        const NObjects::TPodGpuRequests& gpuRequests,
        const NObjects::TPodIP6AddressRequests& ip6AddressRequests,
        const NObjects::TPodIP6SubnetRequests& ip6SubnetRequests,
        TString nodeFilter,
        const NObjects::TSchedulingHints& schedulingHints,
        bool enableScheduling,
        NClient::NApi::NProto::TPodStatus_TEviction eviction,
        NYT::NProto::TError schedulingError,
        const NObjects::TNodeAlerts& nodeAlerts,
        NClient::NApi::NProto::TPodStatus_TMaintenance maintenance);

    DEFINE_BYREF_RO_PROPERTY(TObjectId, PodSetId);
    DEFINE_BYREF_RO_PROPERTY(TObjectId, NodeId);
    DEFINE_BYREF_RO_PROPERTY(TObjectId, AccountId);

    // Meta.
    DEFINE_BYREF_RO_PROPERTY(TObjectId, Uuid);

    // Spec.
    DEFINE_BYREF_RO_PROPERTY(NObjects::TPodResourceRequests, ResourceRequests);
    DEFINE_BYREF_RO_PROPERTY(NObjects::TPodDiskVolumeRequests, DiskVolumeRequests);
    DEFINE_BYREF_RO_PROPERTY(NObjects::TPodGpuRequests, GpuRequests);
    DEFINE_BYREF_RW_PROPERTY(TPodIP6AddressRequests, IP6AddressRequests);
    DEFINE_BYREF_RO_PROPERTY(NObjects::TPodIP6SubnetRequests, IP6SubnetRequests);
    DEFINE_BYREF_RO_PROPERTY(TString, NodeFilter);
    DEFINE_BYREF_RO_PROPERTY(NObjects::TSchedulingHints, SchedulingHints);
    DEFINE_BYVAL_RO_PROPERTY(bool, EnableScheduling);

    // Status.
    DEFINE_BYREF_RO_PROPERTY(NClient::NApi::NProto::TPodStatus_TEviction, Eviction);
    DEFINE_BYREF_RO_PROPERTY(NYT::NProto::TError, SchedulingError);
    DEFINE_BYREF_RO_PROPERTY(NObjects::TNodeAlerts, NodeAlerts);
    DEFINE_BYREF_RO_PROPERTY(NClient::NApi::NProto::TPodStatus_TMaintenance, Maintenance);

    DEFINE_BYVAL_RW_PROPERTY(TPodSet*, PodSet);
    DEFINE_BYVAL_RW_PROPERTY(TNode*, Node);
    DEFINE_BYVAL_RW_PROPERTY(TAccount*, Account);

    TAccount* GetEffectiveAccount() const;
    const TString& GetEffectiveNodeFilter() const;

    TError ParseSchedulingError() const;

    ui64 GetInternetAddressRequestCount() const;
    ui64 GetDiskRequestTotalCapacity(const TString& storageClass) const;

    //! It is assumed to be called exactly once during cluster snapshot creation.
    void PostprocessAttributes();

    TError GetSchedulingAttributesValidationError() const;

    //! Returns a null object id if the attribute is missing.
    //! Returns a validation error if any occurred.
    TErrorOr<TString> GetAntiaffinityGroupId(const NYPath::TYPath& groupIdAttributePath) const;

private:
    using TAntiaffinityGroupIdByAttributePath = THashMap<NYPath::TYPath, TString>;

    TErrorOr<TAntiaffinityGroupIdByAttributePath> AntiaffinityGroupIdsOrError_;


    TErrorOr<TAntiaffinityGroupIdByAttributePath> PostprocessAntiaffinityGroupIds(
        const NYTree::IMapNodePtr& labelsMap) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
