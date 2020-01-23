#pragma once

#include "object.h"

#include <yp/server/objects/proto/autogen.pb.h>

#include <yp/server/nodes/public.h>

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/ref_tracked.h>

#include <contrib/libs/protobuf/google/protobuf/any.pb.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TNode
    : public TObject
    , public TRefTracked<TNode>
{
public:
    static constexpr EObjectType Type = EObjectType::Node;

    TNode(
        const TObjectId& id,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    using TResourcesAttribute = TChildrenAttribute<TResource>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TResourcesAttribute, Resources);
    TResource* GetCpuResourceOrThrow();

    class TStatus
    {
    public:
        explicit TStatus(TNode* node);

        static const TScalarAttributeSchema<TNode, TString> AgentAddressSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TString>, AgentAddress);

        static const TScalarAttributeSchema<TNode, NNodes::TEpochId> EpochIdSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<NNodes::TEpochId>, EpochId);

        static const TScalarAttributeSchema<TNode, TInstant> LastSeenTimeSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TInstant>, LastSeenTime);

        static const TScalarAttributeSchema<TNode, ui64> HeartbeatSequenceNumberSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<ui64>, HeartbeatSequenceNumber);

        using THostManager = google::protobuf::Any;
        static const TScalarAttributeSchema<TNode, THostManager> HostManagerSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<THostManager>, HostManager);

        using TEtc = NProto::TNodeStatusEtc;
        static const TScalarAttributeSchema<TNode, TEtc> EtcSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);

    using TSpec = NClient::NApi::NProto::TNodeSpec;
    static const TScalarAttributeSchema<TNode, TSpec> SpecSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TSpec>, Spec);

    static const TOneToManyAttributeSchema<TNode, TPod> PodsSchema;
    using TPods = TOneToManyAttribute<TNode, TPod>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPods, Pods);

    // Hfsm.
    void UpdateHfsmStatus(
        EHfsmState state,
        const TString& message,
        std::optional<NClient::NApi::NProto::TMaintenanceInfo> maintenanceInfo);

    // Maintenance.
    void UpdateMaintenanceStatus(
        ENodeMaintenanceState state,
        const TString& message,
        TGenericUpdate<NClient::NApi::NProto::TMaintenanceInfo> infoUpdate);

    // Alerts.
    void RemoveAlert(
        const TObjectId& uuid);
    void AddAlert(
        const TString& type,
        const TString& description);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
