#pragma once

#include "object.h"

#include <yp/server/objects/proto/objects.pb.h>

#include <yp/server/nodes/public.h>

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP {
namespace NServer {
namespace NObjects {

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

        using TOther = NProto::TNodeStatusOther;
        static const TScalarAttributeSchema<TNode, TOther> OtherSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TOther>, Other);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);

    using TSpec = NClient::NApi::NProto::TNodeSpec;
    static const TScalarAttributeSchema<TNode, TSpec> SpecSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TSpec>, Spec);

    static const TOneToManyAttributeSchema<TNode, TPod> PodsSchema;
    using TPods = TOneToManyAttribute<TNode, TPod>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPods, Pods);

    void UpdateHfsmStatus(
        EHfsmState state,
        const TString& message);
    void UpdateMaintenanceStatus(
        ENodeMaintenanceState state,
        const TString& message);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
