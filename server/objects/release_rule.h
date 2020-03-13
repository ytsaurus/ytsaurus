#pragma once

#include "object.h"

#include <yp/server/objects/proto/autogen.pb.h>

#include <yp/client/api/proto/data_model.pb.h>
#include <yp/client/api/proto/release_rule.pb.h>

#include <yt/core/misc/ref_tracked.h>
#include <yt/core/misc/property.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TReleaseRule
    : public TObject
    , public NYT::TRefTracked<TReleaseRule>
{
public:
    static constexpr EObjectType Type = EObjectType::ReleaseRule;

    TReleaseRule(
        const TObjectId& id,
        const TObjectId& stageId,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    using TStageAttribute = TParentAttribute<TStage>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStageAttribute, Stage);

    using TDeployTicketsAttribute = TOneToManyAttribute<TReleaseRule, TDeployTicket>;
    static const TDeployTicketsAttribute::TSchema DeployTicketsSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TDeployTicketsAttribute, DeployTickets);

    class TSpec
    {
    public:
        explicit TSpec(TReleaseRule* releaseRule);

        using TEtc = NProto::TReleaseRuleSpecEtc;
        static const TScalarAttributeSchema<TReleaseRule, TEtc> EtcSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TEtc>, Etc);
    };
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    using TStatus = NYP::NClient::NApi::NProto::TReleaseRuleStatus;
    static const TScalarAttributeSchema<TReleaseRule, TStatus> StatusSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TStatus>, Status);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
