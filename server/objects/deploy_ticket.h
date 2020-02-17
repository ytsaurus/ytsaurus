#pragma once

#include "object.h"

#include <yp/server/objects/proto/autogen.pb.h>

#include <yp/client/api/proto/data_model.pb.h>
#include <yp/client/api/proto/release_rule.pb.h>

#include <yt/core/misc/ref_tracked.h>
#include <yt/core/misc/property.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TDeployTicket
    : public TObject
    , public NYT::TRefTracked<TDeployTicket>
{
public:
    static constexpr EObjectType Type = EObjectType::DeployTicket;

    TDeployTicket(
        const TObjectId& id,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    class TSpec
    {
    public:
        explicit TSpec(TDeployTicket* deployTicket);

        static const TManyToOneAttributeSchema<TDeployTicket, TStage> StageSchema;
        using TStageAttribute = TManyToOneAttribute<TDeployTicket, TStage>;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStageAttribute, Stage);

        static const TManyToOneAttributeSchema<TDeployTicket, TRelease> ReleaseSchema;
        using TReleaseAttribute = TManyToOneAttribute<TDeployTicket, TRelease>;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TReleaseAttribute, Release);

        static const TManyToOneAttributeSchema<TDeployTicket, TReleaseRule> ReleaseRuleSchema;
        using TReleaseRuleAttribute = TManyToOneAttribute<TDeployTicket, TReleaseRule>;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TReleaseRuleAttribute, ReleaseRule);

        using TEtc = NProto::TDeployTicketSpecEtc;
        static const TScalarAttributeSchema<TDeployTicket, TEtc> EtcSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TEtc>, Etc);
    };
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    using TStatus = NYP::NClient::NApi::NProto::TDeployTicketStatus;
    static const TScalarAttributeSchema<TDeployTicket, TStatus> StatusSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TStatus>, Status);

    void UpdateTicketStatus(
        EDeployPatchActionType type,
        const TString& reason,
        const TString& message);

    void UpdatePatchStatus(
        const TObjectId& patchId,
        EDeployPatchActionType type,
        const TString& reason,
        const TString& message,
        TTimestamp startTimestamp);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
