#pragma once

#include "object.h"

#include <yp/server/objects/proto/autogen.pb.h>

#include <yt/core/misc/ref_tracked.h>
#include <yt/core/misc/property.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TRelease
    : public TObject
    , public NYT::TRefTracked<TRelease>
{
public:
    static constexpr EObjectType Type = EObjectType::Release;

    TRelease(
        const TObjectId& id,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    static const TOneToManyAttributeSchema<TRelease, TDeployTicket> DeployTicketsSchema;
    using TDeployTickets = TOneToManyAttribute<TRelease, TDeployTicket>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TDeployTickets, DeployTickets);

    class TSpec
    {
    public:
        explicit TSpec(TRelease* release);

        using TEtc = NProto::TReleaseSpecEtc;
        static const TScalarAttributeSchema<TRelease, TEtc> EtcSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TEtc>, Etc);
    };
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    using TStatus = NYP::NClient::NApi::NProto::TReleaseStatus;
    static const TScalarAttributeSchema<TRelease, TStatus> StatusSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TStatus>, Status);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
