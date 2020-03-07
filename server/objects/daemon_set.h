#pragma once

#include "object.h"

#include <yp/server/objects/proto/autogen.pb.h>

#include <yp/client/api/proto/data_model.pb.h>
#include <yp/client/api/proto/daemon_set.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TDaemonSet
    : public TObject
    , public NYT::TRefTracked<TDaemonSet>
{
public:
    static constexpr EObjectType Type = EObjectType::DaemonSet;

    TDaemonSet(
        const TObjectId& id,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    static const TScalarAttributeSchema<TDaemonSet, TObjectId> PodSetIdSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TObjectId>, PodSetId);

    using TSpec = NYP::NClient::NApi::NProto::TDaemonSetSpec;
    static const TScalarAttributeSchema<TDaemonSet, TSpec> SpecSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TSpec>, Spec);

    using TStatus = NYP::NClient::NApi::NProto::TDaemonSetStatus;
    static const TScalarAttributeSchema<TDaemonSet, TStatus> StatusSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TStatus>, Status);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
