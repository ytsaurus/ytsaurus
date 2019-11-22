#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

#include <util/datetime/base.h>

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

class TPodDisruptionBudget
    : public TObject
    , public NYT::TRefTracked<TPodDisruptionBudget>
{
public:
    static constexpr EObjectType Type = EObjectType::PodDisruptionBudget;

    TPodDisruptionBudget(
        TObjectId id,
        NYT::NYson::TYsonString labels,
        TObjectId uuid,
        ui64 creationTime,
        NClient::NApi::NProto::TPodDisruptionBudgetSpec spec,
        NClient::NApi::NProto::TPodDisruptionBudgetStatus status);

    DEFINE_BYREF_RO_PROPERTY(TObjectId, Uuid);
    DEFINE_BYREF_RO_PROPERTY(TInstant, CreationTime);

    DEFINE_BYREF_RO_PROPERTY(NClient::NApi::NProto::TPodDisruptionBudgetSpec, Spec);
    DEFINE_BYREF_RO_PROPERTY(NClient::NApi::NProto::TPodDisruptionBudgetStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
