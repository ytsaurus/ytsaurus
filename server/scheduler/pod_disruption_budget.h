#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TPodDisruptionBudget
    : public TObject
    , public NYT::TRefTracked<TPodDisruptionBudget>
{
public:
    TPodDisruptionBudget(
        const TObjectId& id,
        NYT::NYson::TYsonString labels,
        TObjectId uuid,
        NClient::NApi::NProto::TPodDisruptionBudgetSpec spec);

    DEFINE_BYREF_RO_PROPERTY(TObjectId, Uuid);
    DEFINE_BYREF_RO_PROPERTY(NClient::NApi::NProto::TPodDisruptionBudgetSpec, Spec);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
