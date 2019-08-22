#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TIP4AddressPool
    : public TObject
    , public NYT::TRefTracked<TIP4AddressPool>
{
public:
    TIP4AddressPool(
        TObjectId id,
        NYT::NYson::TYsonString labels,
        NClient::NApi::NProto::TIP4AddressPoolSpec spec,
        NClient::NApi::NProto::TIP4AddressPoolStatus status);

    DEFINE_BYREF_RO_PROPERTY(NClient::NApi::NProto::TIP4AddressPoolSpec, Spec);
    DEFINE_BYREF_RO_PROPERTY(NClient::NApi::NProto::TIP4AddressPoolStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
