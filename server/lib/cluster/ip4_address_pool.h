#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

class TIP4AddressPool
    : public TObject
    , public NYT::TRefTracked<TIP4AddressPool>
{
public:
    static constexpr EObjectType Type = EObjectType::IP4AddressPool;

    TIP4AddressPool(
        TObjectId id,
        NYT::NYson::TYsonString labels,
        NClient::NApi::NProto::TIP4AddressPoolSpec spec,
        NClient::NApi::NProto::TIP4AddressPoolStatus status);

    DEFINE_BYREF_RO_PROPERTY(NClient::NApi::NProto::TIP4AddressPoolSpec, Spec);
    DEFINE_BYREF_RO_PROPERTY(NClient::NApi::NProto::TIP4AddressPoolStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
