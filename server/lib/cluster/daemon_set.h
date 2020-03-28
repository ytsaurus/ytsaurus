#pragma once

#include "object.h"

#include <yp/client/api/proto/daemon_set.pb.h>
#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

class TDaemonSet
    : public TObject
    , public NYT::TRefTracked<TDaemonSet>
{
public:
    static constexpr EObjectType Type = EObjectType::DaemonSet;

    TDaemonSet(
        TObjectId id,
        NYT::NYson::TYsonString labels,
        TObjectId podSetId,
        NClient::NApi::NProto::TDaemonSetSpec spec);

    DEFINE_BYREF_RO_PROPERTY(TObjectId, PodSetId);
    DEFINE_BYREF_RO_PROPERTY(NClient::NApi::NProto::TDaemonSetSpec, Spec);

    DEFINE_BYVAL_RW_PROPERTY(TPodSet*, PodSet);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
