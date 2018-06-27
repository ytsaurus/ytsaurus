#pragma once

#include "public.h"

#include <yp/server/objects/config.h>

#include <yp/server/net/config.h>

#include <yp/server/nodes/config.h>

#include <yp/server/scheduler/config.h>

#include <yt/ytlib/program/config.h>

#include <yt/ytlib/api/config.h>

#include <yt/ytlib/auth/config.h>

#include <yt/core/http/config.h>

#include <yt/core/https/config.h>

#include <yt/core/rpc/grpc/config.h>

#include <yt/core/ypath/public.h>

namespace NYP {
namespace NServer {
namespace NAccessControl {

////////////////////////////////////////////////////////////////////////////////

class TAccessControlManagerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    TDuration ClusterStateUpdatePeriod;

    TAccessControlManagerConfig()
    {
        RegisterParameter("cluster_state_update_period", ClusterStateUpdatePeriod)
            .Default(TDuration::Seconds(1));
    }
};

DEFINE_REFCOUNTED_TYPE(TAccessControlManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NAccessControl
} // namespace NNodes
} // namespace NYP
