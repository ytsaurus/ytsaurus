#pragma once

#include "public.h"

#include <yp/server/objects/config.h>

#include <yp/server/net/config.h>

#include <yp/server/nodes/config.h>

#include <yp/server/scheduler/config.h>

#include <yt/ytlib/program/config.h>

#include <yt/client/api/config.h>

#include <yt/ytlib/auth/config.h>

#include <yt/core/http/config.h>

#include <yt/core/https/config.h>

#include <yt/core/rpc/grpc/config.h>

#include <yt/core/ypath/public.h>

namespace NYP::NServer::NAccessControl {

////////////////////////////////////////////////////////////////////////////////

class TAccessControlManagerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    TDuration ClusterStateUpdatePeriod;
    std::vector<NObjects::EObjectType> ClusterStateAllowedObjectTypes;

    TAccessControlManagerConfig()
    {
        RegisterParameter("cluster_state_update_period", ClusterStateUpdatePeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("cluster_state_allowed_object_types", ClusterStateAllowedObjectTypes)
            .Default({
                NObjects::EObjectType::NetworkProject,
                NObjects::EObjectType::Account,
                NObjects::EObjectType::EndpointSet,
                NObjects::EObjectType::MultiClusterReplicaSet,
                NObjects::EObjectType::ReplicaSet,
                NObjects::EObjectType::PodSet,
                NObjects::EObjectType::Stage,
                NObjects::EObjectType::DynamicResource});
    }
};

DEFINE_REFCOUNTED_TYPE(TAccessControlManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NNodes::NAccessControl
