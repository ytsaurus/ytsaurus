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

class TRequestTrackerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    bool Enabled;
    i64 DefaultUserRequestWeightRateLimit;
    int DefaultUserRequestQueueSizeLimit;
    size_t ReconfigureBatchSize;

    TRequestTrackerConfig()
    {
        RegisterParameter("enabled", Enabled)
            .Default(false);
        RegisterParameter("default_user_request_weight_rate_limit", DefaultUserRequestWeightRateLimit)
            .GreaterThanOrEqual(0)
            .LessThanOrEqual(100000)
            .Default(0);
        RegisterParameter("default_user_request_queue_size_limit", DefaultUserRequestQueueSizeLimit)
            .GreaterThanOrEqual(0)
            .LessThanOrEqual(100000)
            .Default(0);
        RegisterParameter("reconfigure_batch_size", ReconfigureBatchSize)
            .GreaterThan(0)
            .LessThan(100000)
            .Default(500);
    }
};

DEFINE_REFCOUNTED_TYPE(TRequestTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

class TAccessControlManagerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    TDuration ClusterStateUpdatePeriod;
    std::vector<NObjects::EObjectType> ClusterStateAllowedObjectTypes;
    TRequestTrackerConfigPtr RequestTracker;

    TAccessControlManagerConfig()
    {
        RegisterParameter("cluster_state_update_period", ClusterStateUpdatePeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("cluster_state_allowed_object_types", ClusterStateAllowedObjectTypes)
            .Default({
                NObjects::EObjectType::Account,
                NObjects::EObjectType::DaemonSet,
                NObjects::EObjectType::DynamicResource,
                NObjects::EObjectType::EndpointSet,
                NObjects::EObjectType::HorizontalPodAutoscaler,
                NObjects::EObjectType::IP4AddressPool,
                NObjects::EObjectType::MultiClusterReplicaSet,
                NObjects::EObjectType::NetworkProject,
                NObjects::EObjectType::PodSet,
                NObjects::EObjectType::Project,
                NObjects::EObjectType::ReleaseRule,
                NObjects::EObjectType::ReplicaSet,
                NObjects::EObjectType::ResourceCache,
                NObjects::EObjectType::Stage,
            });
        RegisterParameter("request_tracker", RequestTracker)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TAccessControlManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NNodes::NAccessControl
