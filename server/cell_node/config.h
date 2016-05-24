#pragma once

#include "public.h"

#include <yt/server/exec_agent/config.h>

#include <yt/server/misc/config.h>

#include <yt/server/object_server/config.h>

#include <yt/server/query_agent/config.h>

#include <yt/server/tablet_node/config.h>

#include <yt/ytlib/api/config.h>

#include <yt/ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NCellNode {

////////////////////////////////////////////////////////////////////////////////

class TResourceLimitsConfig
    : public NYTree::TYsonSerializable
{
public:
    i64 Memory;

    TResourceLimitsConfig()
    {
        // Very low default, override for production use.
        RegisterParameter("memory", Memory)
            .GreaterThanOrEqual(0)
            .Default((i64) 5 * 1024 * 1024 * 1024);
    }
};

DEFINE_REFCOUNTED_TYPE(TResourceLimitsConfig)

class TCellNodeConfig
    : public TServerConfig
{
public:
    //! Interval between Orchid cache rebuilds.
    TDuration OrchidCacheUpdatePeriod;

    //! Node-to-master connection.
    NApi::TConnectionConfigPtr ClusterConnection;

    //! Data node configuration part.
    NDataNode::TDataNodeConfigPtr DataNode;

    //! Exec node configuration part.
    NExecAgent::TExecAgentConfigPtr ExecAgent;

    //! Tablet node configuration part.
    NTabletNode::TTabletNodeConfigPtr TabletNode;

    //! Query node configuration part.
    NQueryAgent::TQueryAgentConfigPtr QueryAgent;

    //! Metadata cache service configuration.
    NObjectServer::TMasterCacheServiceConfigPtr MasterCacheService;

    //! Jobs-to-master redirector.
    NRpc::TThrottlingChannelConfigPtr MasterRedirectorService;

    //! Known node addresses.
    NNodeTrackerClient::TAddressMap Addresses;

    //! Limits for the node process and all jobs controlled by it.
    TResourceLimitsConfigPtr ResourceLimits;

    TCellNodeConfig()
    {
        RegisterParameter("orchid_cache_update_period", OrchidCacheUpdatePeriod)
            .Default(TDuration::Seconds(5));
        RegisterParameter("cluster_connection", ClusterConnection);
        RegisterParameter("data_node", DataNode)
            .DefaultNew();
        RegisterParameter("exec_agent", ExecAgent)
            .DefaultNew();
        RegisterParameter("tablet_node", TabletNode)
            .DefaultNew();
        RegisterParameter("query_agent", QueryAgent)
            .DefaultNew();
        RegisterParameter("master_cache_service", MasterCacheService)
            .DefaultNew();
        RegisterParameter("master_redirector_service", MasterRedirectorService)
            .DefaultNew();
        RegisterParameter("addresses", Addresses)
            .Default();
        RegisterParameter("resource_limits", ResourceLimits)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TCellNodeConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
