#pragma once

#include "public.h"

#include <ytlib/api/config.h>

#include <server/misc/config.h>

#include <server/exec_agent/config.h>

#include <server/tablet_node/config.h>

#include <server/query_agent/config.h>

#include <server/object_server/config.h>

namespace NYT {
namespace NCellNode {

////////////////////////////////////////////////////////////////////////////////

class TCellNodeConfig
    : public TServerConfig
{
public:
    //! Orchid cache expiration timeout.
    TDuration OrchidCacheExpirationTime;

    //! RPC interface port number.
    int RpcPort;

    //! HTTP monitoring interface port number.
    int MonitoringPort;

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
    NObjectServer::TMasterCacheServiceConfigPtr MasterCache;


    yhash_map<Stroka, Stroka> Addresses;

    TCellNodeConfig()
    {
        RegisterParameter("orchid_cache_expiration_time", OrchidCacheExpirationTime)
            .Default(TDuration::Seconds(5));
        RegisterParameter("rpc_port", RpcPort)
            .Default(9000);
        RegisterParameter("addresses", Addresses)
            .Default();
        RegisterParameter("monitoring_port", MonitoringPort)
            .Default(10000);
        RegisterParameter("cluster_connection", ClusterConnection)
            .DefaultNew();
        RegisterParameter("data_node", DataNode)
            .DefaultNew();
        RegisterParameter("exec_agent", ExecAgent)
            .DefaultNew();
        RegisterParameter("tablet_node", TabletNode)
            .DefaultNew();
        RegisterParameter("query_agent", QueryAgent)
            .DefaultNew();
        RegisterParameter("caching_object_service", MasterCache)
            .DefaultNew();

        SetKeepOptions(true);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
