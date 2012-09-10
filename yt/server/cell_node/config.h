#pragma once

#include "public.h"

#include <server/exec_agent/config.h>

namespace NYT {
namespace NCellNode {

////////////////////////////////////////////////////////////////////////////////

struct TCellNodeConfig
    : public TYsonSerializable
{
    //! RPC interface port number.
    int RpcPort;

    //! HTTP monitoring interface port number.
    int MonitoringPort;

    //! Cell masters.
    NMetaState::TMasterDiscoveryConfigPtr Masters;

    //! Data node configuration part.
    NChunkHolder::TDataNodeConfigPtr DataNode;

    //! Exec node configuration part.
    NExecAgent::TExecAgentConfigPtr ExecAgent;

    i64 TotalMemorySize;

    TCellNodeConfig()
    {
        Register("rpc_port", RpcPort)
            .Default(9000);
        Register("monitoring_port", MonitoringPort)
            .Default(10000);
        Register("masters", Masters)
            .DefaultNew();
        Register("data_node", DataNode)
            .DefaultNew();
        Register("exec_agent", ExecAgent)
            .DefaultNew();

        // Very low default, override in production installation.
        Register("total_memory_size", TotalMemorySize)
            .GreaterThan(1024L * 1024 * 1024)
            .Default(5L * 1024 * 1024 * 1024);

        SetKeepOptions(true);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
