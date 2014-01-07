#pragma once

#include "public.h"

#include <core/rpc/config.h>

#include <ytlib/hive/config.h>

#include <ytlib/transaction_client/config.h>

#include <server/misc/config.h>

#include <server/exec_agent/config.h>

#include <server/tablet_node/config.h>

#include <server/query_agent/config.h>

namespace NYT {
namespace NCellNode {

////////////////////////////////////////////////////////////////////////////////

class TCellNodeConfig
    : public TServerConfig
{
public:
    //! RPC interface port number.
    int RpcPort;

    //! HTTP monitoring interface port number.
    int MonitoringPort;

    //! Cell masters.
    NHydra::TPeerDiscoveryConfigPtr Masters;

    //! Cell directory.
    NHive::TCellDirectoryConfigPtr CellDirectory;

    //! Data node configuration part.
    NDataNode::TDataNodeConfigPtr DataNode;

    //! Exec node configuration part.
    NExecAgent::TExecAgentConfigPtr ExecAgent;

    //! Tablet node configuration part.
    NTabletNode::TTabletNodeConfigPtr TabletNode;

    //! Query node configuration part.
    NQueryAgent::TQueryAgentConfigPtr QueryAgent;

    //! Throttling configuration for jobs-to-master communication.
    NRpc::TThrottlingChannelConfigPtr JobsToMasterChannel;

    //! Timestamp provider configuration for transaction coordination.
    NHive::TRemoteTimestampProviderConfigPtr TimestampProvider;

    //! Transaction manager configuration used by tablets for e.g. flushing stores.
    NTransactionClient::TTransactionManagerConfigPtr TransactionManager;

    TCellNodeConfig()
    {
        RegisterParameter("rpc_port", RpcPort)
            .Default(9000);
        RegisterParameter("monitoring_port", MonitoringPort)
            .Default(10000);
        RegisterParameter("masters", Masters);
        RegisterParameter("cell_directory", CellDirectory)
            .DefaultNew();
        RegisterParameter("data_node", DataNode);
        RegisterParameter("exec_agent", ExecAgent);
        RegisterParameter("tablet_node", TabletNode);
        RegisterParameter("query_agent", QueryAgent);
        RegisterParameter("jobs_to_master_channel", JobsToMasterChannel)
            .DefaultNew();
        RegisterParameter("timestamp_provider", TimestampProvider);
        RegisterParameter("transaction_manager", TransactionManager)
            .DefaultNew();

        SetKeepOptions(true);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
