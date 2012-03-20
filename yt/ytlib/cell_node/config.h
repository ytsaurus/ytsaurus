#pragma once

#include "public.h"

#include <ytlib/chunk_holder/public.h>
#include <ytlib/election/leader_lookup.h>
#include <ytlib/exec_agent/config.h>

namespace NYT {
namespace NCellNode {

////////////////////////////////////////////////////////////////////////////////

struct TCellNodeConfig
    : public TConfigurable
{
    //! RPC interface port number.
    int RpcPort;

    //! HTTP monitoring interface port number.
    int MonitoringPort;

    //! Cell masters.
    NElection::TLeaderLookup::TConfig::TPtr Masters;

    //! Data node configuration part.
    NChunkHolder::TChunkHolderConfigPtr Data;

    //! Exec node configuration part.
    NExecAgent::TExecAgentConfigPtr Exec;

    TCellNodeConfig()
    {
        Register("rpc_port", RpcPort)
            .Default(9000);
        Register("monitoring_port", MonitoringPort)
            .Default(10000);
        Register("masters", Masters);
        Register("data", Data);
        Register("exec", Exec);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
