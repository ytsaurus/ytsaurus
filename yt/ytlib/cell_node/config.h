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
    NChunkHolder::TChunkHolderConfigPtr ChunkHolder;

    //! Exec node configuration part.
    NExecAgent::TExecAgentConfigPtr ExecAgent;

    TCellNodeConfig()
    {
        Register("rpc_port", RpcPort)
            .Default(9000);
        Register("monitoring_port", MonitoringPort)
            .Default(10000);
        Register("masters", Masters);
        Register("chunk_holder", ChunkHolder);
        Register("exec_agent", ExecAgent);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
