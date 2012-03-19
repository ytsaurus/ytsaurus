#pragma once

#include "public.h"

#include <ytlib/chunk_holder/public.h>
#include <ytlib/election/leader_lookup.h>

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

    //! Chunk Holder's configuration part.
    NChunkHolder::TChunkHolderConfigPtr ChunkHolder;

    // TODO(babenko): add ExecAgent's part

    TCellNodeConfig()
    {
        Register("rpc_port", RpcPort)
            .Default(9000);
        Register("monitoring_port", MonitoringPort)
            .Default(10000);
        Register("masters", Masters);
        Register("chunk_holder", ChunkHolder)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
