#pragma once

#include "public.h"

#include <ytlib/election/leader_lookup.h>
#include <ytlib/transaction_client/config.h>

namespace NYT {
namespace NCellScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TCellSchedulerConfig
    : public TYsonSerializable
{
    //! RPC interface port number.
    int RpcPort;

    //! HTTP monitoring interface port number.
    int MonitoringPort;

    NElection::TLeaderLookup::TConfigPtr Masters;
    NTransactionClient::TTransactionManagerConfigPtr TransactionManager;
    NScheduler::TSchedulerConfigPtr Scheduler;

    TCellSchedulerConfig()
    {
        Register("rpc_port", RpcPort)
            .Default(9001);
        Register("monitoring_port", MonitoringPort)
            .Default(10001);
        Register("masters", Masters).
            DefaultNew();
        Register("transaction_manager", TransactionManager)
            .DefaultNew();
        Register("scheduler", Scheduler)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellScheduler
} // namespace NYT
