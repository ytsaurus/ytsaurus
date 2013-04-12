#pragma once

#include "public.h"

#include <ytlib/meta_state/public.h>

#include <ytlib/transaction_client/config.h>

#include <ytlib/rpc/retrying_channel.h>

#include <server/misc/config.h>

#include <server/scheduler/config.h>

namespace NYT {
namespace NCellScheduler {

////////////////////////////////////////////////////////////////////////////////

class TCellSchedulerConfig
    : public TServerConfig
{
public:
    //! RPC interface port number.
    int RpcPort;

    //! HTTP monitoring interface port number.
    int MonitoringPort;

    NMetaState::TMasterDiscoveryConfigPtr Masters;
    NTransactionClient::TTransactionManagerConfigPtr TransactionManager;
    NScheduler::TSchedulerConfigPtr Scheduler;

    TCellSchedulerConfig()
    {
        RegisterParameter("rpc_port", RpcPort)
            .Default(9001);
        RegisterParameter("monitoring_port", MonitoringPort)
            .Default(10001);
        RegisterParameter("masters", Masters).
            DefaultNew();
        RegisterParameter("transaction_manager", TransactionManager)
            .DefaultNew();
        RegisterParameter("scheduler", Scheduler)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellScheduler
} // namespace NYT
