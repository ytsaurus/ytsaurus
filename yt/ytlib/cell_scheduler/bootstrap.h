#pragma once

#include "public.h"

#include <ytlib/actions/invoker.h>
#include <ytlib/bus/public.h>
#include <ytlib/rpc/public.h>
#include <ytlib/scheduler/public.h>
#include <ytlib/transaction_client/public.h>

namespace NYT {
namespace NCellScheduler {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(
        const Stroka& configFileName,
        TCellSchedulerConfigPtr config);
    ~TBootstrap();

    TCellSchedulerConfigPtr GetConfig() const;
    NRpc::IChannelPtr GetMasterChannel() const;
    Stroka GetPeerAddress() const;
    IInvokerPtr GetControlInvoker() const;
    NTransactionClient::TTransactionManagerPtr GetTransactionManager() const;
    NScheduler::TSchedulerPtr GetScheduler() const;

    void Run();

private:
    Stroka ConfigFileName;
    TCellSchedulerConfigPtr Config;

    IInvokerPtr ControlInvoker;
    NBus::IBusServerPtr BusServer;
    NRpc::IChannelPtr MasterChannel;
    Stroka PeerAddress;
    NTransactionClient::TTransactionManagerPtr TransactionManager;
    NScheduler::TSchedulerPtr Scheduler;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellScheduler
} // namespace NYT
