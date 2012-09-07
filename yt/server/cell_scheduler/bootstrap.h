#pragma once

#include "public.h"

#include <ytlib/actions/action_queue.h>
#include <ytlib/bus/public.h>
#include <ytlib/rpc/public.h>
#include <ytlib/transaction_client/public.h>

#include <server/scheduler/public.h>

namespace NYT {
namespace NCellScheduler {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EControlQueue,
    (Default)
    (Heartbeat)
);

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
    IInvokerPtr GetControlInvoker(EControlQueue queue = EControlQueue::Default) const;
    NTransactionClient::TTransactionManagerPtr GetTransactionManager() const;
    NScheduler::TSchedulerPtr GetScheduler() const;

    void Run();

private:
    Stroka ConfigFileName;
    TCellSchedulerConfigPtr Config;

    TFairShareActionQueuePtr ControlQueue;
    NBus::IBusServerPtr BusServer;
    NRpc::IChannelPtr MasterChannel;
    Stroka PeerAddress;
    NTransactionClient::TTransactionManagerPtr TransactionManager;
    NScheduler::TSchedulerPtr Scheduler;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellScheduler
} // namespace NYT
