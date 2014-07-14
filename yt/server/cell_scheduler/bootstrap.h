#pragma once

#include "public.h"

#include <core/concurrency/action_queue.h>

#include <core/bus/public.h>

#include <core/rpc/public.h>

#include <ytlib/api/public.h>

#include <ytlib/hive/public.h>

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
    NApi::IClientPtr GetMasterClient() const;
    const Stroka& GetLocalAddress() const;
    IInvokerPtr GetControlInvoker(EControlQueue queue = EControlQueue::Default) const;
    NScheduler::TSchedulerPtr GetScheduler() const;
    NHive::TClusterDirectoryPtr GetClusterDirectory() const;

    void Run();

private:
    Stroka ConfigFileName;
    TCellSchedulerConfigPtr Config;

    NConcurrency::TFairShareActionQueuePtr ControlQueue;
    NBus::IBusServerPtr BusServer;
    NApi::IClientPtr MasterClient;
    Stroka LocalAddress;
    NScheduler::TSchedulerPtr Scheduler;
    NHive::TClusterDirectoryPtr ClusterDirectory;

    void DoRun();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellScheduler
} // namespace NYT
