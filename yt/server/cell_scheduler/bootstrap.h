#pragma once

#include "public.h"

#include <core/concurrency/action_queue.h>
#include <core/bus/public.h>
#include <core/rpc/public.h>
#include <ytlib/cell_directory/public.h>
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
    const Stroka& GetLocalAddress() const;
    IInvokerPtr GetControlInvoker(EControlQueue queue = EControlQueue::Default) const;
    NTransactionClient::TTransactionManagerPtr GetTransactionManager() const;
    NScheduler::TSchedulerPtr GetScheduler() const;
    NCellDirectory::TCellDirectoryPtr GetCellDirectory() const;
    NConcurrency::IThroughputThrottlerPtr GetChunkLocationThrottler() const;

    void Run();

private:
    Stroka ConfigFileName;
    TCellSchedulerConfigPtr Config;

    NConcurrency::TFairShareActionQueuePtr ControlQueue;
    NBus::IBusServerPtr BusServer;
    NRpc::IChannelPtr MasterChannel;
    Stroka LocalAddress;
    NTransactionClient::TTransactionManagerPtr TransactionManager;
    NScheduler::TSchedulerPtr Scheduler;
    NCellDirectory::TCellDirectoryPtr CellDirectory;
    NConcurrency::IThroughputThrottlerPtr ChunkLocationThrottler;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellScheduler
} // namespace NYT
