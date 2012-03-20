#pragma once

#include "public.h"

#include <ytlib/actions/invoker.h>
// TODO(babenko): replace with public.h
#include <ytlib/bus/server.h>
// TODO(babenko): replace with public.h
#include <ytlib/rpc/channel.h>
// TODO(babenko): replace with public.h
#include <ytlib/scheduler/public.h>

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
    NRpc::IChannel::TPtr GetMasterChannel() const;
    Stroka GetPeerAddress() const;
    IInvoker::TPtr GetControlInvoker() const;
    NScheduler::TSchedulerPtr GetScheduler() const;

    void Run();

private:
    Stroka ConfigFileName;
    TCellSchedulerConfigPtr Config;

    IInvoker::TPtr ControlInvoker;
    NBus::IBusServer::TPtr BusServer;
    NRpc::IChannel::TPtr MasterChannel;
    Stroka PeerAddress;
    NScheduler::TSchedulerPtr Scheduler;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellScheduler
} // namespace NYT
