#pragma once

#include "public.h"

#include <ytlib/actions/invoker.h>
// TODO(babenko): replace with public.h
#include <ytlib/bus/server.h>
// TODO(babenko): replace with public.h
#include <ytlib/rpc/channel.h>
// TODO(babenko): replace with public.h
#include <ytlib/transaction_client/transaction_manager.h>

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
    NRpc::IChannel::TPtr GetLeaderChannel() const;
    Stroka GetPeerAddress() const;
    IInvoker::TPtr GetControlInvoker() const;
    NTransactionClient::TTransactionManager::TPtr GetTransactionManager() const;

    void Run();

private:
    Stroka ConfigFileName;
    TCellSchedulerConfigPtr Config;

    IInvoker::TPtr ControlInvoker;
    NBus::IBusServer::TPtr BusServer;
    NRpc::IChannel::TPtr LeaderChannel;
    Stroka PeerAddress;
    NTransactionClient::TTransactionManager::TPtr TransactionManager;
    NTransactionClient::ITransaction::TPtr BootstrapTransaction;

    void Init();
    void Register();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellScheduler
} // namespace NYT
