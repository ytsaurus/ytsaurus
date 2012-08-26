#pragma once

#include "public.h"

#include <server/transaction_server/public.h>
#include <server/cypress_server/public.h>
#include <ytlib/actions/action_queue.h>
#include <server/object_server/public.h>
#include <server/chunk_server/public.h>
#include <ytlib/rpc/public.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(
        const Stroka& configFileName,
        TCellMasterConfigPtr config);

    ~TBootstrap();

    TCellMasterConfigPtr GetConfig() const;

    NRpc::IServerPtr GetRpcServer() const;
    NTransactionServer::TTransactionManagerPtr GetTransactionManager() const;
    NCypressServer::TCypressManagerPtr GetCypressManager() const;
    TMetaStateFacadePtr GetMetaStateFacade() const;
    NObjectServer::TObjectManagerPtr GetObjectManager() const;
    NChunkServer::TChunkManagerPtr GetChunkManager() const;
    NChunkServer::INodeAuthorityPtr GetNodeAuthority() const;
    IInvokerPtr GetControlInvoker();

    void Run();

private:
    Stroka ConfigFileName;
    TCellMasterConfigPtr Config;

    NRpc::IServerPtr RpcServer;
    NTransactionServer::TTransactionManagerPtr TransactionManager;
    NCypressServer::TCypressManagerPtr CypressManager;
    TMetaStateFacadePtr MetaStateFacade;
    NObjectServer::TObjectManagerPtr ObjectManager;
    NChunkServer::TChunkManagerPtr ChunkManager;
    NChunkServer::INodeAuthorityPtr HolderAuthority;
    TActionQueuePtr ControlQueue;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
