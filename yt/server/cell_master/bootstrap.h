#pragma once

#include "public.h"

#include <ytlib/concurrency/action_queue.h>

#include <ytlib/rpc/public.h>

#include <server/node_tracker_server/public.h>

#include <server/object_server/public.h>

#include <server/chunk_server/public.h>

#include <server/transaction_server/public.h>

#include <server/cypress_server/public.h>

#include <server/security_server/public.h>

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

    NNodeTrackerServer::TNodeTrackerPtr GetNodeTracker() const;

    NTransactionServer::TTransactionManagerPtr GetTransactionManager() const;

    NCypressServer::TCypressManagerPtr GetCypressManager() const;

    TMetaStateFacadePtr GetMetaStateFacade() const;

    NObjectServer::TObjectManagerPtr GetObjectManager() const;

    NChunkServer::TChunkManagerPtr GetChunkManager() const;

    NSecurityServer::TSecurityManagerPtr GetSecurityManager() const;

    IInvokerPtr GetControlInvoker() const;

    void Run();

private:
    Stroka ConfigFileName;
    TCellMasterConfigPtr Config;

    NRpc::IServerPtr RpcServer;

    NNodeTrackerServer::TNodeTrackerPtr NodeTracker;

    NTransactionServer::TTransactionManagerPtr TransactionManager;

    NCypressServer::TCypressManagerPtr CypressManager;

    TMetaStateFacadePtr MetaStateFacade;

    NObjectServer::TObjectManagerPtr ObjectManager;

    NChunkServer::TChunkManagerPtr ChunkManager;

    NSecurityServer::TSecurityManagerPtr SecurityManager;

    TActionQueuePtr ControlQueue;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
