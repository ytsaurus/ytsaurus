#pragma once

#include "public.h"

#include <core/concurrency/action_queue.h>

#include <core/rpc/public.h>

#include <ytlib/election/public.h>

#include <server/hydra/public.h>

#include <server/hive/public.h>

#include <server/node_tracker_server/public.h>

#include <server/object_server/public.h>

#include <server/chunk_server/public.h>

#include <server/transaction_server/public.h>

#include <server/cypress_server/public.h>

#include <server/security_server/public.h>

#include <server/tablet_server/public.h>

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

    const NElection::TCellGuid& GetCellGuid() const;
    ui16 GetCellId() const;
    TCellMasterConfigPtr GetConfig() const;
    NRpc::IRpcServerPtr GetRpcServer() const;
    NElection::TCellManagerPtr GetCellManager() const;
    NHydra::IChangelogStorePtr GetChangelogStore() const;
    NHydra::ISnapshotStorePtr GetSnapshotStore() const;
    NNodeTrackerServer::TNodeTrackerPtr GetNodeTracker() const;
    NTransactionServer::TTransactionManagerPtr GetTransactionManager() const;
    NCypressServer::TCypressManagerPtr GetCypressManager() const;
    TMetaStateFacadePtr GetMetaStateFacade() const;
    NObjectServer::TObjectManagerPtr GetObjectManager() const;
    NChunkServer::TChunkManagerPtr GetChunkManager() const;
    NSecurityServer::TSecurityManagerPtr GetSecurityManager() const;
    NTabletServer::TTabletManagerPtr GetTabletManager() const;
    NHive::THiveManagerPtr GetHiveManager() const;
    NHive::TCellRegistryPtr GetCellRegistry() const;
    IInvokerPtr GetControlInvoker() const;

    void Run();

private:
    Stroka ConfigFileName;
    TCellMasterConfigPtr Config;

    NRpc::IRpcServerPtr RpcServer;
    NElection::TCellManagerPtr CellManager;
    NHydra::IChangelogStorePtr ChangelogStore;
    NHydra::ISnapshotStorePtr SnapshotStore;
    NNodeTrackerServer::TNodeTrackerPtr NodeTracker;
    NTransactionServer::TTransactionManagerPtr TransactionManager;
    NCypressServer::TCypressManagerPtr CypressManager;
    TMetaStateFacadePtr MetaStateFacade;
    NObjectServer::TObjectManagerPtr ObjectManager;
    NChunkServer::TChunkManagerPtr ChunkManager;
    NSecurityServer::TSecurityManagerPtr SecurityManager;
    NTabletServer::TTabletManagerPtr TabletManager;
    NHive::THiveManagerPtr HiveManager;
    NHive::TCellRegistryPtr CellRegistry;
    NConcurrency::TActionQueuePtr ControlQueue;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
