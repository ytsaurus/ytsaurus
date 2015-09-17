#pragma once

#include "public.h"

#include <core/concurrency/action_queue.h>

#include <core/rpc/public.h>

#include <ytlib/monitoring/public.h>
#include <ytlib/monitoring/http_server.h>

#include <ytlib/election/public.h>

#include <server/hydra/public.h>

#include <server/hive/public.h>

#include <server/node_tracker_server/public.h>

#include <server/object_server/public.h>

#include <server/chunk_server/public.h>

#include <server/journal_server/public.h>

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
    explicit TBootstrap(NYTree::INodePtr configNode);
    ~TBootstrap();

    const NElection::TCellId& GetCellId() const;
    NObjectClient::TCellTag GetCellTag() const;
    TCellMasterConfigPtr GetConfig() const;
    NRpc::IServerPtr GetRpcServer() const;
    NElection::TCellManagerPtr GetCellManager() const;
    NHydra::IChangelogStorePtr GetChangelogStore() const;
    NHydra::ISnapshotStorePtr GetSnapshotStore() const;
    NNodeTrackerServer::TNodeTrackerPtr GetNodeTracker() const;
    NTransactionServer::TTransactionManagerPtr GetTransactionManager() const;
    NHive::TTransactionSupervisorPtr GetTransactionSupervisor() const;
    NCypressServer::TCypressManagerPtr GetCypressManager() const;
    THydraFacadePtr GetHydraFacade() const;
    TWorldInitializerPtr GetWorldInitializer() const;
    NObjectServer::TObjectManagerPtr GetObjectManager() const;
    NChunkServer::TChunkManagerPtr GetChunkManager() const;
    NSecurityServer::TSecurityManagerPtr GetSecurityManager() const;
    NTabletServer::TTabletManagerPtr GetTabletManager() const;
    NHive::THiveManagerPtr GetHiveManager() const;
    NHive::TCellDirectoryPtr GetCellDirectory() const;
    IInvokerPtr GetControlInvoker() const;

    void Initialize();
    void Run();
    void TryLoadSnapshot(const Stroka& fileName, bool dump);

private:
    const NYTree::INodePtr ConfigNode_;

    TCellMasterConfigPtr Config_;
    NRpc::IServerPtr RpcServer_;
    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    std::unique_ptr<NHttp::TServer> HttpServer_;
    NElection::TCellManagerPtr CellManager_;
    NHydra::IChangelogStorePtr ChangelogStore_;
    NHydra::ISnapshotStorePtr SnapshotStore_;
    NNodeTrackerServer::TNodeTrackerPtr NodeTracker_;
    NTransactionServer::TTransactionManagerPtr TransactionManager_;
    NHive::TTransactionSupervisorPtr TransactionSupervisor_;
    NCypressServer::TCypressManagerPtr CypressManager_;
    THydraFacadePtr HydraFacade_;
    TWorldInitializerPtr WorldInitializer_;
    NObjectServer::TObjectManagerPtr ObjectManager_;
    NChunkServer::TChunkManagerPtr ChunkManager_;
    NSecurityServer::TSecurityManagerPtr SecurityManager_;
    NTabletServer::TTabletManagerPtr TabletManager_;
    NHive::THiveManagerPtr HiveManager_;
    NHive::TCellDirectoryPtr CellDirectory_;
    NConcurrency::TActionQueuePtr ControlQueue_;


    void DoInitialize();
    void DoRun();
    void DoLoadSnapshot(const Stroka& fileName, bool dump);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
