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
    TBootstrap(
        const Stroka& configFileName,
        TCellMasterConfigPtr config);

    ~TBootstrap();

    TCellMasterConfigPtr GetConfig() const;

    bool IsPrimaryMaster() const;
    bool IsSecondaryMaster() const;
    bool IsMulticell() const;

    const NObjectClient::TCellId& GetCellId() const;
    NObjectClient::TCellTag GetCellTag() const;

    const NObjectClient::TCellId& GetPrimaryCellId() const;
    NObjectClient::TCellTag GetPrimaryCellTag() const;

    NObjectClient::TCellId GetSecondaryCellId(NObjectClient::TCellTag cellTag) const;
    const std::vector<NObjectClient::TCellTag>& GetSecondaryCellTags() const;

    TMulticellManagerPtr GetMulticellManager() const;
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
    void DumpSnapshot(const Stroka& fileName);

private:
    const Stroka ConfigFileName_;
    const TCellMasterConfigPtr Config_;

    bool PrimaryMaster_ = false;
    bool SecondaryMaster_ = false;
    bool Multicell_ = false;

    NObjectClient::TCellId CellId_;
    NObjectClient::TCellTag CellTag_;
    NObjectClient::TCellId PrimaryCellId_;
    NObjectClient::TCellTag PrimaryCellTag_;
    std::vector<NObjectClient::TCellTag> SecondaryCellTags_;

    TMulticellManagerPtr MulticellManager_;
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
    NHive::TCellDirectorySynchronizerPtr CellDirectorySynchronizer_;
    NConcurrency::TActionQueuePtr ControlQueue_;


    static NElection::TPeerId ComputePeerId(
        NElection::TCellConfigPtr config,
        const Stroka& localAddress);

    void DoInitialize();
    void DoRun();
    void DoDumpSnapshot(const Stroka& fileName);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
