#pragma once

#include "public.h"

#include <yt/server/chunk_server/public.h>

#include <yt/server/cypress_server/public.h>

#include <yt/server/hive/public.h>

#include <yt/server/hydra/public.h>

#include <yt/server/journal_server/public.h>

#include <yt/server/misc/public.h>

#include <yt/server/node_tracker_server/public.h>

#include <yt/server/object_server/public.h>

#include <yt/server/security_server/public.h>

#include <yt/server/tablet_server/public.h>

#include <yt/server/transaction_server/public.h>

#include <yt/server/journal_server/public.h>

#include <yt/server/transaction_server/public.h>

#include <yt/ytlib/election/public.h>

#include <yt/ytlib/monitoring/http_server.h>
#include <yt/ytlib/monitoring/public.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    explicit TBootstrap(NYTree::INodePtr configNode);
    ~TBootstrap();

    TCellMasterConfigPtr GetConfig() const;

    bool IsPrimaryMaster() const;
    bool IsSecondaryMaster() const;
    bool IsMulticell() const;

    const NObjectClient::TCellId& GetCellId() const;
    NObjectClient::TCellId GetCellId(NObjectClient::TCellTag cellTag) const;
    NObjectClient::TCellTag GetCellTag() const;

    const NObjectClient::TCellId& GetPrimaryCellId() const;
    NObjectClient::TCellTag GetPrimaryCellTag() const;

    const NObjectClient::TCellTagList& GetSecondaryCellTags() const;

    TMulticellManagerPtr GetMulticellManager() const;
    NRpc::IServerPtr GetRpcServer() const;
    NRpc::IChannelPtr GetLocalRpcChannel() const;
    NElection::TCellManagerPtr GetCellManager() const;
    NHydra::IChangelogStoreFactoryPtr GetChangelogStoreFactory() const;
    NHydra::ISnapshotStorePtr GetSnapshotStore() const;
    NNodeTrackerServer::TNodeTrackerPtr GetNodeTracker() const;
    NTransactionServer::TTransactionManagerPtr GetTransactionManager() const;
    NHive::TTransactionSupervisorPtr GetTransactionSupervisor() const;
    NCypressServer::TCypressManagerPtr GetCypressManager() const;
    THydraFacadePtr GetHydraFacade() const;
    TWorldInitializerPtr GetWorldInitializer() const;
    NObjectServer::TObjectManagerPtr GetObjectManager() const;
    NChunkServer::TChunkManagerPtr GetChunkManager() const;
    NJournalServer::TJournalManagerPtr GetJournalManager() const;
    NSecurityServer::TSecurityManagerPtr GetSecurityManager() const;
    NTabletServer::TTabletManagerPtr GetTabletManager() const;
    NHive::THiveManagerPtr GetHiveManager() const;
    NHive::TCellDirectoryPtr GetCellDirectory() const;
    IInvokerPtr GetControlInvoker() const;

    NNodeTrackerClient::INodeChannelFactoryPtr GetLightNodeChannelFactory() const;
    NNodeTrackerClient::INodeChannelFactoryPtr GetHeavyNodeChannelFactory() const;

    void Initialize();
    void Run();
    void TryLoadSnapshot(const Stroka& fileName, bool dump);

private:
    const NYTree::INodePtr ConfigNode_;
    TCellMasterConfigPtr Config_;

    bool PrimaryMaster_ = false;
    bool SecondaryMaster_ = false;
    bool Multicell_ = false;

    NObjectClient::TCellId CellId_;
    NObjectClient::TCellTag CellTag_;
    NObjectClient::TCellId PrimaryCellId_;
    NObjectClient::TCellTag PrimaryCellTag_;
    NObjectClient::TCellTagList SecondaryCellTags_;

    TMulticellManagerPtr MulticellManager_;
    NRpc::IServerPtr RpcServer_;
    NRpc::IChannelPtr LocalRpcChannel_;
    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    std::unique_ptr<NHttp::TServer> HttpServer_;
    NElection::TCellManagerPtr CellManager_;
    NHydra::IChangelogStoreFactoryPtr ChangelogStoreFactory_;
    NHydra::ISnapshotStorePtr SnapshotStore_;
    NNodeTrackerServer::TNodeTrackerPtr NodeTracker_;
    NTransactionServer::TTransactionManagerPtr TransactionManager_;
    NHive::TTransactionSupervisorPtr TransactionSupervisor_;
    NCypressServer::TCypressManagerPtr CypressManager_;
    THydraFacadePtr HydraFacade_;
    TWorldInitializerPtr WorldInitializer_;
    NObjectServer::TObjectManagerPtr ObjectManager_;
    NChunkServer::TChunkManagerPtr ChunkManager_;
    NJournalServer::TJournalManagerPtr JournalManager_;
    NSecurityServer::TSecurityManagerPtr SecurityManager_;
    NTabletServer::TTabletManagerPtr TabletManager_;
    NHive::THiveManagerPtr HiveManager_;
    NHive::TCellDirectoryPtr CellDirectory_;
    NHive::TCellDirectorySynchronizerPtr CellDirectorySynchronizer_;
    NConcurrency::TActionQueuePtr ControlQueue_;
    TCoreDumperPtr CoreDumper_;

    NNodeTrackerClient::INodeChannelFactoryPtr LightNodeChannelFactory_;
    NNodeTrackerClient::INodeChannelFactoryPtr HeavyNodeChannelFactory_;

    static NElection::TPeerId ComputePeerId(
        NElection::TCellConfigPtr config,
        const Stroka& localAddress);

    void DoInitialize();
    void DoRun();
    void DoLoadSnapshot(const Stroka& fileName, bool dump);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
