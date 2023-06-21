#include "alien_cell_synchronizer.h"

#include "alien_cell.h"
#include "alien_cluster_registry.h"
#include "config.h"
#include "private.h"
#include "chaos_cell.h"
#include "chaos_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/cell_server/tamed_cell_manager.h>
#include <yt/yt/server/master/cell_server/cell_base.h>

#include <yt/yt/server/master/chaos_server/proto/chaos_manager.pb.h>

#include <yt/yt/server/master/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NChaosServer {

using namespace NApi;
using namespace NCellServer;
using namespace NConcurrency;
using namespace NObjectServer;
using namespace NSecurityClient;
using namespace NYTree;
using namespace NHiveServer;
using namespace NHiveClient;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

const auto static& Logger = ChaosServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TAlienCellSynchronizer
    : public IAlienCellSynchronizer
{
public:
    explicit TAlienCellSynchronizer(NCellMaster::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(New<TAlienCellSynchronizerConfig>())
        , SynchronizationExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::AlienCellSynchronizer),
            BIND(&TAlienCellSynchronizer::Synchronize, MakeWeak(this))))
    { }

    void Start() override
    {
        DoReconfigure();
        SynchronizationExecutor_->Start();
    }

    void Stop() override
    {
        YT_UNUSED_FUTURE(SynchronizationExecutor_->Stop());
    }

    void Reconfigure(TAlienCellSynchronizerConfigPtr config) override
    {
        Config_ = std::move(config);
        DoReconfigure();
    }

private:
    NCellMaster::TBootstrap* const Bootstrap_;

    TAlienCellSynchronizerConfigPtr Config_;
    TPeriodicExecutorPtr SynchronizationExecutor_;
    TInstant LastFullSync_ = Now();

    using TAlienDescriptorsMap = THashMap<int, std::vector<TAlienCellDescriptorLite>>;

    void DoReconfigure()
    {
        SynchronizationExecutor_->SetPeriod(Config_->SyncPeriod);
    }

    bool IsTimeForFullSync()
    {
        auto now = Now();
        if (now - LastFullSync_ > Config_->FullSyncPeriod) {
            LastFullSync_ = now;
            return true;
        }
        return false;
    }

    void Synchronize()
    {
        if (!Config_->Enable) {
            return;
        }

        YT_PROFILE_TIMING("/chaos_server/alien_cell_synchronizer/synchronize_time") {
            DoSynchronize();
        }
    }

    void DoSynchronize()
    {
        auto fullSync = IsTimeForFullSync();
        auto descriptorsMap = ScanCells();

        std::vector<TFuture<NNative::TSyncAlienCellsResult>> asyncAlienClusterInfos;
        std::vector<int> clusterIndexes;
        std::vector<TString> clusterNames;
        std::vector<IClientPtr> clients;

        NNative::TSyncAlienCellOptions options{
            .FullSync = fullSync
        };

        for (auto& [alienClusterIndex, descriptors] : descriptorsMap) {
            auto clusterName = GetAlienClusterRegistry()->GetAlienClusterName(alienClusterIndex);
            if (auto client = GetAlienClusterClient(clusterName)) {
                auto asyncResult = client->SyncAlienCells(std::move(descriptors), options);
                asyncAlienClusterInfos.push_back(std::move(asyncResult));
                clients.push_back(std::move(client));
                clusterIndexes.push_back(alienClusterIndex);
                clusterNames.push_back(clusterName);
            }
        }

        YT_LOG_DEBUG("Syncing alien cells (AlienClusters: %v)",
            clusterNames);

        auto alienClusterInfosOrError = WaitFor(AllSet(asyncAlienClusterInfos));
        if (!alienClusterInfosOrError.IsOK()) {
            YT_LOG_DEBUG(alienClusterInfosOrError, "Error synchronizing alien cells");
            return;
        }

        const auto& alienClusterInfos = alienClusterInfosOrError.Value();
        std::vector<TAlienCellConstellation> constellations;
        YT_VERIFY(alienClusterInfos.size() == clusterIndexes.size());

        for (int index = 0; index < std::ssize(clusterIndexes); ++index) {
            const auto& alienClusterInfoOrError = alienClusterInfos[index];
            const auto& alienClusterIndex = clusterIndexes[index];
            if (!alienClusterInfoOrError.IsOK()) {
                YT_LOG_DEBUG(alienClusterInfoOrError, "Error synchronizing alien cells (Cluster: %v)",
                    GetAlienClusterRegistry()->GetAlienClusterName(alienClusterIndex));
                continue;
            }

            const auto& alienClusterInfo = alienClusterInfoOrError.Value();
            auto lostAlienCellIds = fullSync
                ? CalculateLostCellIds(descriptorsMap[alienClusterIndex], alienClusterInfo.AlienCellDescriptors)
                : std::vector<TCellId>();

            constellations.push_back({
                .AlienClusterIndex = alienClusterIndex,
                .AlienCells = std::move(alienClusterInfo.AlienCellDescriptors),
                .LostAlienCellIds = std::move(lostAlienCellIds),
                .EnableMetadataCells = alienClusterInfo.EnableMetadataCells
            });
        }

        if (constellations.empty()) {
            return;
        }

        NProto::TReqUpdateAlienCellPeers request;
        ToProto(request.mutable_constellations(), constellations);
        request.set_full_sync(fullSync);

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        YT_UNUSED_FUTURE(CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger));
    }

    TAlienDescriptorsMap ScanCells()
    {
        TAlienDescriptorsMap result;

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        for (auto* cell : cellManager->Cells(NCellarClient::ECellarType::Chaos)) {
            if (!IsObjectAlive(cell)) {
                continue;
            }

            for (const auto& [alienClusterIndex, configVersion] : cell->As<TChaosCell>()->AlienConfigVersions()) {
                result[alienClusterIndex].push_back({cell->GetId(), configVersion});
                YT_LOG_DEBUG("Alien peers found (ChaosCellId: %v, AlienCluster: %v, AlienConfigVersion: %v)",
                    cell->GetId(),
                    GetAlienClusterRegistry()->GetAlienClusterName(alienClusterIndex),
                    configVersion);
            }
        }

        return result;
    }

    std::vector<TCellId> CalculateLostCellIds(
        const std::vector<TAlienCellDescriptorLite>& knownByUs,
        const std::vector<TAlienCellDescriptor>& knownByOther)
    {
        THashSet<TCellId> seen;
        for (const auto& [cellId, version] : knownByUs) {
            seen.insert(cellId);
        }
        for (const auto& descriptor : knownByOther) {
            seen.erase(descriptor.CellId);
        }
        return std::vector<TCellId>(seen.begin(), seen.end());
    }

    NNative::IClientPtr GetAlienClusterClient(const TString& clusterName)
    {
        auto connection = NNative::FindRemoteConnection(Bootstrap_->GetClusterConnection(), clusterName);
        if (!connection) {
            YT_LOG_WARNING("Could not find native connection config to alien cluster (ClusterName: %Qv)",
                clusterName);
            return {};
        }

        return connection->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::AlienCellSynchronizerUserName));
    }

    const TAlienClusterRegistryPtr& GetAlienClusterRegistry()
    {
        return Bootstrap_->GetChaosManager()->GetAlienClusterRegistry();
    }
};

////////////////////////////////////////////////////////////////////////////////

IAlienCellSynchronizerPtr CreateAlienCellSynchronizer(NCellMaster::TBootstrap* bootstrap)
{
    return New<TAlienCellSynchronizer>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer

