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
        SynchronizationExecutor_->Stop();
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

    using TAlienDescriptorsMap = THashMap<int, std::vector<TAlienCellDescriptorLite>>;

    void DoReconfigure()
    {
        SynchronizationExecutor_->SetPeriod(Config_->SyncPeriod);
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
        auto descriptorsMap = ScanCells();

        std::vector<TFuture<std::vector<TAlienCellDescriptor>>> asyncAlienDescriptors;
        std::vector<int> clusterIndexes;
        std::vector<TString> clusterNames;
        std::vector<IClientPtr> clients;

        for (auto& [alienClusterIndex, descriptors] : descriptorsMap) {
            auto clusterName = GetAlienClusterRegistry()->GetAlienClusterName(alienClusterIndex);
            if (auto client = GetAlienClusterClient(clusterName)) {
                auto asyncResult = client->SyncAlienCells(std::move(descriptors)); 
                asyncAlienDescriptors.push_back(std::move(asyncResult));
                clients.push_back(std::move(client));
                clusterIndexes.push_back(alienClusterIndex);
                clusterNames.push_back(clusterName);
            }
        }

        YT_LOG_DEBUG("Syncing alien cells (AlienClusters: %v)",
            clusterNames);

        auto alienDescriptorsOrError = WaitFor(AllSet(asyncAlienDescriptors));
        if (!alienDescriptorsOrError.IsOK()) {
            YT_LOG_DEBUG(alienDescriptorsOrError, "Error synchronizing alien cells");
            return;
        }

        const auto& alienDescriptors = alienDescriptorsOrError.Value();
        std::vector<TAlienCellConstellation> constellations;
        YT_VERIFY(alienDescriptors.size() == clusterIndexes.size());

        for (int index = 0; index < std::ssize(clusterIndexes); ++index) {
            const auto& descriptorsOrError = alienDescriptors[index];
            const auto& alienClusterIndex = clusterIndexes[index];
            if (!descriptorsOrError.IsOK()) {
                YT_LOG_DEBUG(descriptorsOrError, "Error synchronizing alien cells (Cluster: %v)",
                    GetAlienClusterRegistry()->GetAlienClusterName(alienClusterIndex));
                continue;
            }

            const auto& descriptors = descriptorsOrError.Value();
            constellations.push_back({
                .AlienClusterIndex = alienClusterIndex,
                .AlienCells = std::move(descriptors)
            });
        }

        if (constellations.empty()) {
            return;
        }

        NProto::TReqUpdateAlienCellPeers request;
        ToProto(request.mutable_constellations(), constellations);

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger);
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

