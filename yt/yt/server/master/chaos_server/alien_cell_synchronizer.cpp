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

#include <yt/yt/core/misc/collection_helpers.h>

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
        , SynchronizationExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::AlienCellSynchronizer),
            BIND(&TAlienCellSynchronizer::Synchronize, MakeWeak(this))))
        , Config_(New<TAlienCellSynchronizerConfig>())
    { }

    void Start() override
    {
        ClusterSyncStates_.clear();
        LastMetadataExpirationCheck_ = Now();
        DoReconfigure();
        SynchronizationExecutor_->Start();
    }

    void Stop() override
    {
        YT_UNUSED_FUTURE(SynchronizationExecutor_->Stop());
        ClusterSyncStates_.clear();
    }

    void Reconfigure(TAlienCellSynchronizerConfigPtr config) override
    {
        if (config->EnablePerClusterSync &&
            (!Config_->EnablePerClusterSync || (!Config_->Enable && config->Enable)))
        {
            auto now = Now();
            for (auto& item : ClusterSyncStates_) {
                item.second.LastSuccessfulSync = now;
            }
            LastMetadataExpirationCheck_ = now;
        }
        Config_ = std::move(config);
        DoReconfigure();
    }

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    const TPeriodicExecutorPtr SynchronizationExecutor_;

    TAlienCellSynchronizerConfigPtr Config_;
    TInstant LastFullSync_ = Now();

    struct TClusterSyncState
    {
        bool InFlight = false;
        TInstant LastFullSync = Now();
        TInstant LastSuccessfulSync = Now();
    };

    THashMap<int, TClusterSyncState> ClusterSyncStates_;
    TInstant LastMetadataExpirationCheck_ = Now();

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

        if (Config_->EnablePerClusterSync) {
            DoSynchronizePerCluster();
            MaybeExpireMetadataClusters();
        } else {
            YT_PROFILE_TIMING("/chaos_server/alien_cell_synchronizer/synchronize_time") {
                DoSynchronizeLegacy();
            }
        }
    }

    void DoSynchronizeLegacy()
    {
        auto fullSync = IsTimeForFullSync();
        auto descriptorsMap = ScanCells();

        std::vector<TFuture<NNative::TSyncAlienCellsResult>> asyncAlienClusterInfos;
        std::vector<int> clusterIndexes;
        std::vector<std::string> clusterNames;
        std::vector<IClientPtr> clients;

        NNative::TSyncAlienCellOptions options{
            .FullSync = fullSync
        };

        asyncAlienClusterInfos.reserve(descriptorsMap.size());
        clusterIndexes.reserve(descriptorsMap.size());
        clusterNames.reserve(descriptorsMap.size());
        clients.reserve(descriptorsMap.size());
        for (const auto& [alienClusterIndex, descriptors] : descriptorsMap) {
            auto clusterName = GetAlienClusterRegistry()->GetAlienClusterName(alienClusterIndex);
            if (auto client = GetAlienClusterClient(clusterName)) {
                auto asyncResult = client->SyncAlienCells(descriptors, options);
                asyncAlienClusterInfos.push_back(std::move(asyncResult));
                clients.push_back(std::move(client));
                clusterIndexes.push_back(alienClusterIndex);
                clusterNames.push_back(clusterName);
            }
        }

        YT_TLOG_DEBUG("Syncing alien cells")
            .With("AlienClusters", clusterNames);

        auto alienClusterInfosOrError = WaitFor(AllSet(std::move(asyncAlienClusterInfos)));
        if (!alienClusterInfosOrError.IsOK()) {
            YT_TLOG_DEBUG("Error synchronizing alien cells")
                .With(alienClusterInfosOrError);
            return;
        }

        const auto& alienClusterInfos = alienClusterInfosOrError.Value();
        int clusterCount = std::ssize(clusterIndexes);
        YT_VERIFY(std::ssize(alienClusterInfos) == clusterCount);

        std::vector<TAlienCellConstellation> constellations;
        constellations.reserve(clusterCount);
        for (int index = 0; index < clusterCount; ++index) {
            const auto& alienClusterInfoOrError = alienClusterInfos[index];
            const auto& alienClusterIndex = clusterIndexes[index];
            if (!alienClusterInfoOrError.IsOK()) {
                YT_TLOG_DEBUG("Error synchronizing alien cells")
                    .With("Cluster", clusterNames[index])
                    .With(alienClusterInfoOrError);
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
                .EnableMetadataCells = alienClusterInfo.EnableMetadataCells,
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
            ->CommitAndLog(Logger()));
    }

    void DoSynchronizePerCluster()
    {
        auto descriptorsMap = ScanCells();
        for (auto& [alienClusterIndex, descriptors] : descriptorsMap) {
            auto& state = ClusterSyncStates_[alienClusterIndex];
            if (state.InFlight) {
                continue;
            }

            auto clusterName = GetAlienClusterRegistry()->GetAlienClusterName(alienClusterIndex);
            auto client = GetAlienClusterClient(clusterName);
            if (!client) {
                continue;
            }

            auto fullSync = Now() - state.LastFullSync > Config_->FullSyncPeriod;
            NNative::TSyncAlienCellOptions options{
                .FullSync = fullSync,
            };
            state.InFlight = true;

            YT_TLOG_DEBUG("Syncing alien cells")
                .With("AlienCluster", clusterName);

            client->SyncAlienCells(descriptors, options).Subscribe(
                BIND(
                    &TAlienCellSynchronizer::OnSyncCompleted,
                    MakeWeak(this),
                    alienClusterIndex,
                    std::move(clusterName),
                    std::move(descriptors),
                    fullSync)
                    .Via(Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(
                        NCellMaster::EAutomatonThreadQueue::AlienCellSynchronizer)));
        }
    }

    void MaybeExpireMetadataClusters()
    {
        const auto& clusterNames = GetAlienClusterRegistry()->GetIndexToName();
        for (int index = 0; index < std::ssize(clusterNames); ++index) {
            ClusterSyncStates_[index];
        }

        auto now = Now();
        if (now - LastMetadataExpirationCheck_ < Config_->FullSyncPeriod) {
            return;
        }
        LastMetadataExpirationCheck_ = now;

        NProto::TReqExpireMetadataClusters request;
        for (int index = 0; index < std::ssize(clusterNames); ++index) {
            if (now - ClusterSyncStates_[index].LastSuccessfulSync > 2 * Config_->FullSyncPeriod) {
                request.add_expired_clusters(clusterNames[index]);
            }
        }

        if (request.expired_clusters_size() == 0) {
            return;
        }

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        YT_UNUSED_FUTURE(CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger()));
    }

    void OnSyncCompleted(
        int alienClusterIndex,
        const std::string& clusterName,
        const std::vector<TAlienCellDescriptorLite>& descriptors,
        bool fullSync,
        const TErrorOr<NNative::TSyncAlienCellsResult>& resultOrError)
    {
        auto& state = GetOrCrash(ClusterSyncStates_, alienClusterIndex);
        state.InFlight = false;

        if (!Config_->Enable || !Config_->EnablePerClusterSync) {
            return;
        }

        if (!resultOrError.IsOK()) {
            YT_TLOG_DEBUG("Error synchronizing alien cells")
                .With("AlienCluster", clusterName)
                .With(resultOrError);
            return;
        }

        auto now = Now();
        state.LastSuccessfulSync = now;
        if (fullSync) {
            state.LastFullSync = now;
        }

        const auto& result = resultOrError.Value();
        TAlienCellConstellation constellation{
            .AlienClusterIndex = alienClusterIndex,
            .AlienCells = result.AlienCellDescriptors,
            .LostAlienCellIds = fullSync
                ? CalculateLostCellIds(descriptors, result.AlienCellDescriptors)
                : std::vector<TCellId>(),
            .EnableMetadataCells = result.EnableMetadataCells,
        };

        NProto::TReqUpdateAlienCellPeers request;
        ToProto(request.add_constellations(), constellation);
        request.set_full_sync(fullSync);
        request.set_scoped_full_sync(true);

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        YT_UNUSED_FUTURE(CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger()));
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
                YT_TLOG_DEBUG("Alien peers found")
                    .With("ChaosCellId", cell->GetId())
                    .With("AlienCluster", GetAlienClusterRegistry()->GetAlienClusterName(alienClusterIndex))
                    .With("AlienConfigVersion", configVersion);
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

    NNative::IClientPtr GetAlienClusterClient(const std::string& clusterName)
    {
        auto connection = NNative::FindRemoteConnection(Bootstrap_->GetClusterConnection(), clusterName);
        if (!connection) {
            YT_TLOG_WARNING("Could not find native connection config to alien cluster")
                .With("ClusterName", clusterName);
            return {};
        }

        return connection->CreateNativeClient(NNative::TClientOptions::FromUser(NSecurityClient::AlienCellSynchronizerUserName));
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
