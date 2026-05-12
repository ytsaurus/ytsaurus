#include "cluster_directory_synchronizer.h"
#include "cluster_directory.h"
#include "config.h"
#include "private.h"

#include <yt/yt/ytlib/misc/synchronizer_detail.h>

#include <yt/yt/client/api/connection.h>
#include <yt/yt/client/api/client.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT::NHiveClient {

using namespace NApi;
using namespace NConcurrency;
using namespace NThreading;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TClusterDirectorySynchronizer
    : public IClusterDirectorySynchronizer
    , public TSynchronizerBase
{
public:
    TClusterDirectorySynchronizer(
        TClusterDirectorySynchronizerConfigPtr config,
        IConnectionPtr directoryConnection,
        TClusterDirectoryPtr clusterDirectory)
        : TSynchronizerBase(
            NRpc::TDispatcher::Get()->GetLightInvoker(),
            TPeriodicExecutorOptions{
                .Period = config->SyncPeriod,
            },
            HiveClientLogger())
        , Config_(std::move(config))
        , DirectoryConnection_(std::move(directoryConnection))
        , ClusterDirectory_(std::move(clusterDirectory))
    { }

    void Start() override
    {
        TSynchronizerBase::Start();
    }

    void Stop() override
    {
        TSynchronizerBase::Stop();
    }

    TFuture<TClusterDirectoryUpdateResult> TrySync(bool force) override
    {
        // NB(apachee): A hacky way to preserve Sync behavior and return the result of the sync attempt,
        // without the overhaul of synchronizer base.
        TFuture<TClusterDirectoryUpdateResult> resultFuture;
        {
            auto guard = Guard(Lock_);
            resultFuture = SyncResultPromise_.ToFuture();
        }
        YT_UNUSED_FUTURE(TSynchronizerBase::Sync(force));
        return resultFuture;
    }

    TFuture<void> Sync(bool force) override
    {
        return TSynchronizerBase::Sync(force);
    }

    TFuture<void> GetFirstSuccessfulSyncFuture() override
    {
        // XXX(apachee): Returned future might never get set in case of persistent sync failure.
        return TSynchronizerBase::GetFirstSuccessfulSyncFuture();
    }

    TFuture<void> GetFirstSuccessfulClusterSyncFuture(const std::string& clusterName) override
    {
        // XXX(apachee): Returned future might never get set in case of persistent sync failure.
        auto guard = Guard(Lock_);
        if (SyncedClusters_ && !SyncedClusters_->contains(clusterName)) {
            return MakeFuture(MakeUnknownClusterError(clusterName));
        }
        auto promise = GetOrInsert(ClusterToSyncResultPromise_, clusterName, NewPromise<void>);
        return promise.ToFuture();
    }

    DEFINE_SIGNAL_OVERRIDE(void(const TError&), Synchronized);

private:
    const TClusterDirectorySynchronizerConfigPtr Config_;
    const TWeakPtr<IConnection> DirectoryConnection_;
    const TWeakPtr<TClusterDirectory> ClusterDirectory_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    TPromise<TClusterDirectoryUpdateResult> SyncResultPromise_ = NewPromise<TClusterDirectoryUpdateResult>();
    THashMap<std::string, TPromise<void>> ClusterToSyncResultPromise_;
    //! Set of all clusters visible from the cluster directory.
    //! Used to differentiate between clusters failing to sync and unknown clusters.
    std::optional<THashSet<std::string>> SyncedClusters_;

    void DoSync() override
    {
        try {
            NTracing::TNullTraceContextGuard nullTraceContext;

            auto connection = DirectoryConnection_.Lock();
            if (!connection) {
                THROW_ERROR_EXCEPTION("Directory connection is not available");
            }

            YT_LOG_DEBUG("Started synchronizing cluster directory");

            auto client = connection->CreateClient(TClientOptions::FromUser(NSecurityClient::RootUserName));

            TGetClusterMetaOptions options;
            options.PopulateClusterDirectory = true;
            options.ReadFrom = EMasterChannelKind::Cache;
            options.ExpireAfterSuccessfulUpdateTime = Config_->ExpireAfterSuccessfulUpdateTime;
            options.ExpireAfterFailedUpdateTime = Config_->ExpireAfterFailedUpdateTime;

            auto meta = WaitFor(client->GetClusterMeta(options))
                .ValueOrThrow();

            auto clusterDirectory = ClusterDirectory_.Lock();
            if (!clusterDirectory) {
                THROW_ERROR_EXCEPTION("Directory is not available");
            }

            auto result = clusterDirectory->TryUpdateDirectory(*meta.ClusterDirectory);
            HandleSyncResult(std::move(result));

            YT_LOG_DEBUG("Finished synchronizing cluster directory");

            Synchronized_.Fire(TError());
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            Synchronized_.Fire(error);
            THROW_ERROR_EXCEPTION("Error synchronizing cluster directory")
                << error;
        }
    }

    void HandleSyncResult(TClusterDirectoryUpdateResult result)
    {
        TPromise<TClusterDirectoryUpdateResult> syncResultPromise;
        std::vector<TPromise<void>> resolvedClusterSyncResultPromises;
        std::vector<std::pair<std::string, TPromise<void>>> unknownClusterSyncResultPromises;

        {
            THashSet<std::string> newSyncedClusters;
            for (const auto& [cluster, _] : result.ClusterToErrorMapping) {
                newSyncedClusters.insert(cluster);
            }

            auto guard = Guard(Lock_);

            syncResultPromise = std::exchange(SyncResultPromise_, NewPromise<TClusterDirectoryUpdateResult>());

            // Update synced clusters.
            SyncedClusters_ = std::move(newSyncedClusters);

            // Find synced cluster promises.
            for (const auto& [cluster, error] : result.ClusterToErrorMapping) {
                if (!error.IsOK()) {
                    continue;
                }

                // NB(apachee): GetOrInsert is needed to populate promises for clusters that were not requested yet, but could be requested after this pass.
                resolvedClusterSyncResultPromises.push_back(GetOrInsert(ClusterToSyncResultPromise_, cluster, NewPromise<void>));
            }

            // Find unknown cluster promises.
            std::vector<std::string> unknownClusters;
            for (const auto& [cluster, _] : ClusterToSyncResultPromise_) {
                if (!SyncedClusters_->contains(cluster)) {
                    unknownClusters.push_back(cluster);
                }
            }
            for (const auto& cluster : unknownClusters) {
                auto it = GetIteratorOrCrash(ClusterToSyncResultPromise_, cluster);
                unknownClusterSyncResultPromises.emplace_back(cluster, std::move(it->second));
                ClusterToSyncResultPromise_.erase(it);
            }
        }

        syncResultPromise.Set(result);
        for (const auto& promise : resolvedClusterSyncResultPromises) {
            promise.TrySet();
        }
        for (const auto& [cluster, promise] : unknownClusterSyncResultPromises) {
            promise.TrySet(MakeUnknownClusterError(cluster));
        }

        auto cumulativeError = result.GetCumulativeError();
        YT_LOG_ALERT_AND_THROW_UNLESS(cumulativeError.IsOK(), cumulativeError);
    }

    static TError MakeUnknownClusterError(const std::string& clusterName)
    {
        return TError("Unknown cluster %Qv", clusterName);
    }
};

////////////////////////////////////////////////////////////////////////////////

IClusterDirectorySynchronizerPtr CreateClusterDirectorySynchronizer(
    TClusterDirectorySynchronizerConfigPtr config,
    IConnectionPtr directoryConnection,
    TClusterDirectoryPtr clusterDirectory)
{
    return New<TClusterDirectorySynchronizer>(
        std::move(config),
        std::move(directoryConnection),
        std::move(clusterDirectory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
