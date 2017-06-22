#include "cluster_directory_synchronizer.h"
#include "cluster_directory.h"
#include "config.h"
#include "private.h"

#include <yt/ytlib/api/connection.h>
#include <yt/ytlib/api/client.h>

#include <yt/core/rpc/dispatcher.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler.h>

namespace NYT {
namespace NHiveClient {

using namespace NConcurrency;
using namespace NApi;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HiveClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TClusterDirectorySynchronizer::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TClusterDirectorySynchronizerConfigPtr config,
        IConnectionPtr directoryConnection,
        TClusterDirectoryPtr clusterDirectory)
        : Config_(std::move(config))
        , DirectoryConnection_(std::move(directoryConnection))
        , ClusterDirectory_(std::move(clusterDirectory))
        , SyncExecutor_(New<TPeriodicExecutor>(
            NRpc::TDispatcher::Get()->GetLightInvoker(),
            BIND(&TImpl::OnSync, MakeWeak(this)),
            Config_->SyncPeriod))
    {
        SyncExecutor_->Start();
    }

    TFuture<void> Sync()
    {
        auto guard = Guard(SyncPromiseLock_);
        return SyncPromise_.ToFuture();
    }

private:
    const TClusterDirectorySynchronizerConfigPtr Config_;
    const TWeakPtr<IConnection> DirectoryConnection_;
    const TClusterDirectoryPtr ClusterDirectory_;

    const TPeriodicExecutorPtr SyncExecutor_;

    TSpinLock SyncPromiseLock_;
    TPromise<void> SyncPromise_ = NewPromise<void>();


    void DoSync()
    {
        try {
            auto connection = DirectoryConnection_.Lock();
            if (!connection) {
                THROW_ERROR_EXCEPTION("Directory connection is not available");
            }

            auto client = connection->CreateClient(TClientOptions(NSecurityClient::RootUserName));
            LOG_DEBUG("Started updating cluster directory");

            TGetClusterMetaOptions options;
            options.PopulateClusterDirectory = true;
            options.ReadFrom = EMasterChannelKind::Follower;
            auto meta = WaitFor(client->GetClusterMeta(options))
                .ValueOrThrow();

            ClusterDirectory_->UpdateDirectory(*meta.ClusterDirectory);

            LOG_DEBUG("Finished updating cluster directory");
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error updating cluster directory")
                << ex;
        }
    }

    void OnSync()
    {
        TError error;
        try {
            DoSync();
        } catch (const std::exception& ex) {
            error = TError(ex);
            LOG_DEBUG(error);
        }

        auto guard = Guard(SyncPromiseLock_);
        auto syncPromise = NewPromise<void>();
        std::swap(syncPromise, SyncPromise_);
        guard.Release();
        syncPromise.Set(error);
    }
};

////////////////////////////////////////////////////////////////////////////////

TClusterDirectorySynchronizer::TClusterDirectorySynchronizer(
    TClusterDirectorySynchronizerConfigPtr config,
    IConnectionPtr directoryConnection,
    TClusterDirectoryPtr clusterDirectory)
    : Impl_(New<TImpl>(
        std::move(config),
        std::move(directoryConnection),
        std::move(clusterDirectory)))
{ }

TClusterDirectorySynchronizer::~TClusterDirectorySynchronizer() = default;

TFuture<void> TClusterDirectorySynchronizer::Sync()
{
    return Impl_->Sync();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveClient
} // namespace NYT
