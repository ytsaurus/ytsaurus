#include "cluster_directory_synchronizer.h"
#include "cluster_directory.h"
#include "config.h"
#include "private.h"

#include <yt/client/api/connection.h>
#include <yt/client/api/client.h>

#include <yt/core/rpc/dispatcher.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler.h>

namespace NYT::NHiveClient {

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
    { }

    void Start()
    {
        auto guard = Guard(SpinLock_);
        DoStart();
    }

    void Stop()
    {
        auto guard = Guard(SpinLock_);
        DoStop();
    }

    TFuture<void> Sync(bool force)
    {
        auto guard = Guard(SpinLock_);
        if (Stopped_) {
            return MakeFuture(TError("Cluster directory synchronizer is stopped"));
        }
        DoStart(force);
        return SyncPromise_.ToFuture();
    }

    DEFINE_SIGNAL(void(const TError&), Synchronized);

private:
    const TClusterDirectorySynchronizerConfigPtr Config_;
    const TWeakPtr<IConnection> DirectoryConnection_;
    const TWeakPtr<TClusterDirectory> ClusterDirectory_;

    const TPeriodicExecutorPtr SyncExecutor_;

    TSpinLock SpinLock_;
    bool Started_ = false;
    bool Stopped_= false;
    TPromise<void> SyncPromise_ = NewPromise<void>();


    void DoStart(bool force = false)
    {
        if (Started_) {
            if (force) {
                SyncExecutor_->ScheduleOutOfBand();
            }
            return;
        }
        Started_ = true;
        SyncExecutor_->Start();
        SyncExecutor_->ScheduleOutOfBand();
    }

    void DoStop()
    {
        if (Stopped_) {
            return;
        }
        Stopped_ = true;
        SyncExecutor_->Stop();
    }

    void DoSync()
    {
        try {
            auto connection = DirectoryConnection_.Lock();
            if (!connection) {
                THROW_ERROR_EXCEPTION("Directory connection is not available");
            }

            auto client = connection->CreateClient(TClientOptions(NSecurityClient::RootUserName));
            YT_LOG_DEBUG("Started updating cluster directory");

            TGetClusterMetaOptions options;
            options.PopulateClusterDirectory = true;
            options.ReadFrom = EMasterChannelKind::Follower;
            auto meta = WaitFor(client->GetClusterMeta(options))
                .ValueOrThrow();

            auto clusterDirectory = ClusterDirectory_.Lock();
            if (!clusterDirectory) {
                THROW_ERROR_EXCEPTION("Directory is not available");
            }

            clusterDirectory->UpdateDirectory(*meta.ClusterDirectory);

            YT_LOG_DEBUG("Finished updating cluster directory");
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
            Synchronized_.Fire(TError());
        } catch (const std::exception& ex) {
            error = TError(ex);
            Synchronized_.Fire(error);
            YT_LOG_DEBUG(error);
        }

        auto guard = Guard(SpinLock_);
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

void TClusterDirectorySynchronizer::Start()
{
    Impl_->Start();
}

void TClusterDirectorySynchronizer::Stop()
{
    Impl_->Stop();
}

TFuture<void> TClusterDirectorySynchronizer::Sync(bool force)
{
    return Impl_->Sync(force);
}

DELEGATE_SIGNAL(TClusterDirectorySynchronizer, void(const TError&), Synchronized, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
