#include "medium_directory_synchronizer.h"

#include "config.h"
#include "medium_directory.h"
#include "private.h"

#include <yt/yt/client/api/connection.h>
#include <yt/yt/client/api/client.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/concurrency/periodic_executor.h>


namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NApi;
using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TMediumDirectorySynchronizer::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TMediumDirectorySynchronizerConfigPtr config,
        IConnectionPtr clusterConnection,
        TMediumDirectoryPtr mediumDirectory)
        : Config_(std::move(config))
        , ClusterConnection_(std::move(clusterConnection))
        , MediumDirectory_(std::move(mediumDirectory))
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

    TFuture<void> NextSync(bool force)
    {
        auto guard = Guard(SpinLock_);
        if (Stopped_) {
            return MakeFuture(TError("Cluster directory synchronizer is stopped"));
        }
        DoStart(force);
        return NextSyncPromise_.ToFuture();
    }

    TFuture<void> RecentSync()
    {
        auto guard = Guard(SpinLock_);
        if (Stopped_) {
            return MakeFuture(TError("Cluster directory synchronizer is stopped"));
        }
        DoStart(false);
        return RecentSyncPromise_.ToFuture();
    }

    DEFINE_SIGNAL(void(const TError&), Synchronized);

private:
    const TMediumDirectorySynchronizerConfigPtr Config_;
    const TWeakPtr<IConnection> ClusterConnection_;
    const TWeakPtr<TMediumDirectory> MediumDirectory_;

    const TPeriodicExecutorPtr SyncExecutor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    bool Started_ = false;
    bool Stopped_= false;
    TPromise<void> NextSyncPromise_ = NewPromise<void>();
    TPromise<void> RecentSyncPromise_ = NewPromise<void>();


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
        YT_UNUSED_FUTURE(SyncExecutor_->Stop());
    }

    void DoSync()
    {
        try {
            auto connection = ClusterConnection_.Lock();
            if (!connection) {
                THROW_ERROR_EXCEPTION("Cluster connection is not available");
            }

            YT_LOG_DEBUG("Started synchronizing medium directory");

            auto client = connection->CreateClient(TClientOptions::FromUser(NSecurityClient::RootUserName));

            TGetClusterMetaOptions options;
            options.ReadFrom = NApi::EMasterChannelKind::MasterCache;
            options.PopulateMediumDirectory = true;

            auto result = WaitFor(client->GetClusterMeta(options))
                .ValueOrThrow();

            auto mediumDirectory = MediumDirectory_.Lock();
            if (!mediumDirectory) {
                THROW_ERROR_EXCEPTION("Medium directory is not available");
            }
            mediumDirectory->LoadFrom(*result.MediumDirectory);

            YT_LOG_DEBUG("Finished synchronizing medium directory");
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error synchronizing medium directory")
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

        auto nextSyncPromise = NextSyncPromise_;
        // Don't drop the very first recent sync promise.
        if (!RecentSyncPromise_.IsSet()) {
            RecentSyncPromise_.Set(error);
        }
        RenewSyncPromises();
        nextSyncPromise.Set(error);
        RecentSyncPromise_.Set(error);
    }

    void RenewSyncPromises()
    {
        auto recentSyncPromise = NewPromise<void>();
        auto nextSyncPromise = NewPromise<void>();

        auto guard = Guard(SpinLock_);
        std::swap(nextSyncPromise, NextSyncPromise_);
        std::swap(recentSyncPromise, RecentSyncPromise_);
    }
};

////////////////////////////////////////////////////////////////////////////////

TMediumDirectorySynchronizer::TMediumDirectorySynchronizer(
    TMediumDirectorySynchronizerConfigPtr config,
    IConnectionPtr clusterConnection,
    TMediumDirectoryPtr mediumDirectory)
    : Impl_(New<TImpl>(
        std::move(config),
        std::move(clusterConnection),
        std::move(mediumDirectory)))
{ }

TMediumDirectorySynchronizer::~TMediumDirectorySynchronizer() = default;

void TMediumDirectorySynchronizer::Start()
{
    Impl_->Start();
}

void TMediumDirectorySynchronizer::Stop()
{
    Impl_->Stop();
}

TFuture<void> TMediumDirectorySynchronizer::NextSync(bool force)
{
    return Impl_->NextSync(force);
}

TFuture<void> TMediumDirectorySynchronizer::RecentSync()
{
    return Impl_->RecentSync();
}

DELEGATE_SIGNAL(TMediumDirectorySynchronizer, void(const TError&), Synchronized, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
