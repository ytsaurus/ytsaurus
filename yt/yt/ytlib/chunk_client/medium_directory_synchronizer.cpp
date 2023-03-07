#include "medium_directory_synchronizer.h"

#include "config.h"
#include "medium_directory.h"
#include "private.h"

#include <yt/client/api/connection.h>
#include <yt/client/api/client.h>

#include <yt/core/rpc/dispatcher.h>

#include <yt/core/concurrency/periodic_executor.h>


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
    const TMediumDirectorySynchronizerConfigPtr Config_;
    const TWeakPtr<IConnection> ClusterConnection_;
    const TWeakPtr<TMediumDirectory> MediumDirectory_;

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
            auto connection = ClusterConnection_.Lock();
            if (!connection) {
                THROW_ERROR_EXCEPTION("Cluster connection is not available");
            }

            YT_LOG_DEBUG("Started synchronizing medium directory");

            auto client = connection->CreateClient(TClientOptions(NSecurityClient::RootUserName));

            TGetClusterMetaOptions options;
            options.ReadFrom = NApi::EMasterChannelKind::SecondLevelCache;
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

        auto guard = Guard(SpinLock_);
        auto syncPromise = NewPromise<void>();
        std::swap(syncPromise, SyncPromise_);
        guard.Release();
        syncPromise.Set(error);
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

TFuture<void> TMediumDirectorySynchronizer::Sync(bool force)
{
    return Impl_->Sync(force);
}

DELEGATE_SIGNAL(TMediumDirectorySynchronizer, void(const TError&), Synchronized, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
