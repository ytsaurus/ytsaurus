#include "medium_directory_synchronizer.h"
#include "medium_directory.h"
#include "config.h"
#include "private.h"

#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/native_client.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/rpc/dispatcher.h>

namespace NYT {
namespace NChunkClient {

using namespace NConcurrency;
using namespace NApi;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TMediumDirectorySynchronizer::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TMediumDirectorySynchronizerConfigPtr config,
        TMediumDirectoryPtr cellDirectory,
        INativeConnectionPtr connection)
        : Config_(std::move(config))
        , MediumDirectory_(std::move(cellDirectory))
        , WeakConnection_(MakeWeak(connection))
        , SyncExecutor_(New<TPeriodicExecutor>(
            NRpc::TDispatcher::Get()->GetLightInvoker(),
            BIND(&TImpl::OnSync, MakeWeak(this)),
            Config_->SyncPeriod))
        , Logger(NLogging::TLogger(ChunkClientLogger)
            .AddTag("CellTag: %v", connection->GetCellTag()))
    { }

    TFuture<void> Sync()
    {
        auto guard = Guard(SpinLock_);
        if (!SyncStarted_) {
            SyncExecutor_->Start();
            SyncStarted_ = true;
        }
        return SyncPromise_;
    }

    void Stop()
    {
        SyncExecutor_->Stop();
    }

private:
    const TMediumDirectorySynchronizerConfigPtr Config_;
    const TMediumDirectoryPtr MediumDirectory_;
    const TWeakPtr<INativeConnection> WeakConnection_;

    const TPeriodicExecutorPtr SyncExecutor_;
    const NLogging::TLogger Logger;

    TSpinLock SpinLock_;
    bool SyncStarted_ = false;
    TPromise<void> SyncPromise_ = NewPromise<void>();


    void DoSync()
    {
        try {
            auto connection = WeakConnection_.Lock();
            if (!connection) {
                THROW_ERROR_EXCEPTION("No connection is available");
            }

            LOG_DEBUG("Started synchronizing medium directory");

            auto client = connection->CreateNativeClient();

            TGetClusterMetaOptions options;
            options.ReadFrom = Config_->ReadFrom;
            options.PopulateMediumDirectory = true;
            auto result = WaitFor(client->GetClusterMeta(options))
                .ValueOrThrow();

            MediumDirectory_->UpdateDirectory(*result.MediumDirectory);

            LOG_DEBUG("Finished synchronizing medium directory");
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error synchronizing medium directory")
                << ex;
        }
    }

    void SetSyncResult(const TError& error)
    {
        auto guard = Guard(SpinLock_);
        if (SyncPromise_.IsSet()) {
            SyncPromise_ = MakePromise<void>(error);
        } else {
            auto promise = SyncPromise_;
            guard.Release();
            promise.Set(error);
        }
    }

    void OnSync()
    {
        try {
            DoSync();
            SetSyncResult(TError());
        } catch (const std::exception& ex) {
            TError error(ex);
            LOG_DEBUG(error);
            SetSyncResult(error);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TMediumDirectorySynchronizer::TMediumDirectorySynchronizer(
    TMediumDirectorySynchronizerConfigPtr config,
    TMediumDirectoryPtr mediumDirectory,
    INativeConnectionPtr connection)
    : Impl_(New<TImpl>(
        std::move(config),
        std::move(mediumDirectory),
        std::move(connection)))
{ }

TMediumDirectorySynchronizer::~TMediumDirectorySynchronizer() = default;

TFuture<void> TMediumDirectorySynchronizer::Sync()
{
    return Impl_->Sync();
}

void TMediumDirectorySynchronizer::Stop()
{
    return Impl_->Stop();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
