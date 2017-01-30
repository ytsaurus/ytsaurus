#include "medium_directory_synchronizer.h"
#include "medium_directory.h"
#include "config.h"
#include "private.h"

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
        INativeClientPtr client)
        : Config_(std::move(config))
        , MediumDirectory_(std::move(cellDirectory))
        , Client_(std::move(client))
        , Logger(NLogging::TLogger(ChunkClientLogger)
            .AddTag("CellTag: %v", Client_->GetConnection()->GetCellTag()))
        , SyncExecutor_(New<TPeriodicExecutor>(
            NRpc::TDispatcher::Get()->GetLightInvoker(),
            BIND(&TImpl::OnSync, MakeWeak(this)),
            Config_->SyncPeriod))
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

private:
    const TMediumDirectorySynchronizerConfigPtr Config_;
    const TMediumDirectoryPtr MediumDirectory_;
    const INativeClientPtr Client_;

    const NLogging::TLogger Logger;
    const TPeriodicExecutorPtr SyncExecutor_;

    TSpinLock SpinLock_;
    bool SyncStarted_;
    TPromise<void> SyncPromise_ = NewPromise<void>();


    void DoSync()
    {
        try {
            LOG_DEBUG("Started synchronizing medium directory");

            TGetClusterMetaOptions options;
            options.PopulateMediumDirectory = true;
            auto result = WaitFor(Client_->GetClusterMeta(options))
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
    INativeClientPtr client)
    : Impl_(New<TImpl>(
        std::move(config),
        std::move(mediumDirectory),
        std::move(client)))
{ }

TMediumDirectorySynchronizer::~TMediumDirectorySynchronizer() = default;

TFuture<void> TMediumDirectorySynchronizer::Sync()
{
    return Impl_->Sync();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
