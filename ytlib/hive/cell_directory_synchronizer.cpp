#include "cell_directory_synchronizer.h"
#include "config.h"
#include "private.h"

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/rpc/dispatcher.h>

#include <yt/ytlib/hive/private.h>
#include <yt/ytlib/hive/cell_directory.h>
#include <yt/ytlib/hive/hive_service_proxy.h>

namespace NYT {
namespace NHiveClient {

using namespace NConcurrency;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TCellDirectorySynchronizer::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TCellDirectorySynchronizerConfigPtr config,
        TCellDirectoryPtr cellDirectory,
        const TCellId& primaryCellId,
        const NLogging::TLogger& logger)
        : Config_(std::move(config))
        , CellDirectory_(std::move(cellDirectory))
        , PrimaryCellId_(primaryCellId)
        , Logger(logger)
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

    TFuture<void> Sync()
    {
        auto guard = Guard(SpinLock_);
        if (Stopped_) {
            return MakeFuture(TError("Cell directory synchronizer is stopped"));
        }
        DoStart();
        return SyncPromise_.ToFuture();
    }

private:
    const TCellDirectorySynchronizerConfigPtr Config_;
    const TCellDirectoryPtr CellDirectory_;
    const TCellId PrimaryCellId_;

    const NLogging::TLogger Logger;
    const TPeriodicExecutorPtr SyncExecutor_;

    TSpinLock SpinLock_;
    bool Started_ = false;
    bool Stopped_= false;
    TPromise<void> SyncPromise_ = NewPromise<void>();


    void DoStart()
    {
        if (Started_) {
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
            LOG_DEBUG("Started synchronizing cell directory");

            auto channel = CellDirectory_->GetChannelOrThrow(PrimaryCellId_, NHydra::EPeerKind::Follower);
            THiveServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Config_->SyncRpcTimeout);

            auto req = proxy.SyncCells();
            ToProto(req->mutable_known_cells(), CellDirectory_->GetRegisteredCells());

            auto rsp = WaitFor(req->Invoke())
                .ValueOrThrow();

            for (const auto& info : rsp->cells_to_unregister()) {
                auto cellId = FromProto<TCellId>(info.cell_id());
                CellDirectory_->UnregisterCell(cellId);
            }

            for (const auto& info : rsp->cells_to_reconfigure()) {
                auto descriptor = FromProto<TCellDescriptor>(info.cell_descriptor());
                CellDirectory_->ReconfigureCell(descriptor);
            }

            LOG_DEBUG("Finished synchronizing cell directory");
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error synchronizing cell directory")
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

        auto guard = Guard(SpinLock_);
        auto syncPromise = NewPromise<void>();
        std::swap(syncPromise, SyncPromise_);
        guard.Release();
        syncPromise.Set(error);
    }
};

////////////////////////////////////////////////////////////////////////////////

TCellDirectorySynchronizer::TCellDirectorySynchronizer(
    TCellDirectorySynchronizerConfigPtr config,
    TCellDirectoryPtr cellDirectory,
    const TCellId& primaryCellId,
    const NLogging::TLogger& logger)
    : Impl_(New<TImpl>(
        std::move(config),
        std::move(cellDirectory),
        primaryCellId,
        logger))
{ }

TCellDirectorySynchronizer::~TCellDirectorySynchronizer() = default;

void TCellDirectorySynchronizer::Start()
{
    Impl_->Start();
}

void TCellDirectorySynchronizer::Stop()
{
    Impl_->Stop();
}

TFuture<void> TCellDirectorySynchronizer::Sync()
{
    return Impl_->Sync();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveClient
} // namespace NYT
