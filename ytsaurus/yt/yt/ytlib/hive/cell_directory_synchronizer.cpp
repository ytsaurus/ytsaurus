#include "cell_directory_synchronizer.h"

#include "config.h"
#include "private.h"

#include <yt/yt/ytlib/hive/private.h>
#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/hive_service_proxy.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/random.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NHiveClient {

using namespace NConcurrency;
using namespace NObjectClient;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TCellDirectorySynchronizer
    : public ICellDirectorySynchronizer
{
public:
    TCellDirectorySynchronizer(
        TCellDirectorySynchronizerConfigPtr config,
        ICellDirectoryPtr cellDirectory,
        TCellIdList sourceOfTruthCellIds,
        const NLogging::TLogger& logger)
        : Config_(std::move(config))
        , CellDirectory_(std::move(cellDirectory))
        , SourceOfTruthCellIds_(std::move(sourceOfTruthCellIds))
        , Logger(logger)
        , SyncExecutor_(New<TPeriodicExecutor>(
            NRpc::TDispatcher::Get()->GetLightInvoker(),
            BIND(&TCellDirectorySynchronizer::OnSync, MakeWeak(this)),
            TPeriodicExecutorOptions{
                .Period = Config_->SyncPeriod,
                .Splay = Config_->SyncPeriodSplay
            }))
        , RandomGenerator_(TInstant::Now().GetValue())
    { }

    void Start() override
    {
        auto guard = Guard(SpinLock_);
        DoStart();
    }

    void Stop() override
    {
        auto guard = Guard(SpinLock_);
        DoStop();
    }

    TFuture<void> Sync() override
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
    const ICellDirectoryPtr CellDirectory_;
    const TCellIdList SourceOfTruthCellIds_;

    const NLogging::TLogger Logger;
    const TPeriodicExecutorPtr SyncExecutor_;

    TRandomGenerator RandomGenerator_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    bool Started_ = false;
    bool Stopped_ = false;
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
        YT_UNUSED_FUTURE(SyncExecutor_->Stop());
    }

    void DoSync()
    {
        try {
            YT_LOG_DEBUG("Started synchronizing cell directory");

            auto cellId = SourceOfTruthCellIds_[RandomGenerator_.Generate<size_t>() % SourceOfTruthCellIds_.size()];
            auto channel = CellDirectory_->GetChannelByCellIdOrThrow(cellId, NHydra::EPeerKind::Follower);
            THiveServiceProxy proxy(std::move(channel));

            auto req = proxy.SyncCells();
            req->SetTimeout(Config_->SyncRpcTimeout);
            for (const auto& cellInfo : CellDirectory_->GetRegisteredCells()) {
                if (TypeFromId(cellInfo.CellId) != EObjectType::MasterCell) {
                    ToProto(req->add_known_cells(), cellInfo);
                }
            }

            auto rsp = WaitFor(req->Invoke())
                .ValueOrThrow();

            for (const auto& info : rsp->cells_to_unregister()) {
                auto cellId = FromProto<TCellId>(info.cell_id());
                // NB: Currently we never unregister chaos cells; cf. YT-16393.
                if (TypeFromId(cellId) != EObjectType::ChaosCell) {
                    CellDirectory_->UnregisterCell(cellId);
                }
            }

            for (const auto& info : rsp->cells_to_reconfigure()) {
                auto descriptor = FromProto<TCellDescriptor>(info.cell_descriptor());
                CellDirectory_->ReconfigureCell(descriptor);
            }

            YT_LOG_DEBUG("Finished synchronizing cell directory");
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

ICellDirectorySynchronizerPtr CreateCellDirectorySynchronizer(
    TCellDirectorySynchronizerConfigPtr config,
    ICellDirectoryPtr cellDirectory,
    TCellIdList sourceOfTruthCellIds,
    const NLogging::TLogger& logger)
{
    return New<TCellDirectorySynchronizer>(
        std::move(config),
        std::move(cellDirectory),
        std::move(sourceOfTruthCellIds),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
