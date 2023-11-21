#include "chaos_cell_directory_synchronizer.h"

#include "config.h"

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chaos_client/chaos_master_service_proxy.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NChaosClient {

using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NObjectClient;
using namespace NTracing;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TChaosCellDirectorySynchronizer
    : public IChaosCellDirectorySynchronizer
{
public:
    TChaosCellDirectorySynchronizer(
        TChaosCellDirectorySynchronizerConfigPtr config,
        ICellDirectoryPtr cellDirectory,
        IConnectionPtr connection,
        NLogging::TLogger logger)
        : Config_(std::move(config))
        , CellDirectory_(std::move(cellDirectory))
        , Connection_(std::move(connection))
        , Logger(std::move(logger))
        , SyncExecutor_(New<TPeriodicExecutor>(
            NRpc::TDispatcher::Get()->GetHeavyInvoker(),
            BIND(&TChaosCellDirectorySynchronizer::OnSync, MakeWeak(this)),
            TPeriodicExecutorOptions{
                .Period = Config_->SyncPeriod,
                .Splay = Config_->SyncPeriodSplay
            }))
    { }

    void AddCellIds(const std::vector<NObjectClient::TCellId>& cellIds) override
    {
        auto guard = Guard(SpinLock_);

        for (auto cellId : cellIds) {
            AddCell(CellTagFromId(cellId), cellId);
        }
    }

    void AddCellTag(TCellTag cellTag) override
    {
        auto guard = Guard(SpinLock_);
        AddCell(cellTag);
    }

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
            return MakeFuture(TError("Chaos cell directory synchronizer is stopped"));
        }
        DoStart();
        return SyncPromise_.ToFuture();
    }

private:
    const TChaosCellDirectorySynchronizerConfigPtr Config_;
    const ICellDirectoryPtr CellDirectory_;
    const TWeakPtr<IConnection> Connection_;

    const NLogging::TLogger Logger;
    const TPeriodicExecutorPtr SyncExecutor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    bool Started_ = false;
    bool Stopped_ = false;
    TPromise<void> SyncPromise_ = NewPromise<void>();

    THashMap<TCellTag, TCellId> ObservedCells_;

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

    void AddCell(TCellTag cellTag, TCellId cellId = {})
    {
        if (auto it = ObservedCells_.find(cellTag)) {
            if (it->second && cellId) {
                ValidateChaosCellIdDuplication(cellTag, it->second, cellId);
            } else if (!it->second) {
                it->second = cellId;
            }
        } else {
            InsertOrCrash(ObservedCells_, std::make_pair(cellTag, cellId));
        }
    }

    void DoSync()
    {
        try {
            YT_LOG_DEBUG("Started synchronizing chaos cells in cell directory");

            auto connection = Connection_.Lock();
            if (!connection) {
                THROW_ERROR_EXCEPTION("Unable to synchronize chaos cells in cell directory: connection terminated");
            }

            auto masterChannel = connection->GetMasterChannelOrThrow(NApi::EMasterChannelKind::Follower, PrimaryMasterCellTagSentinel);
            auto proxy = TChaosMasterServiceProxy(std::move(masterChannel));
            auto req = proxy.GetCellDescriptors();

            auto rsp = WaitFor(req->Invoke())
                .ValueOrThrow();

            auto cellDescriptors = FromProto<std::vector<TCellDescriptor>>(rsp->cell_descriptors());

            for (auto descriptor : cellDescriptors) {
                auto cellTag = CellTagFromId(descriptor.CellId);
                TCellId cellId;
                {
                    auto guard = Guard(SpinLock_);
                    if (auto it = ObservedCells_.find(cellTag)) {
                        cellId = it->second;
                    }
                }

                if (cellId) {
                    ValidateChaosCellIdDuplication(cellTag, descriptor.CellId, cellId);
                } else {
                    auto guard = Guard(SpinLock_);
                    ObservedCells_[cellTag] = descriptor.CellId;
                }

                CellDirectory_->ReconfigureCell(descriptor);
            }

        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error synchronizing chaos cells in cell directory")
                << ex;
        }

        YT_LOG_DEBUG("Finished synchronizing chaos cells in cell directory");
    }

    void DoSyncObserved()
    {
        int observedCellCount = 0;

        try {
            YT_LOG_DEBUG("Started synchronizing observed chaos cells in cell directory");

            std::vector<TCellTag> cellTags;
            {
                auto guard = Guard(SpinLock_);

                if (ObservedCells_.empty()) {
                    YT_LOG_DEBUG("Skip synchronizing observed chaos cells as no observed cells found");
                    return;
                }

                observedCellCount = std::ssize(ObservedCells_);

                cellTags.reserve(ObservedCells_.size());
                for (auto [cellTag, cellId] : ObservedCells_) {
                    cellTags.push_back(cellTag);
                }
            }

            auto connection = Connection_.Lock();
            if (!connection) {
                THROW_ERROR_EXCEPTION("Unable to synchronize observed chaos cells in cell directory: connection terminated");
            }

            auto masterChannel = connection->GetMasterChannelOrThrow(NApi::EMasterChannelKind::Follower, PrimaryMasterCellTagSentinel);
            auto proxy = TChaosMasterServiceProxy(std::move(masterChannel));

            auto req = proxy.FindCellDescriptorsByCellTags();
            for (auto cellTag : cellTags) {
                req->add_cell_tags(cellTag);
            }

            auto rsp = WaitFor(req->Invoke())
                .ValueOrThrow();

            YT_VERIFY(std::ssize(cellTags) == rsp->cell_descriptors_size());

            for (int index = 0; index < std::ssize(cellTags); ++index) {
                auto descriptor = FromProto<TCellDescriptor>(rsp->cell_descriptors(index));
                auto cellTag = cellTags[index];
                auto cellId = ObservedCells_[cellTag];

                if (!descriptor.CellId) {
                    YT_LOG_DEBUG("Forgetting cell tag in chaos cell synchronizer (CellTag: %v)",
                        cellTag);

                    auto guard = Guard(SpinLock_);
                    EraseOrCrash(ObservedCells_, cellTag);
                    continue;
                }

                if (!cellId) {
                    auto guard = Guard(SpinLock_);
                    auto it = ObservedCells_.find(cellTag);
                    if (it->second) {
                        cellId = it->second;
                    } else {
                        cellId = descriptor.CellId;
                        it->second = cellId;
                    }
                }

                ValidateChaosCellIdDuplication(cellTag, descriptor.CellId, cellId);

                CellDirectory_->ReconfigureCell(descriptor);
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error synchronizing observed chaos cells in cell directory")
                << ex;
        }

        YT_LOG_DEBUG("Finished synchronizing observed chaos cells in cell directory (ObservedCellCount: %v)",
            observedCellCount);
    }

    void OnSync()
    {
        TTraceContextGuard traceContextGuard(TTraceContext::NewRoot("ChaosCellDirectorySync"));

        TError error;
        TPromise<void> syncPromise;

        {
            auto guard = Guard(SpinLock_);
            syncPromise = std::exchange(SyncPromise_, NewPromise<void>());
        }

        try {
            if (Config_->SyncAllChaosCells) {
                DoSync();
            } else {
                DoSyncObserved();
            }
        } catch (const std::exception& ex) {
            error = TError(ex);
            YT_LOG_DEBUG(error);
        }

        syncPromise.Set(error);
    }

    void ValidateChaosCellIdDuplication(
        TCellTag cellTag,
        TCellId existingCellId,
        TCellId newCellId)
    {
        if (newCellId != existingCellId) {
            YT_LOG_ALERT("Duplicate chaos cell id (CellTag: %v, ExistingCellId: %v, NewCellId: %v)",
                cellTag,
                existingCellId,
                newCellId);
            THROW_ERROR_EXCEPTION("Duplicate chaos cell id for tag %v", cellTag)
                << TErrorAttribute("existing_cell_id", existingCellId)
                << TErrorAttribute("new_cell_id", newCellId);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IChaosCellDirectorySynchronizerPtr CreateChaosCellDirectorySynchronizer(
    TChaosCellDirectorySynchronizerConfigPtr config,
    ICellDirectoryPtr cellDirectory,
    IConnectionPtr connection,
    NLogging::TLogger logger)
{
    return New<TChaosCellDirectorySynchronizer>(
        std::move(config),
        std::move(cellDirectory),
        std::move(connection),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
