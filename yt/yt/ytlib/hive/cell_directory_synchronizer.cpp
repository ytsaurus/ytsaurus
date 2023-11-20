#include "cell_directory_synchronizer.h"

#include "config.h"
#include "private.h"

#include <yt/yt/ytlib/misc/synchronizer_detail.h>

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

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TCellDirectorySynchronizer
    : public ICellDirectorySynchronizer
    , public TSynchronizerBase
{
public:
    TCellDirectorySynchronizer(
        TCellDirectorySynchronizerConfigPtr config,
        ICellDirectoryPtr cellDirectory,
        TCellIdList sourceOfTruthCellIds,
        NLogging::TLogger logger)
        : TSynchronizerBase(
            NRpc::TDispatcher::Get()->GetLightInvoker(),
            TPeriodicExecutorOptions{
                .Period = config->SyncPeriod,
                .Splay = config->SyncPeriodSplay
            },
            std::move(logger))
        , Config_(std::move(config))
        , CellDirectory_(std::move(cellDirectory))
        , SourceOfTruthCellIds_(std::move(sourceOfTruthCellIds))
    { }

    void Start() override
    {
        TSynchronizerBase::Start();
    }

    void Stop() override
    {
        TSynchronizerBase::Stop();
    }

    TFuture<void> Sync() override
    {
        return TSynchronizerBase::Sync(/*immediately*/ false);
    }

private:
    const TCellDirectorySynchronizerConfigPtr Config_;
    const ICellDirectoryPtr CellDirectory_;
    const TCellIdList SourceOfTruthCellIds_;

    TRandomGenerator RandomGenerator_{TInstant::Now().GetValue()};

    void DoSync() override
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
};

////////////////////////////////////////////////////////////////////////////////

ICellDirectorySynchronizerPtr CreateCellDirectorySynchronizer(
    TCellDirectorySynchronizerConfigPtr config,
    ICellDirectoryPtr cellDirectory,
    TCellIdList sourceOfTruthCellIds,
    NLogging::TLogger logger)
{
    return New<TCellDirectorySynchronizer>(
        std::move(config),
        std::move(cellDirectory),
        std::move(sourceOfTruthCellIds),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
