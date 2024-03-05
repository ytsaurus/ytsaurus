#include "cell_directory_synchronizer.h"

#include "private.h"

#include <yt/yt/server/lib/hive/config.h>

#include <yt/yt/server/lib/hydra/hydra_manager.h>

#include <yt/yt/server/master/cell_server/tamed_cell_manager.h>
#include <yt/yt/server/master/cell_server/cell_base.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/cell_directory_synchronizer.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NHiveServer {

using namespace NCellServer;
using namespace NObjectServer;
using namespace NHiveClient;
using namespace NObjectClient;
using namespace NHydra;
using namespace NConcurrency;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HiveServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TCellDirectorySynchronizer
    : public ICellDirectorySynchronizer
{
public:
    TCellDirectorySynchronizer(
        TCellDirectorySynchronizerConfigPtr config,
        ICellDirectoryPtr cellDirectory,
        ITamedCellManagerPtr cellManager,
        IHydraManagerPtr hydraManager,
        IInvokerPtr automatonInvoker)
        : Config_(std::move(config))
        , CellDirectory_(std::move(cellDirectory))
        , CellManager_(std::move(cellManager))
        , HydraManager_(std::move(hydraManager))
        , SyncExecutor_(New<TPeriodicExecutor>(
            std::move(automatonInvoker),
            BIND(&TCellDirectorySynchronizer::OnSync, MakeWeak(this)),
            Config_->SyncPeriod))
    { }

    void Start() override
    {
        // Refresh promise after Stop() if it has been called
        if (SyncPromise_.IsSet()) {
            SyncPromise_ = NewPromise<void>();
        }
        SyncExecutor_->Start();
    }

    void Stop() override
    {
        SyncExecutor_->Stop().Subscribe(BIND([this, this_ = MakeStrong(this)] (const TErrorOr<void>& /*result*/) {
            // Sync() sets promise after replacing only, so we never see it set when synchronizer is active
            if (!SyncPromise_.IsSet()) {
                SyncPromise_.Set(TError("Synchronizer is stopped"));
            }
        }));
    }

    TFuture<void> Sync() override
    {
        return SyncPromise_.ToFuture();
    }

private:
    const TCellDirectorySynchronizerConfigPtr Config_;
    const ICellDirectoryPtr CellDirectory_;
    const ITamedCellManagerPtr CellManager_;
    const IHydraManagerPtr HydraManager_;

    const TPeriodicExecutorPtr SyncExecutor_;

    TPromise<void> SyncPromise_ = NewPromise<void>();

    void OnSync()
    {
        auto promise = std::exchange(SyncPromise_, NewPromise<void>());
        if (!HydraManager_->IsActive()) {
            promise.Set(TError("Peer is not active"));
            return;
        }

        try {
            YT_LOG_DEBUG("Started synchronizing cell directory");

            THashMap<TCellId, int> idToVersion;
            for (const auto& info : CellDirectory_->GetRegisteredCells()) {
                YT_VERIFY(idToVersion.emplace(info.CellId, info.ConfigVersion).second);
            }

            WaitFor(HydraManager_->SyncWithLeader())
                .ThrowOnError();

            for (auto [cellId, cell] : CellManager_->Cells()) {
                if (!IsObjectAlive(cell)) {
                    continue;
                }

                auto it = idToVersion.find(cell->GetId());
                if (it == idToVersion.end() || it->second < cell->GetDescriptorConfigVersion()) {
                    CellDirectory_->ReconfigureCell(cell->GetDescriptor());
                }
                if (it != idToVersion.end()) {
                    idToVersion.erase(it);
                }
            }

            for (auto [cellId, version] : idToVersion) {
                if (IsCellType(TypeFromId(cellId)) && TypeFromId(cellId) != EObjectType::ChaosCell) {
                    CellDirectory_->UnregisterCell(cellId);
                }
            }

            YT_LOG_DEBUG("Finished synchronizing cell directory");
            promise.Set();
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Error synchronizing cell directory");
            promise.Set(ex);
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TCellDirectorySynchronizer)

////////////////////////////////////////////////////////////////////////////////

NHiveClient::ICellDirectorySynchronizerPtr CreateCellDirectorySynchronizer(
    TCellDirectorySynchronizerConfigPtr config,
    NHiveClient::ICellDirectoryPtr cellDirectory,
    NCellServer::ITamedCellManagerPtr cellManager,
    NHydra::IHydraManagerPtr hydraManager,
    IInvokerPtr automatonInvoker)
{
    return New<TCellDirectorySynchronizer>(
        std::move(config),
        std::move(cellDirectory),
        std::move(cellManager),
        std::move(hydraManager),
        std::move(automatonInvoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
