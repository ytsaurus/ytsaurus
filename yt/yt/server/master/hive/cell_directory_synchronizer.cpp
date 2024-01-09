#include "cell_directory_synchronizer.h"

#include "private.h"

#include <yt/yt/server/lib/hive/config.h>

#include <yt/yt/server/lib/hydra/hydra_manager.h>

#include <yt/yt/server/master/cell_server/tamed_cell_manager.h>
#include <yt/yt/server/master/cell_server/cell_base.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

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
        SyncExecutor_->Start();
    }

    void Stop() override
    {
        YT_UNUSED_FUTURE(SyncExecutor_->Stop());
    }

private:
    const TCellDirectorySynchronizerConfigPtr Config_;
    const ICellDirectoryPtr CellDirectory_;
    const ITamedCellManagerPtr CellManager_;
    const IHydraManagerPtr HydraManager_;

    const TPeriodicExecutorPtr SyncExecutor_;


    void OnSync()
    {
        if (!HydraManager_->IsActive()) {
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
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Error synchronizing cell directory");
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TCellDirectorySynchronizer)

////////////////////////////////////////////////////////////////////////////////

ICellDirectorySynchronizerPtr CreateCellDirectorySynchronizer(
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
