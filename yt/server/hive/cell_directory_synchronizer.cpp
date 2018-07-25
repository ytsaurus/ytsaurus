#include "cell_directory_synchronizer.h"
#include "config.h"
#include "private.h"

#include <yt/server/hydra/hydra_manager.h>

#include <yt/server/tablet_server/tablet_manager.h>
#include <yt/server/tablet_server/tablet_cell.h>

#include <yt/server/object_server/object.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/periodic_executor.h>

namespace NYT {
namespace NHiveServer {

using namespace NTabletServer;
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

class TCellDirectorySynchronizer::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TCellDirectorySynchronizerConfigPtr config,
        TCellDirectoryPtr cellDirectory,
        TTabletManagerPtr tabletManager,
        IHydraManagerPtr hydraManager,
        IInvokerPtr automatonInvoker)
        : Config_(std::move(config))
        , CellDirectory_(std::move(cellDirectory))
        , TabletManager_(std::move(tabletManager))
        , HydraManager_(std::move(hydraManager))
        , SyncExecutor_(New<TPeriodicExecutor>(
            std::move(automatonInvoker),
            BIND(&TImpl::OnSync, MakeWeak(this)),
            Config_->SyncPeriod))
    { }

    void Start()
    {
        SyncExecutor_->Start();
    }

    void Stop()
    {
        SyncExecutor_->Stop();
    }

private:
    const TCellDirectorySynchronizerConfigPtr Config_;
    const TCellDirectoryPtr CellDirectory_;
    const TTabletManagerPtr TabletManager_;
    const IHydraManagerPtr HydraManager_;

    const TPeriodicExecutorPtr SyncExecutor_;


    void OnSync()
    {
        if (!HydraManager_->IsActive()) {
            return;
        }

        try {
            LOG_DEBUG("Started synchronizing cell directory");

            THashMap<TCellId, int> idToVersion;
            for (const auto& info : CellDirectory_->GetRegisteredCells()) {
                YCHECK(idToVersion.emplace(info.CellId, info.ConfigVersion).second);
            }

            WaitFor(HydraManager_->SyncWithUpstream())
                .ThrowOnError();

            for (const auto& pair : TabletManager_->TabletCells()) {
                auto* cell = pair.second;
                if (!IsObjectAlive(cell)) {
                    continue;
                }

                auto it = idToVersion.find(cell->GetId());
                if (it == idToVersion.end() || it->second < cell->GetConfigVersion()) {
                    CellDirectory_->ReconfigureCell(cell->GetDescriptor());
                }
                if (it != idToVersion.end()) {
                    idToVersion.erase(it);
                }
            }

            for (const auto& pair : idToVersion) {
                const auto& id = pair.first;
                if (TypeFromId(id) == EObjectType::TabletCell) {
                    CellDirectory_->UnregisterCell(pair.first);
                }
            }

            LOG_DEBUG("Finished synchronizing cell directory");
        } catch (const std::exception& ex) {
            LOG_DEBUG(ex, "Error synchronizing cell directory");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TCellDirectorySynchronizer::TCellDirectorySynchronizer(
    TCellDirectorySynchronizerConfigPtr config,
    TCellDirectoryPtr cellDirectory,
    TTabletManagerPtr tabletManager,
    IHydraManagerPtr hydraManager,
    IInvokerPtr automatonInvoker)
    : Impl_(New<TImpl>(
        std::move(config),
        std::move(cellDirectory),
        std::move(tabletManager),
        std::move(hydraManager),
        std::move(automatonInvoker)))
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT
