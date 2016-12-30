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

static const auto& Logger = HiveClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TCellDirectorySynchronizer::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TCellDirectorySynchronizerConfigPtr config,
        TCellDirectoryPtr cellDirectory,
        const TCellId& primaryCellId)
        : Config_(std::move(config))
        , CellDirectory_(std::move(cellDirectory))
        , PrimaryCellId_(primaryCellId)
        , SyncExecutor_(New<TPeriodicExecutor>(
            NRpc::TDispatcher::Get()->GetLightInvoker(),
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

    TFuture<void> Sync()
    {
        return BIND(&TImpl::DoSync, MakeStrong(this))
            .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
            .Run();
    }

private:
    const TCellDirectorySynchronizerConfigPtr Config_;
    const TCellDirectoryPtr CellDirectory_;
    const TCellId PrimaryCellId_;

    const TPeriodicExecutorPtr SyncExecutor_;


    void DoSync()
    {
        try {
            LOG_DEBUG("Started synchronizing cell directory");

            auto channel = CellDirectory_->GetChannelOrThrow(PrimaryCellId_, NHydra::EPeerKind::Leader);
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
        try {
            DoSync();
        } catch (const std::exception& ex) {
            LOG_DEBUG(TError(ex));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TCellDirectorySynchronizer::TCellDirectorySynchronizer(
    TCellDirectorySynchronizerConfigPtr config,
    TCellDirectoryPtr cellDirectory,
    const TCellId& primaryCellId)
    : Impl_(New<TImpl>(
        std::move(config),
        std::move(cellDirectory),
        primaryCellId))
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
