#include "stdafx.h"
#include "cell_directory_synchronizer.h"
#include "hive_service_proxy.h"
#include "config.h"
#include "private.h"

#include <core/concurrency/periodic_executor.h>
#include <core/concurrency/scheduler.h>

#include <core/rpc/dispatcher.h>

#include <ytlib/hive/cell_directory.h>

namespace NYT {
namespace NHive {

using namespace NConcurrency;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HiveLogger;

////////////////////////////////////////////////////////////////////////////////

class TCellDirectorySynchronizer::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TCellDirectorySynchronizerConfigPtr config,
        TCellDirectoryPtr cellDirectory,
        const TCellId& primaryCellId)
        : Config_(config)
        , CellDirectory_(cellDirectory)
        , PrimaryCellId_(primaryCellId)
        , SyncExecutor_(New<TPeriodicExecutor>(
            NRpc::TDispatcher::Get()->GetInvoker(),
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
    const TCellId PrimaryCellId_;

    const TPeriodicExecutorPtr SyncExecutor_;


    void OnSync()
    {
        auto channel = CellDirectory_->FindChannel(PrimaryCellId_, NHydra::EPeerKind::Leader);
        if (!channel)
            return;

        LOG_INFO("Requesting cells sync data");

        THiveServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(Config_->RpcTimeout);

        auto req = proxy.SyncCells();
        ToProto(req->mutable_known_cells(), CellDirectory_->GetRegisteredCells());

        auto rspOrError = WaitFor(req->Invoke());

        if (!rspOrError.IsOK()) {
            LOG_INFO(rspOrError, "Error requesting cells sync data");
            return;
        }

        const auto& rsp = rspOrError.Value();

        LOG_INFO("Cells sync data received");

        for (const auto& info : rsp->cells_to_unregister()) {
            auto cellId = FromProto<TCellId>(info.cell_id());
            YCHECK(cellId != NullCellId);
            if (CellDirectory_->UnregisterCell(cellId)) {
                LOG_DEBUG("Cell unregistered (CellId: %v)",
                    cellId);
            }
        }

        for (const auto& info : rsp->cells_to_reconfigure()) {
            auto descriptor = FromProto<NHive::TCellDescriptor>(info.cell_descriptor());
            if (CellDirectory_->ReconfigureCell(descriptor)) {
                LOG_DEBUG("Cell reconfigured (CellId: %v, ConfigVersion: %v)",
                    descriptor.CellId,
                    descriptor.ConfigVersion);
            }
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

TCellDirectorySynchronizer::TCellDirectorySynchronizer(
    TCellDirectorySynchronizerConfigPtr config,
    TCellDirectoryPtr cellDirectory,
    const TCellId& primaryCellId)
    : Impl_(New<TImpl>(
        config,
        cellDirectory,
        primaryCellId))
{ }

TCellDirectorySynchronizer::~TCellDirectorySynchronizer()
{ }

void TCellDirectorySynchronizer::Start()
{
    Impl_->Start();
}

void TCellDirectorySynchronizer::Stop()
{
    Impl_->Stop();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
