#include "stdafx.h"
#include "cell_directory_synchronizer.h"
#include "config.h"

#include <core/concurrency/periodic_executor.h>
#include <core/concurrency/scheduler.h>

#include <core/rpc/dispatcher.h>

#include <ytlib/hive/private.h>
#include <ytlib/hive/cell_directory.h>

namespace NYT {
namespace NHive {

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

        auto result = WaitFor(CellDirectory_->Synchronize(channel));
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
