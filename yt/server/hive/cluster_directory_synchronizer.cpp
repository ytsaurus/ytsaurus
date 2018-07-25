#include "cluster_directory_synchronizer.h"
#include "config.h"
#include "private.h"

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/ytree/ypath_client.h>

#include <yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/ytlib/hive/cluster_directory.h>

#include <yt/server/object_server/object_manager.h>

namespace NYT {
namespace NHiveServer {

using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HiveServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TClusterDirectorySynchronizer::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TDuration syncPeriod,
        const IInvokerPtr& invoker,
        const NObjectServer::TObjectManagerPtr& objectManager,
        const NHiveClient::TClusterDirectoryPtr& clusterDirectory)
        : SyncExecutor_(New<TPeriodicExecutor>(invoker, BIND(&TImpl::OnSync, MakeWeak(this)), syncPeriod))
        , ObjectManager_(objectManager)
        , ClusterDirectory_(clusterDirectory)
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

    TFuture<void> Sync(bool force)
    {
        auto guard = Guard(SpinLock_);
        if (Stopped_) {
            return MakeFuture(TError("Cluster directory synchronizer is stopped"));
        }
        DoStart(force);
        return SyncPromise_.ToFuture();
    }

    DEFINE_SIGNAL(void(const TError&), Synchronized);

private:
    const TPeriodicExecutorPtr SyncExecutor_;
    const NObjectServer::TObjectManagerPtr ObjectManager_;
    const NHiveClient::TClusterDirectoryPtr ClusterDirectory_;

    TSpinLock SpinLock_;
    bool Started_ = false;
    bool Stopped_= false;
    TPromise<void> SyncPromise_ = NewPromise<void>();

    void DoStart(bool force = false)
    {
        if (Started_) {
            if (force) {
                SyncExecutor_->ScheduleOutOfBand();
            }
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
            auto req = NObjectClient::TMasterYPathProxy::GetClusterMeta();
            req->set_populate_cluster_directory(true);
            auto res = WaitFor(ExecuteVerb(ObjectManager_->GetMasterProxy(), req))
                .ValueOrThrow();

            ClusterDirectory_->UpdateDirectory(res->cluster_directory());
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error updating cluster directory")
                << ex;
        }
    }

    void OnSync()
    {
        TError error;
        try {
            DoSync();
            Synchronized_.Fire(TError());
        } catch (const std::exception& ex) {
            error = TError(ex);
            Synchronized_.Fire(error);
            LOG_DEBUG(error);
        }

        auto guard = Guard(SpinLock_);
        auto syncPromise = NewPromise<void>();
        std::swap(syncPromise, SyncPromise_);
        guard.Release();
        syncPromise.Set(error);
    }
};

TClusterDirectorySynchronizer::TClusterDirectorySynchronizer(
    const TClusterDirectorySynchronizerConfigPtr& config,
    const IInvokerPtr& invoker,
    const NObjectServer::TObjectManagerPtr& objectManager,
    const NHiveClient::TClusterDirectoryPtr& clusterDirectory)
    : Impl_(New<TImpl>(config->SyncPeriod, invoker, objectManager, clusterDirectory))
{ }

void TClusterDirectorySynchronizer::Start()
{
    Impl_->Start();
}

void TClusterDirectorySynchronizer::Stop()
{
    Impl_->Stop();
}

TFuture<void> TClusterDirectorySynchronizer::Sync(bool force)
{
    return Impl_->Sync(force);
}

DELEGATE_SIGNAL(TClusterDirectorySynchronizer, void(const TError&), Synchronized, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT
