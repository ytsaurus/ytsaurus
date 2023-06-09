#include "cluster_directory_synchronizer.h"
#include "private.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/server/lib/hive/config.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>
#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/rpc_helpers.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/object_server/object_manager.h>
#include <yt/yt/server/master/object_server/object_proxy.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NHiveServer {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NYTree;
using namespace NApi;
using namespace NApi::NNative;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HiveServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TClusterDirectorySynchronizer
    : public IClusterDirectorySynchronizer
{
public:
    TClusterDirectorySynchronizer(
        TClusterDirectorySynchronizerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap,
        NHiveClient::TClusterDirectoryPtr clusterDirectory)
        : Bootstrap_(bootstrap)
        , SyncExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::ClusterDirectorySynchronizer),
            BIND(&TClusterDirectorySynchronizer::OnSync, MakeWeak(this)),
            config->SyncPeriod))
        , ObjectManager_(Bootstrap_->GetObjectManager())
        , MulticellManager_(Bootstrap_->GetMulticellManager())
        , CellTag_(MulticellManager_->GetPrimaryCellTag())
        , ClusterDirectory_(std::move(clusterDirectory))
        , Config_(std::move(config))
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

    TFuture<void> Sync(bool force) override
    {
        auto guard = Guard(SpinLock_);
        if (Stopped_) {
            return MakeFuture(TError("Cluster directory synchronizer is stopped"));
        }
        DoStart(force);
        return SyncPromise_.ToFuture();
    }

    void Reconfigure(const TClusterDirectorySynchronizerConfigPtr& config) override
    {
        Config_.Store(config);
        SyncExecutor_->SetPeriod(config->SyncPeriod);
    }

    DEFINE_SIGNAL_OVERRIDE(void(const TError&), Synchronized);

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    const TPeriodicExecutorPtr SyncExecutor_;
    const NObjectServer::IObjectManagerPtr ObjectManager_;
    const NCellMaster::IMulticellManagerPtr MulticellManager_;
    const NObjectClient::TCellTag CellTag_;
    const NHiveClient::TClusterDirectoryPtr ClusterDirectory_;
    TAtomicIntrusivePtr<TClusterDirectorySynchronizerConfig> Config_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
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
            YT_LOG_DEBUG("Started synchronizing cluster directory");

            auto config = Config_.Acquire();
            TMasterReadOptions options{
                .ReadFrom = EMasterChannelKind::Cache,
                .ExpireAfterSuccessfulUpdateTime = config->ExpireAfterSuccessfulUpdateTime,
                .ExpireAfterFailedUpdateTime = config->ExpireAfterFailedUpdateTime,
                .CacheStickyGroupSize = 1
            };

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            if (multicellManager->IsSecondaryMaster()) {
                const auto& connection = Bootstrap_->GetClusterConnection();
                auto proxy = CreateObjectServiceReadProxy(
                    Bootstrap_->GetRootClient(),
                    EMasterChannelKind::Follower,
                    CellTag_,
                    connection->GetStickyGroupSizeCache());

                auto batchReq = proxy.ExecuteBatch();
                batchReq->SetSuppressTransactionCoordinatorSync(true);
                SetBalancingHeader(batchReq, connection, options);

                auto req = NObjectClient::TMasterYPathProxy::GetClusterMeta();
                req->set_populate_cluster_directory(true);
                SetCachingHeader(req, connection, options);
                batchReq->AddRequest(req);

                auto batchRsp = WaitFor(batchReq->Invoke())
                    .ValueOrThrow();
                auto rsp = batchRsp->GetResponse<NObjectClient::TMasterYPathProxy::TRspGetClusterMeta>(0)
                    .ValueOrThrow();

                ClusterDirectory_->UpdateDirectory(rsp->cluster_directory());
            } else {
                auto req = NObjectClient::TMasterYPathProxy::GetClusterMeta();
                req->set_populate_cluster_directory(true);

                auto rsp = WaitFor(ExecuteVerb(ObjectManager_->GetMasterProxy(), req))
                    .ValueOrThrow();

                ClusterDirectory_->UpdateDirectory(rsp->cluster_directory());
            }

            YT_LOG_DEBUG("Finished synchronizing cluster directory");
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error synchronizing cluster directory")
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
            YT_LOG_DEBUG(error);
        }

        auto guard = Guard(SpinLock_);
        auto syncPromise = NewPromise<void>();
        std::swap(syncPromise, SyncPromise_);
        guard.Release();
        syncPromise.Set(error);
    }
};

IClusterDirectorySynchronizerPtr CreateClusterDirectorySynchronizer(
    TClusterDirectorySynchronizerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap,
    NHiveClient::TClusterDirectoryPtr clusterDirectory)
{
    return New<TClusterDirectorySynchronizer>(std::move(config), bootstrap, std::move(clusterDirectory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
