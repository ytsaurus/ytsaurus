#include "cluster_directory_synchronizer.h"
#include "private.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/ytlib/misc/synchronizer_detail.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>
#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/rpc_helpers.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/server/lib/hive/config.h>

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

class TClusterDirectorySynchronizer
    : public IClusterDirectorySynchronizer
    , public TSynchronizerBase
{
public:
    TClusterDirectorySynchronizer(
        TClusterDirectorySynchronizerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap,
        NHiveClient::TClusterDirectoryPtr clusterDirectory)
        : TSynchronizerBase(
            bootstrap->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::ClusterDirectorySynchronizer),
            TPeriodicExecutorOptions{
                .Period = config->SyncPeriod,
            },
            HiveServerLogger)
        , Bootstrap_(bootstrap)
        , ObjectManager_(Bootstrap_->GetObjectManager())
        , MulticellManager_(Bootstrap_->GetMulticellManager())
        , CellTag_(MulticellManager_->GetPrimaryCellTag())
        , ClusterDirectory_(std::move(clusterDirectory))
        , Config_(std::move(config))
    { }

    void Start() override
    {
        TSynchronizerBase::Start();
    }

    void Stop() override
    {
        TSynchronizerBase::Stop();
    }

    TFuture<void> Sync(bool immediately) override
    {
        return TSynchronizerBase::Sync(immediately);
    }

    void Reconfigure(const TClusterDirectorySynchronizerConfigPtr& config) override
    {
        Config_.Store(config);
        SyncExecutor_->SetPeriod(config->SyncPeriod);
    }

    DEFINE_SIGNAL_OVERRIDE(void(const TError&), Synchronized);

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    const NObjectServer::IObjectManagerPtr ObjectManager_;
    const NCellMaster::IMulticellManagerPtr MulticellManager_;
    const NObjectClient::TCellTag CellTag_;
    const NHiveClient::TClusterDirectoryPtr ClusterDirectory_;
    TAtomicIntrusivePtr<TClusterDirectorySynchronizerConfig> Config_;

    void DoSync() override
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

            Synchronized_.Fire(TError());
            YT_LOG_DEBUG("Finished synchronizing cluster directory");
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            Synchronized_.Fire(error);
            THROW_ERROR_EXCEPTION("Error synchronizing cluster directory")
                << error;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IClusterDirectorySynchronizerPtr CreateClusterDirectorySynchronizer(
    TClusterDirectorySynchronizerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap,
    NHiveClient::TClusterDirectoryPtr clusterDirectory)
{
    return New<TClusterDirectorySynchronizer>(
        std::move(config),
        bootstrap,
        std::move(clusterDirectory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
