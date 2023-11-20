#include "cluster_directory_synchronizer.h"
#include "cluster_directory.h"
#include "config.h"
#include "private.h"

#include <yt/yt/ytlib/misc/synchronizer_detail.h>

#include <yt/yt/client/api/connection.h>
#include <yt/yt/client/api/client.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT::NHiveClient {

using namespace NConcurrency;
using namespace NApi;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TClusterDirectorySynchronizer
    : public IClusterDirectorySynchronizer
    , public TSynchronizerBase
{
public:
    TClusterDirectorySynchronizer(
        TClusterDirectorySynchronizerConfigPtr config,
        IConnectionPtr directoryConnection,
        TClusterDirectoryPtr clusterDirectory)
        : TSynchronizerBase(
            NRpc::TDispatcher::Get()->GetLightInvoker(),
            TPeriodicExecutorOptions{
                .Period = config->SyncPeriod,
            },
            HiveClientLogger)
        , Config_(std::move(config))
        , DirectoryConnection_(std::move(directoryConnection))
        , ClusterDirectory_(std::move(clusterDirectory))
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

    DEFINE_SIGNAL_OVERRIDE(void(const TError&), Synchronized);

private:
    const TClusterDirectorySynchronizerConfigPtr Config_;
    const TWeakPtr<IConnection> DirectoryConnection_;
    const TWeakPtr<TClusterDirectory> ClusterDirectory_;

    const TPeriodicExecutorPtr SyncExecutor_;

    void DoSync() override
    {
        try {
            NTracing::TNullTraceContextGuard nullTraceContext;

            auto connection = DirectoryConnection_.Lock();
            if (!connection) {
                THROW_ERROR_EXCEPTION("Directory connection is not available");
            }

            YT_LOG_DEBUG("Started synchronizing cluster directory");

            auto client = connection->CreateClient(TClientOptions::FromUser(NSecurityClient::RootUserName));

            TGetClusterMetaOptions options;
            options.PopulateClusterDirectory = true;
            options.ReadFrom = EMasterChannelKind::Cache;
            options.ExpireAfterSuccessfulUpdateTime = Config_->ExpireAfterSuccessfulUpdateTime;
            options.ExpireAfterFailedUpdateTime = Config_->ExpireAfterFailedUpdateTime;

            auto meta = WaitFor(client->GetClusterMeta(options))
                .ValueOrThrow();

            auto clusterDirectory = ClusterDirectory_.Lock();
            if (!clusterDirectory) {
                THROW_ERROR_EXCEPTION("Directory is not available");
            }

            clusterDirectory->UpdateDirectory(*meta.ClusterDirectory);

            YT_LOG_DEBUG("Finished synchronizing cluster directory");

            Synchronized_.Fire(TError());
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
    IConnectionPtr directoryConnection,
    TClusterDirectoryPtr clusterDirectory)
{
    return New<TClusterDirectorySynchronizer>(
        std::move(config),
        std::move(directoryConnection),
        std::move(clusterDirectory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
