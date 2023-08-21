#include "node_directory_synchronizer.h"
#include "config.h"

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/node_tracker_client/private.h>
#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NNodeTrackerClient {

using namespace NConcurrency;
using namespace NApi;
using namespace NYTree;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = NodeTrackerClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TNodeDirectorySynchronizer
    : public INodeDirectorySynchronizer
{
public:
    TNodeDirectorySynchronizer(
        const NApi::NNative::IConnectionPtr& directoryConnection,
        TNodeDirectoryPtr nodeDirectory)
        : Config_(directoryConnection->GetConfig()->NodeDirectorySynchronizer)
        , Connection_(directoryConnection)
        , NodeDirectory_(nodeDirectory)
        , SyncExecutor_(New<TPeriodicExecutor>(
            NRpc::TDispatcher::Get()->GetHeavyInvoker(),
            BIND(&TNodeDirectorySynchronizer::OnSync, MakeWeak(this)),
            Config_->SyncPeriod))
    { }

    void Start() const override
    {
        SyncExecutor_->Start();
    }

    TFuture<void> Stop() const override
    {
        return SyncExecutor_->Stop();
    }

private:
    const TNodeDirectorySynchronizerConfigPtr Config_;
    const TWeakPtr<NApi::IConnection> Connection_;
    const TNodeDirectoryPtr NodeDirectory_;

    const TPeriodicExecutorPtr SyncExecutor_;


    void DoSync()
    {
        try {
            NTracing::TNullTraceContextGuard nullTraceContext;

            auto connection = Connection_.Lock();
            if (!connection) {
                return;
            }

            YT_LOG_DEBUG("Started synchronizing node directory");

            auto client = connection->CreateClient(TClientOptions::FromUser(NSecurityClient::RootUserName));

            TGetClusterMetaOptions options;
            options.ReadFrom = EMasterChannelKind::Cache;
            options.PopulateNodeDirectory = true;
            options.ExpireAfterSuccessfulUpdateTime = Config_->ExpireAfterSuccessfulUpdateTime;
            options.ExpireAfterFailedUpdateTime = Config_->ExpireAfterFailedUpdateTime;
            options.CacheStickyGroupSize = Config_->CacheStickyGroupSize;

            // Request may block for prolonged periods of time;
            // handle cancelation requests (induced by stopping the executor) immediately.
            auto meta = WaitFor(client->GetClusterMeta(options).ToImmediatelyCancelable())
                .ValueOrThrow();

            NodeDirectory_->MergeFrom(*meta.NodeDirectory);

            YT_LOG_DEBUG("Finished synchronizing node directory");
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error synchronizing node directory")
                << ex;
        }
    }

    void OnSync()
    {
        try {
            DoSync();
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(TError(ex));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeDirectorySynchronizerPtr CreateNodeDirectorySynchronizer(
    const NApi::NNative::IConnectionPtr& directoryConnection,
    TNodeDirectoryPtr nodeDirectory)
{
    return New<TNodeDirectorySynchronizer>(
        directoryConnection,
        std::move(nodeDirectory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
