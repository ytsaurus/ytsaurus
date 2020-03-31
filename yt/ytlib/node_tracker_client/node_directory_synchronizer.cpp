#include "node_directory_synchronizer.h"
#include "config.h"

#include <yt/client/api/connection.h>
#include <yt/client/api/client.h>

#include <yt/client/node_tracker_client/private.h>
#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/core/rpc/dispatcher.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler.h>

namespace NYT::NNodeTrackerClient {

using namespace NConcurrency;
using namespace NApi;
using namespace NYTree;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = NodeTrackerClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TNodeDirectorySynchronizer::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TNodeDirectorySynchronizerConfigPtr config,
        IConnectionPtr directoryConnection,
        TNodeDirectoryPtr nodeDirectory)
        : Config_(config)
        , Connection_(directoryConnection)
        , NodeDirectory_(nodeDirectory)
        , SyncExecutor_(New<TPeriodicExecutor>(
            NRpc::TDispatcher::Get()->GetHeavyInvoker(),
            BIND(&TImpl::OnSync, MakeWeak(this)),
            Config_->SyncPeriod))
    { }

    void Start()
    {
        SyncExecutor_->Start();
    }

    TFuture<void> Stop()
    {
        return SyncExecutor_->Stop();
    }

private:
    const TNodeDirectorySynchronizerConfigPtr Config_;
    const TWeakPtr<IConnection> Connection_;
    const TNodeDirectoryPtr NodeDirectory_;

    const TPeriodicExecutorPtr SyncExecutor_;

    void DoSync()
    {
        try {
            auto connection = Connection_.Lock();
            if (!connection) {
                return;
            }

            YT_LOG_DEBUG("Started synchronizing node directory");

            auto client = connection->CreateClient(TClientOptions(NSecurityClient::RootUserName));

            TGetClusterMetaOptions options;
            options.ReadFrom = EMasterChannelKind::Cache;
            options.PopulateNodeDirectory = true;
            options.ExpireAfterSuccessfulUpdateTime = Config_->ExpireAfterSuccessfulUpdateTime;
            options.ExpireAfterFailedUpdateTime = Config_->ExpireAfterFailedUpdateTime;

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

TNodeDirectorySynchronizer::TNodeDirectorySynchronizer(
    TNodeDirectorySynchronizerConfigPtr config,
    IConnectionPtr directoryConnection,
    TNodeDirectoryPtr nodeDirectory)
    : Impl_(New<TImpl>(
        config,
        directoryConnection,
        nodeDirectory))
{ }

TNodeDirectorySynchronizer::~TNodeDirectorySynchronizer() = default;

void TNodeDirectorySynchronizer::Start()
{
    Impl_->Start();
}

TFuture<void> TNodeDirectorySynchronizer::Stop()
{
    return Impl_->Stop();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
