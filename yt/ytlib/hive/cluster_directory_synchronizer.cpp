#include "cluster_directory_synchronizer.h"
#include "cluster_directory.h"
#include "config.h"
#include "private.h"

#include <yt/ytlib/api/connection.h>
#include <yt/ytlib/api/client.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/core/rpc/dispatcher.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/ytree/convert.h>

namespace NYT {
namespace NHiveClient {

using namespace NConcurrency;
using namespace NApi;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HiveClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TClusterDirectorySynchronizer::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TClusterDirectorySynchronizerConfigPtr config,
        IConnectionPtr directoryConnection,
        TClusterDirectoryPtr clusterDirectory)
        : Config_(config)
        , DirectoryClient_(directoryConnection->CreateClient(TClientOptions(NSecurityClient::RootUserName)))
        , ClusterDirectory_(clusterDirectory)
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
    const TClusterDirectorySynchronizerConfigPtr Config_;
    const IClientPtr DirectoryClient_;
    const TClusterDirectoryPtr ClusterDirectory_;

    const TPeriodicExecutorPtr SyncExecutor_;


    void DoSync()
    {
        try {
            LOG_DEBUG("Started updating cluster directory");

            TGetClusterMetaOptions options;
            options.PopulateClusterDirectory = true;
            auto asyncResult = DirectoryClient_->GetClusterMeta(options);
            auto result = WaitFor(asyncResult)
                .ValueOrThrow();

            ClusterDirectory_->UpdateDirectory(*result.ClusterDirectory);

            LOG_DEBUG("Finished updating cluster directory");
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error updating cluster directory")
                << ex;
        }
    }

    void OnSync()
    {
        try {
            DoSync();
        } catch (const std::exception& ex) {
            LOG_DEBUG(ex, "Cluster directory synchronization failed");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TClusterDirectorySynchronizer::TClusterDirectorySynchronizer(
    TClusterDirectorySynchronizerConfigPtr config,
    IConnectionPtr directoryConnection,
    TClusterDirectoryPtr clusterDirectory)
    : Impl_(New<TImpl>(
        config,
        directoryConnection,
        clusterDirectory))
{ }

TClusterDirectorySynchronizer::~TClusterDirectorySynchronizer() = default;

void TClusterDirectorySynchronizer::Start()
{
    Impl_->Start();
}

void TClusterDirectorySynchronizer::Stop()
{
    Impl_->Stop();
}

TFuture<void> TClusterDirectorySynchronizer::Sync()
{
    return Impl_->Sync();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveClient
} // namespace NYT
