#include "bootstrap.h"

#include "skynet_manager.h"
#include "skynet_api.h"
#include "share_cache.h"
#include "cypress_sync.h"
#include "private.h"

#include <yt/ytlib/monitoring/monitoring_manager.h>
#include <yt/ytlib/monitoring/http_integration.h>

#include <yt/core/net/listener.h>
#include <yt/core/net/local_address.h>

#include <yt/core/http/server.h>
#include <yt/core/http/client.h>

#include <yt/core/concurrency/thread_pool_poller.h>
#include <yt/core/concurrency/poller.h>
#include <yt/core/concurrency/throughput_throttler.h>
#include <yt/core/concurrency/action_queue.h>

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/misc/ref_counted_tracker.h>
#include <yt/core/misc/ref_counted_tracker_statistics_producer.h>

#include <yt/core/ytree/virtual.h>

namespace NYT {
namespace NSkynetManager {

using namespace NYTree;
using namespace NMonitoring;
using namespace NConcurrency;
using namespace NProfiling;
using namespace NNet;
using namespace NHttp;
using namespace NLogging;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////


static const NLogging::TLogger Logger("Bootstrap");

void TCluster::ThrottleBackground() const
{
    WaitFor(BackgroundThrottler->Throttle(1))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TSkynetManagerConfigPtr config)
    : Config_(std::move(config))
{
    WarnForUnrecognizedOptions(Logger, Config_);

    Poller_ = CreateThreadPoolPoller(Config_->IOPoolSize, "Poller");

    SkynetApiActionQueue_ = New<TActionQueue>("SkynetApi");
    if (Config_->EnableSkyboneMds) {
        SkynetApi_ = CreateShellSkynetApi(
            SkynetApiActionQueue_->GetInvoker(),
            Config_->SkynetPythonInterpreterPath,
            Config_->SkynetMdsToolPath);
    } else {
        SkynetApi_ = CreateNullSkynetApi();
    }

    HttpListener_ = CreateListener(TNetworkAddress::CreateIPv6Any(Config_->Port), Poller_);
    HttpServer_ = CreateServer(Config_->HttpServer, HttpListener_, Poller_);

    HttpClient_ = CreateClient(Config_->HttpClient, Poller_);

    Manager_ = New<TSkynetManager>(this);
    ShareCache_ = New<TShareCache>(Manager_, Config_->TombstoneCache);

    MonitoringManager_ = New<TMonitoringManager>();
    MonitoringManager_->Register(
        "/ref_counted",
        CreateRefCountedTrackerStatisticsProducer());

    OrchidRoot_ = GetEphemeralNodeFactory(true)->CreateMap();
    SetNodeByYPath(
        OrchidRoot_,
        "/monitoring",
        CreateVirtualNode(MonitoringManager_->GetService()));
    SetNodeByYPath(
        OrchidRoot_,
        "/profiling",
        CreateVirtualNode(TProfileManager::Get()->GetService()));

    if (Config_->MonitoringServer) {
        Config_->MonitoringServer->Port = Config_->MonitoringPort;
        MonitoringHttpServer_ = NHttp::CreateServer(
            Config_->MonitoringServer);

        MonitoringHttpServer_->AddHandler(
            "/orchid/",
            GetOrchidYPathHttpHandler(OrchidRoot_));
    }

    for (const auto& clusterConfig : Config_->Clusters) {
        auto connection = CreateConnection(clusterConfig->Connection);

        TClientOptions options;
        options.User = clusterConfig->User;
        options.Token = clusterConfig->OAuthToken;
        auto client = connection->CreateClient(options);

        TCluster cluster;
        cluster.Config = clusterConfig;
        cluster.UserRequestThrottler = CreateReconfigurableThroughputThrottler(
            clusterConfig->UserRequestThrottler,
            SkynetManagerLogger,
            SkynetManagerProfiler);
        cluster.BackgroundThrottler = CreateReconfigurableThroughputThrottler(
            clusterConfig->BackgroundThrottler,
            SkynetManagerLogger,
            SkynetManagerProfiler);
        cluster.CypressSync = New<TCypressSync>(
            clusterConfig,
            std::move(client),
            Format("%s:%d", GetLocalHostName(), Config_->Port),
            ShareCache_);
        Clusters_.emplace_back(std::move(cluster));
    }
}

const TCluster& TBootstrap::GetCluster(const TString& name) const
{
    for (const auto& cluster : Clusters_) {
        if (cluster.Config->ClusterName == name) {
            return cluster;
        }
    }

    THROW_ERROR_EXCEPTION("Cluster %Qv not found", name);
}

void TBootstrap::Run()
{
    std::vector<TFuture<void>> tasks;
    HttpServer_->Start();

    if (MonitoringHttpServer_) {
        MonitoringHttpServer_->Start();
    }

    tasks.emplace_back(BIND(&TBootstrap::DoRun, MakeStrong(this))
        .AsyncVia(SkynetApiActionQueue_->GetInvoker())
        .Run());

    WaitFor(Combine(tasks))
        .ThrowOnError();
}

void TBootstrap::DoRun()
{
    while (true) {
        auto asyncSkynetList = SkynetApi_->ListResources();
        if (WaitFor(asyncSkynetList).IsOK()) {
            break;
        } else {
            LOG_ERROR(asyncSkynetList.Get(), "Skynet daemon is not available");
            WaitFor(TDelayedExecutor::MakeDelayed(TDuration::Seconds(5)))
                .ThrowOnError();
        }
    }

    std::vector<TFuture<void>> syncTasks;
    auto spawn = [&] (auto fn) {
        auto task = BIND(fn)
            .AsyncVia(SkynetApiActionQueue_->GetInvoker())
            .Run();
        syncTasks.emplace_back(task);
    };

    for (const auto& cluster : Clusters_) {
        spawn([manager = Manager_, Logger = SkynetManagerLogger, cluster] {
            while (true) {
                try {
                    cluster.CypressSync->CreateCypressNodesIfNeeded();
                    break;
                } catch (const std::exception& ex) {
                    LOG_ERROR(ex, "Cypress initialization failed (Cluster: %v)", cluster.Config->ClusterName);
                    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::Seconds(5)))
                        .ThrowOnError();
                }
            }

            while (true) {
                WaitFor(TDelayedExecutor::MakeDelayed(TDuration::Seconds(5)))
                    .ThrowOnError();

                try {
                    manager->RunCypressSyncIteration(cluster);
                } catch (const std::exception& ex) {
                    LOG_ERROR(ex, "Error synchronizing state with cypress (Cluster: %v)", cluster.Config->ClusterName);
                }
            }
        });

        spawn([manager = Manager_, Logger = SkynetManagerLogger, cluster] {
            while (true) {
                WaitFor(TDelayedExecutor::MakeDelayed(TDuration::Seconds(5)))
                    .ThrowOnError();

                try {
                    manager->RunTableScanIteration(cluster);
                } catch (const std::exception& ex) {
                    LOG_ERROR(ex, "Deleted tables scan failed (Cluster: %v)", cluster.Config->ClusterName);
                }
            }
        });
    }

    WaitFor(Combine(syncTasks))
        .ThrowOnError();

    THROW_ERROR_EXCEPTION("Sync tasks stopped");
}

size_t TBootstrap::GetClustersCount() const
{
    return Clusters_.size();
}

IInvokerPtr TBootstrap::GetInvoker() const
{
    return SkynetApiActionQueue_->GetInvoker();
}

const ISkynetApiPtr& TBootstrap::GetSkynetApi() const
{
    return SkynetApi_;
}

const TSkynetManagerConfigPtr& TBootstrap::GetConfig() const
{
    return Config_;
}

const NHttp::IServerPtr& TBootstrap::GetHttpServer() const
{
    return HttpServer_;
}

const NHttp::IClientPtr& TBootstrap::GetHttpClient() const
{
    return HttpClient_;
}

const TShareCachePtr& TBootstrap::GetShareCache() const
{
    return ShareCache_;
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
