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
    : Config(std::move(config))
{
    WarnForUnrecognizedOptions(Logger, Config);

    Poller = CreateThreadPoolPoller(Config->IOPoolSize, "Poller");

    SkynetApiActionQueue = New<TActionQueue>("SkynetApi");
    SkynetApi = CreateShellSkynetApi(
        SkynetApiActionQueue->GetInvoker(),
        Config->SkynetPythonInterpreterPath,
        Config->SkynetMdsToolPath);

    HttpListener = CreateListener(TNetworkAddress::CreateIPv6Any(Config->Port), Poller);
    HttpServer = CreateServer(Config->HttpServer, HttpListener, Poller);

    HttpClient = CreateClient(Config->HttpClient, Poller);

    Manager = New<TSkynetManager>(this);
    ShareCache = New<TShareCache>(Manager, Config->TombstoneCache);

    MonitoringManager = New<TMonitoringManager>();
    MonitoringManager->Register(
        "/ref_counted",
        TRefCountedTracker::Get()->GetMonitoringProducer());

    OrchidRoot = GetEphemeralNodeFactory(true)->CreateMap();
    SetNodeByYPath(
        OrchidRoot,
        "/monitoring",
        CreateVirtualNode(MonitoringManager->GetService()));
    SetNodeByYPath(
        OrchidRoot,
        "/profiling",
        CreateVirtualNode(TProfileManager::Get()->GetService()));

    if (Config->MonitoringServer) {
        Config->MonitoringServer->Port = Config->MonitoringPort;
        MonitoringHttpServer = NHttp::CreateServer(
            Config->MonitoringServer);

        MonitoringHttpServer->AddHandler(
            "/orchid/",
            GetOrchidYPathHttpHandler(OrchidRoot));
    }

    for (const auto& clusterConfig : Config->Clusters) {
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
            Format("%s:%d", GetLocalHostName(), Config->Port),
            ShareCache);
        Clusters.emplace_back(std::move(cluster));
    }
}

const TCluster& TBootstrap::GetCluster(const TString& name) const
{
    for (const auto& cluster : Clusters) {
        if (cluster.Config->ClusterName == name) {
            return cluster;
        }
    }

    THROW_ERROR_EXCEPTION("Cluster %Qv not found", name);
}

void TBootstrap::Run()
{
    std::vector<TFuture<void>> tasks;
    HttpServer->Start();

    if (MonitoringHttpServer) {
        MonitoringHttpServer->Start();
    }

    tasks.emplace_back(BIND(&TBootstrap::DoRun, MakeStrong(this))
        .AsyncVia(SkynetApiActionQueue->GetInvoker())
        .Run());

    WaitFor(Combine(tasks))
        .ThrowOnError();
}

void TBootstrap::DoRun()
{
    while (true) {
        auto asyncSkynetList = SkynetApi->ListResources();
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
            .AsyncVia(SkynetApiActionQueue->GetInvoker())
            .Run();
        syncTasks.emplace_back(task);
    };

    for (const auto& cluster : Clusters) {
        spawn([manager=Manager, Logger=SkynetManagerLogger, cluster] {
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
                    manager->RunCypressPullIteration(cluster);
                } catch (const std::exception& ex) {
                    LOG_ERROR(ex, "Cypress pull crashed (Cluster: %v)", cluster.Config->ClusterName);
                }
            }
        });

        spawn([manager=Manager, Logger=SkynetManagerLogger, cluster] {
            while (true) {
                WaitFor(TDelayedExecutor::MakeDelayed(TDuration::Seconds(5)))
                    .ThrowOnError();

                try {
                    manager->RunTableScanIteration(cluster);
                } catch (const std::exception& ex) {
                    LOG_ERROR(ex, "Deleted tables scan crashed (Cluster: %v)", cluster.Config->ClusterName);
                }
            }
        });
    }

    WaitFor(Combine(syncTasks))
        .ThrowOnError();

    THROW_ERROR_EXCEPTION("Sync tasks stopped");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
