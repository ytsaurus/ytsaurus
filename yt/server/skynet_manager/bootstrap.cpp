#include "bootstrap.h"

#include "private.h"
#include "announcer.h"
#include "skynet_service.h"

#include <yt/ytlib/monitoring/monitoring_manager.h>
#include <yt/ytlib/monitoring/http_integration.h>

#include <yt/client/api/rpc_proxy/connection.h>

#include <yt/client/api/connection.h>

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

#include <util/string/hex.h>

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
using namespace NApi::NRpcProxy;

////////////////////////////////////////////////////////////////////////////////

TString GetOrGeneratePeerId(const TString& filename)
{
    try {
        TFileInput file(filename);
        auto peerId = file.ReadAll();
        if (!peerId.empty()) {
            return peerId;
        }
    } catch (...) { }

    std::array<char, 8> entropy;
    TUnbufferedFileInput urandom("/dev/urandom");
    urandom.LoadOrFail(entropy.data(), entropy.size());
    
    auto peerId = to_lower(HexEncode(entropy.data(), entropy.size()));
    TFileOutput file(filename);
    file.Write(peerId);
    file.Finish();
    return peerId;
}

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TSkynetManagerConfigPtr config)
    : Config_(std::move(config))
{
    WarnForUnrecognizedOptions(SkynetManagerLogger, Config_);

    Poller_ = CreateThreadPoolPoller(Config_->IOPoolSize, "Poller");

    ActionQueue_ = New<TActionQueue>("SkynetApi");

    HttpListener_ = CreateListener(TNetworkAddress::CreateIPv6Any(Config_->Port), Poller_);
    HttpServer_ = CreateServer(Config_->HttpServer, HttpListener_, Poller_);

    HttpClient_ = CreateClient(Config_->HttpClient, Poller_);

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

    auto peerId = GetOrGeneratePeerId(Config_->PeerIdFile);
    PeerListener_ = CreateListener(TNetworkAddress::CreateIPv6Any(Config_->SkynetPort), Poller_);
    Announcer_ = New<TAnnouncer>(GetInvoker(), Poller_, Config_->Announcer, peerId, Config_->SkynetPort);

    for (const auto& clusterConfig : Config_->Clusters) {
        clusterConfig->LoadToken();

        auto apiConnection = CreateConnection(clusterConfig->Connection);

        TClientOptions options;
        options.Token = clusterConfig->OAuthToken;
        auto client = apiConnection->CreateClient(options);

        auto tables = New<TTables>(client, clusterConfig);

        auto clusterConnection = New<TClusterConnection>(clusterConfig, client, HttpClient_);

        Clusters_.push_back(clusterConnection);
    }

    SkynetService_ = New<TSkynetService>(this, peerId);
}

void TBootstrap::Run()
{
    std::vector<TFuture<void>> tasks;
    HttpServer_->Start();

    if (MonitoringHttpServer_) {
        MonitoringHttpServer_->Start();
    }

    SkynetService_->Start();
    Announcer_->Start();

    while (true) {
        Sleep(TDuration::Seconds(60));
    }
}

IInvokerPtr TBootstrap::GetInvoker() const
{
    return ActionQueue_->GetInvoker();
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

const TAnnouncerPtr& TBootstrap::GetAnnouncer() const
{
    return Announcer_;
}

const IListenerPtr& TBootstrap::GetPeerListener() const
{
    return PeerListener_;
}

const std::vector<TClusterConnectionPtr>& TBootstrap::GetClusters() const
{
    return Clusters_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
