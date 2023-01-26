#include "bootstrap.h"
#include "private.h"
#include "config.h"

#include <yt/yt/server/lib/discovery_server/discovery_server.h>

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/library/program/build_attributes.h>

#include <yt/yt/library/monitoring/http_integration.h>

#include <yt/yt/library/coredumper/public.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/bus/server.h>
#include <yt/yt/core/http/config.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/net/local_address.h>
#include <yt/yt/core/net/address.h>

#include <yt/yt/core/rpc/bus/server.h>
#include <yt/yt/core/rpc/bus/channel.h>

#include <yt/yt/core/rpc/authenticator.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/caching_channel_factory.h>

#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <cstdlib>
#include <ctime>

namespace NYT::NClusterDiscoveryServer {

using namespace NAdmin;
using namespace NConcurrency;
using namespace NMonitoring;
using namespace NNet;
using namespace NOrchid;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ClusterDiscoveryServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    explicit TBootstrap(TClusterDiscoveryServerConfigPtr config)
        : Config_(std::move(config))

    {
        if (Config_->AbortOnUnrecognizedOptions) {
            AbortOnUnrecognizedOptions(Logger, Config_);
        } else {
            WarnForUnrecognizedOptions(Logger, Config_);
        }
    }

    void Initialize() override
    {
        ControlQueue_ = New<TActionQueue>("Control");
        WorkerPool_ = CreateThreadPool(Config_->WorkerThreadPoolSize, "Worker");

        BIND(&TBootstrap::DoInitialize, this)
            .AsyncVia(GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();
    }

    void Run() override
    {
        BIND(&TBootstrap::DoRun, this)
            .AsyncVia(GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();
        Sleep(TDuration::Max());
    }

private:
    const TClusterDiscoveryServerConfigPtr Config_;

    TActionQueuePtr ControlQueue_;
    IThreadPoolPtr WorkerPool_;

    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;

    NRpc::IChannelFactoryPtr ChannelFactory_;

    NCoreDump::ICoreDumperPtr CoreDumper_;

    NDiscoveryServer::IDiscoveryServerPtr DiscoveryServer_;

    IMapNodePtr OrchidRoot_;
    TMonitoringManagerPtr MonitoringManager_;


    const IInvokerPtr& GetControlInvoker()
    {
        return ControlQueue_->GetInvoker();
    }

    const IInvokerPtr& GetWorkerInvoker()
    {
        return WorkerPool_->GetInvoker();
    }

    void DoInitialize()
    {
        BusServer_ = NBus::CreateBusServer(Config_->BusServer);
        RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);
        HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

        ChannelFactory_ = CreateCachingChannelFactory(NRpc::NBus::CreateTcpBusChannelFactory(Config_->BusClient));

        if (Config_->CoreDumper) {
            CoreDumper_ = NCoreDump::CreateCoreDumper(Config_->CoreDumper);
        }

        auto localAddress = BuildServiceAddress(GetLocalHostName(), Config_->RpcPort);

        // TODO(gepardo): Pass authenticator here instead of nullptr.

        DiscoveryServer_ = CreateDiscoveryServer(
            RpcServer_,
            localAddress,
            Config_->DiscoveryServer,
            ChannelFactory_,
            GetWorkerInvoker(),
            GetWorkerInvoker(),
            /*authenticator*/ nullptr);
        DiscoveryServer_->Initialize();

        NMonitoring::Initialize(
            HttpServer_,
            Config_->SolomonExporter,
            &MonitoringManager_,
            &OrchidRoot_);

        SetNodeByYPath(
            OrchidRoot_,
            "/config",
            ConvertTo<INodePtr>(Config_));
        SetNodeByYPath(
            OrchidRoot_,
            "/discovery_server",
            CreateVirtualNode(DiscoveryServer_->GetYPathService()));
        SetBuildAttributes(
            OrchidRoot_,
            "discovery_server");

        RpcServer_->RegisterService(CreateOrchidService(
            OrchidRoot_,
            GetControlInvoker(),
            /*authenticator*/ nullptr));
        RpcServer_->RegisterService(CreateAdminService(
            GetControlInvoker(),
            CoreDumper_,
            /*authenticator*/ nullptr));
    }

    void DoRun()
    {
        YT_LOG_INFO("Listening for HTTP requests (Port: %v)", Config_->MonitoringPort);
        HttpServer_->Start();

        YT_LOG_INFO("Listening for RPC requests (Port: %v)", Config_->RpcPort);
        RpcServer_->Start();
    }
};

std::unique_ptr<IBootstrap> CreateBootstrap(TClusterDiscoveryServerConfigPtr config)
{
    return std::make_unique<TBootstrap>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterDiscoveryServer
