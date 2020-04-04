#include "bootstrap.h"
#include "config.h"

#include <yt/server/lib/admin/admin_service.h>

#include <yt/server/lib/misc/address_helpers.h>

#include <yt/server/lib/core_dump/core_dumper.h>

#include <yt/server/rpc_proxy/api_service.h>
#include <yt/server/rpc_proxy/discovery_service.h>
#include <yt/server/rpc_proxy/proxy_coordinator.h>
#include <yt/server/rpc_proxy/private.h>

#include <yt/ytlib/program/build_attributes.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/monitoring/http_integration.h>
#include <yt/ytlib/monitoring/monitoring_manager.h>

#include <yt/ytlib/orchid/orchid_service.h>

#include <yt/ytlib/auth/authentication_manager.h>

#include <yt/core/bus/server.h>

#include <yt/core/bus/tcp/config.h>
#include <yt/core/bus/tcp/server.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/thread_pool_poller.h>

#include <yt/core/net/address.h>

#include <yt/core/misc/core_dumper.h>
#include <yt/core/misc/ref_counted_tracker.h>
#include <yt/core/misc/ref_counted_tracker_statistics_producer.h>

#include <yt/core/ytalloc/statistics_producer.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/rpc/bus/channel.h>
#include <yt/core/rpc/bus/server.h>
#include <yt/core/rpc/response_keeper.h>
#include <yt/core/rpc/retrying_channel.h>
#include <yt/core/rpc/server.h>
#include <yt/core/rpc/authenticator.h>

#include <yt/core/rpc/grpc/server.h>
#include <yt/core/rpc/grpc/config.h>

#include <yt/core/http/server.h>

#include <yt/core/ytree/virtual.h>
#include <yt/core/ytree/ypath_client.h>

namespace NYT::NRpcProxy {

using namespace NAdmin;
using namespace NBus;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NProfiling;
using namespace NRpc;
using namespace NYTree;
using namespace NConcurrency;
using namespace NApi;
using namespace NYT::NRpcProxy;
using namespace NAuth;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = RpcProxyLogger;

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TCellProxyConfigPtr config, INodePtr configNode)
    : Config_(std::move(config))
    , ConfigNode_(std::move(configNode))
    , ControlQueue_(New<TActionQueue>("Control"))
    , WorkerPool_(New<TThreadPool>(Config_->WorkerThreadPoolSize, "Worker"))
    , HttpPoller_(CreateThreadPoolPoller(1, "HttpPoller"))
{
    if (Config_->AbortOnUnrecognizedOptions) {
        AbortOnUnrecognizedOptions(Logger, Config_);
    } else {
        WarnForUnrecognizedOptions(Logger, Config_);
    }
}

TBootstrap::~TBootstrap() = default;

void TBootstrap::Run()
{
    BIND(&TBootstrap::DoRun, this)
        .AsyncVia(ControlQueue_->GetInvoker())
        .Run()
        .Get()
        .ThrowOnError();

    Sleep(TDuration::Max());
}

void TBootstrap::DoRun()
{
    LocalAddresses_ = NYT::GetLocalAddresses(Config_->Addresses, Config_->RpcPort);

    YT_LOG_INFO("Starting proxy (LocalAddresses: %v, PrimaryMasterAddresses: %v)",
        GetValues(LocalAddresses_),
        Config_->ClusterConnection->PrimaryMaster->Addresses);

    NNative::TConnectionOptions connectionOptions;
    connectionOptions.RetryRequestQueueSizeLimitExceeded = true;
    NativeConnection_ = NApi::NNative::CreateConnection(Config_->ClusterConnection, connectionOptions);

    TClientOptions clientOptions;
    clientOptions.PinnedUser = NSecurityClient::RootUserName;
    NativeClient_ = NativeConnection_->CreateNativeClient(clientOptions);

    AuthenticationManager_ = New<TAuthenticationManager>(
        Config_,
        HttpPoller_,
        NativeClient_);
    ProxyCoordinator_ = CreateProxyCoordinator();

    BusServer_ = CreateTcpBusServer(Config_->BusServer);

    RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);

    Config_->MonitoringServer->Port = Config_->MonitoringPort;
    Config_->MonitoringServer->BindRetryCount = Config_->BusServer->BindRetryCount;
    Config_->MonitoringServer->BindRetryBackoff = Config_->BusServer->BindRetryBackoff;
    Config_->MonitoringServer->ServerName = "monitoring";
    HttpServer_ = NHttp::CreateServer(
        Config_->MonitoringServer);

    if (Config_->CoreDumper) {
        CoreDumper_ = NCoreDump::CreateCoreDumper(Config_->CoreDumper);
    }

    NYTree::IMapNodePtr orchidRoot;
    NMonitoring::Initialize(HttpServer_, &MonitoringManager_, &orchidRoot);

    SetNodeByYPath(
        orchidRoot,
        "/config",
        ConfigNode_);
    SetNodeByYPath(
        orchidRoot,
        "/coordinator",
        CreateVirtualNode(ProxyCoordinator_->CreateOrchidService()));
    SetBuildAttributes(orchidRoot, "proxy");

    RpcServer_->RegisterService(CreateOrchidService(
        orchidRoot,
        GetControlInvoker()));

    ApiService_ = CreateApiService(this);
    DiscoveryService_ = CreateDiscoveryService(this);

    RpcServer_->RegisterService(ApiService_);
    RpcServer_->RegisterService(DiscoveryService_);

    if (Config_->GrpcServer) {
        GrpcServer_ = NRpc::NGrpc::CreateServer(Config_->GrpcServer);
        GrpcServer_->RegisterService(ApiService_);
        GrpcServer_->RegisterService(DiscoveryService_);
    }

    YT_LOG_INFO("Listening for HTTP requests on port %v", Config_->MonitoringPort);
    HttpServer_->Start();

    YT_LOG_INFO("Listening for RPC requests on port %v", Config_->RpcPort);
    RpcServer_->Configure(Config_->RpcServer);
    RpcServer_->Start();

    if (Config_->GrpcServer) {
        const auto& addresses = Config_->GrpcServer->Addresses;
        YT_VERIFY(addresses.size() == 1);

        int port;
        NNet::ParseServiceAddress(addresses[0]->Address, nullptr, &port);

        YT_LOG_INFO("Listening for GRPC requests on port %v", port);
        GrpcServer_->Start();
    }
}

const TCellProxyConfigPtr& TBootstrap::GetConfig() const
{
    return Config_;
}

const IInvokerPtr& TBootstrap::GetControlInvoker() const
{
    return ControlQueue_->GetInvoker();
}

const IInvokerPtr& TBootstrap::GetWorkerInvoker() const
{
    return WorkerPool_->GetInvoker();
}

const NNative::IConnectionPtr& TBootstrap::GetNativeConnection() const
{
    return NativeConnection_;
}

const NNative::IClientPtr& TBootstrap::GetNativeClient() const
{
    return NativeClient_;
}

const IAuthenticatorPtr& TBootstrap::GetRpcAuthenticator() const
{
    return AuthenticationManager_->GetRpcAuthenticator();
}

const IProxyCoordinatorPtr& TBootstrap::GetProxyCoordinator() const
{
    return ProxyCoordinator_;
}

const NNodeTrackerClient::TAddressMap& TBootstrap::GetLocalAddresses() const
{
    return LocalAddresses_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
