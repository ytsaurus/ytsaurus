#include "bootstrap.h"

#include "config.h"
#include "dynamic_config_manager.h"
#include "private.h"
#include "server.h"

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/server/lib/cypress_registrar/config.h>
#include <yt/yt/server/lib/cypress_registrar/cypress_registrar.h>

#include <yt/yt/server/lib/misc/address_helpers.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/client/kafka/packet.h>

#include <yt/yt/client/logging/dynamic_table_log_writer.h>

#include <yt/yt/library/auth_server/authentication_manager.h>

#include <yt/yt/library/monitoring/http_integration.h>

#include <yt/yt/library/program/build_attributes.h>
#include <yt/yt/library/program/config.h>
#include <yt/yt/library/program/helpers.h>

#include <yt/yt/library/fusion/service_locator.h>

#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/rpc/caching_channel_factory.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT::NKafkaProxy {

using namespace NAdmin;
using namespace NApi;
using namespace NAuth;
using namespace NBus;
using namespace NConcurrency;
using namespace NKafka;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NYTree;
using namespace NFusion;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = KafkaProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    TBootstrap(
        TProxyBootstrapConfigPtr config,
        INodePtr configNode,
        IServiceLocatorPtr serviceLocator)
        : Config_(std::move(config))
        , ConfigNode_(std::move(configNode))
        , ServiceLocator_(std::move(serviceLocator))
    {
        if (Config_->AbortOnUnrecognizedOptions) {
            AbortOnUnrecognizedOptions(Logger(), Config_);
        } else {
            WarnForUnrecognizedOptions(Logger(), Config_);
        }
    }

    TFuture<void> Run() final
    {
        return BIND(&TBootstrap::DoRun, MakeStrong(this))
            .AsyncVia(GetControlInvoker())
            .Run();
    }

    const TProxyBootstrapConfigPtr& GetConfig() const override
    {
        return Config_;
    }

    const TDynamicConfigManagerPtr& GetDynamicConfigManager() const override
    {
        return DynamicConfigManager_;
    }

    const NRpc::IAuthenticatorPtr& GetNativeAuthenticator() const override
    {
        return NativeAuthenticator_;
    }

    const IInvokerPtr& GetControlInvoker() const override
    {
        return ControlQueue_->GetInvoker();
    }

    const NApi::NNative::IConnectionPtr& GetNativeConnection() const override
    {
        return NativeConnection_;
    }

    const NApi::NNative::IClientPtr& GetNativeClient() const override
    {
        return NativeRootClient_;
    }

    NApi::IClientPtr GetRootClient() const override
    {
        return NativeRootClient_;
    }

    NConcurrency::IPollerPtr GetPoller() const override
    {
        return Poller_;
    }

    NConcurrency::IPollerPtr GetAcceptor() const override
    {
        return Acceptor_;
    }

private:
    const TProxyBootstrapConfigPtr Config_;
    const INodePtr ConfigNode_;
    const IServiceLocatorPtr ServiceLocator_;

    const TActionQueuePtr ControlQueue_ = New<TActionQueue>("Control");

    NApi::NNative::IConnectionPtr NativeConnection_;
    NApi::NNative::IClientPtr NativeRootClient_;
    NRpc::IAuthenticatorPtr NativeAuthenticator_;

    NAuth::IAuthenticationManagerPtr AuthenticationManager_;

    NConcurrency::IThreadPoolPollerPtr Poller_;
    NConcurrency::IThreadPoolPollerPtr Acceptor_;

    IServerPtr Server_;

    NRpc::IServerPtr RpcServer_;

    NHttp::IServerPtr HttpServer_;

    IMapNodePtr OrchidRoot_;
    TMonitoringManagerPtr MonitoringManager_;
    ICypressRegistrarPtr CypressRegistrar_;

    TDynamicConfigManagerPtr DynamicConfigManager_;

    void DoRun()
    {
        DoInitialize();
        DoStart();
    }

    void DoInitialize()
    {
        HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

        NativeConnection_ = NApi::NNative::CreateConnection(Config_->ClusterConnection);

        SetupClusterConnectionDynamicConfigUpdate(
            NativeConnection_,
            NApi::NNative::EClusterConnectionDynamicConfigPolicy::FromClusterDirectory,
            /*staticClusterConnectionNode*/ nullptr,
            Logger());

        NativeRootClient_ = NativeConnection_->CreateNativeClient({.User = NSecurityClient::RootUserName});
        NativeAuthenticator_ = NApi::NNative::CreateNativeAuthenticator(NativeConnection_);

        NLogging::GetDynamicTableLogWriterFactory()->SetClient(NativeRootClient_);

        DynamicConfigManager_ = New<TDynamicConfigManager>(this);
        DynamicConfigManager_->SubscribeConfigChanged(BIND(&TBootstrap::OnDynamicConfigChanged, Unretained(this)));

        {
            TCypressRegistrarOptions options{
                .RootPath = Format("%v/%v", KafkaProxiesInstancesPath, NNet::BuildServiceAddress(
                    NNet::GetLocalHostName(),
                    Config_->Port)),
                .OrchidRemoteAddresses = GetLocalAddresses(/*addresses*/ {}, Config_->RpcPort),
                .ExpireSelf = true,
            };
            CypressRegistrar_ = CreateCypressRegistrar(
                std::move(options),
                Config_->CypressRegistrar,
                NativeRootClient_,
                GetControlInvoker());
        }

        NMonitoring::Initialize(
            HttpServer_,
            ServiceLocator_->GetServiceOrThrow<NProfiling::TSolomonExporterPtr>(),
            &MonitoringManager_,
            &OrchidRoot_);

        SetNodeByYPath(
            OrchidRoot_,
            "/config",
            CreateVirtualNode(ConfigNode_));
        SetNodeByYPath(
            OrchidRoot_,
            "/dynamic_config_manager",
            CreateVirtualNode(DynamicConfigManager_->GetOrchidService()));
        SetBuildAttributes(
            OrchidRoot_,
            "cypress_proxy");

        Poller_ = CreateThreadPoolPoller(
            DynamicConfigManager_->GetConfig()->PollerThreadCount,
            "KafkaPoller");

        Acceptor_ = CreateThreadPoolPoller(
            DynamicConfigManager_->GetConfig()->AcceptorThreadCount,
            "KafkaAcceptor");

        AuthenticationManager_ = CreateAuthenticationManager(
            Config_->Auth,
            Poller_,
            NativeRootClient_);

        Server_ = CreateServer(
            Config_,
            NativeConnection_,
            AuthenticationManager_,
            Poller_,
            Acceptor_);

        RpcServer_ = NRpc::NBus::CreateBusServer(CreateBusServer(Config_->BusServer));

        RpcServer_->RegisterService(CreateOrchidService(
            OrchidRoot_,
            GetControlInvoker(),
            /*authenticator*/ nullptr));
    }

    void DoStart()
    {
        DynamicConfigManager_->Start();

        YT_LOG_INFO("Listening for HTTP requests (Port: %v)", Config_->MonitoringPort);
        HttpServer_->Start();

        YT_LOG_INFO("Listening for Kafka requests (Port: %v)", Config_->Port);
        Server_->Start();

        RpcServer_->Start();

        NativeConnection_->GetClusterDirectorySynchronizer()->Start();

        CypressRegistrar_->Start();
    }

    void OnDynamicConfigChanged(
        const TProxyDynamicConfigPtr& /*oldConfig*/,
        const TProxyDynamicConfigPtr& newConfig)
    {
        TSingletonManager::Reconfigure(newConfig);

        Poller_->SetThreadCount(newConfig->PollerThreadCount);
        Acceptor_->SetThreadCount(newConfig->AcceptorThreadCount);

        Server_->OnDynamicConfigChanged(newConfig);
    }
};

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateKafkaProxyBootstrap(
    TProxyBootstrapConfigPtr config,
    INodePtr configNode,
    IServiceLocatorPtr serviceLocator)
{
    return New<TBootstrap>(
        std::move(config),
        std::move(configNode),
        std::move(serviceLocator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy
