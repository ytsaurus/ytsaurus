#include "bootstrap.h"

#include "config.h"
#include "dynamic_config_manager.h"
#include "private.h"
#include "router.h"

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/server/lib/cypress_registrar/config.h>
#include <yt/yt/server/lib/cypress_registrar/cypress_registrar.h>

#include <yt/yt/server/lib/misc/address_helpers.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/library/profiling/solomon/public.h>

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

namespace NYT::NTcpProxy {

using namespace NAdmin;
using namespace NConcurrency;
using namespace NCoreDump;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NYTree;
using namespace NFusion;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = TcpProxyLogger;

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
        , ControlQueue_(New<TActionQueue>("Control"))
    {
        if (Config_->AbortOnUnrecognizedOptions) {
            AbortOnUnrecognizedOptions(Logger(), Config_);
        } else {
            WarnForUnrecognizedOptions(Logger(), Config_);
        }
    }

    TFuture<void> Run() override
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

    const TActionQueuePtr ControlQueue_;

    NApi::NNative::IConnectionPtr NativeConnection_;
    NApi::NNative::IClientPtr NativeRootClient_;
    NRpc::IAuthenticatorPtr NativeAuthenticator_;

    NConcurrency::IThreadPoolPollerPtr Poller_;
    NConcurrency::IThreadPoolPollerPtr Acceptor_;

    IRouterPtr Router_;

    NBus::IBusServerPtr BusServer_;
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
        BusServer_ = NBus::CreateBusServer(Config_->BusServer);
        HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

        NativeConnection_ = NApi::NNative::CreateConnection(Config_->ClusterConnection);
        NativeRootClient_ = NativeConnection_->CreateNativeClient({.User = NSecurityClient::RootUserName});
        NativeAuthenticator_ = NApi::NNative::CreateNativeAuthenticator(NativeConnection_);

        DynamicConfigManager_ = New<TDynamicConfigManager>(this);
        DynamicConfigManager_->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TBootstrap::OnDynamicConfigChanged, Unretained(this)));

        {
            TCypressRegistrarOptions options{
                .RootPath = TcpProxiesInstancesPath + NNet::BuildServiceAddress(
                    NNet::GetLocalHostName(),
                    Config_->RpcPort),
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

        if (Config_->ExposeConfigInOrchid) {
            SetNodeByYPath(
                OrchidRoot_,
                "/config",
                CreateVirtualNode(ConfigNode_));
            SetNodeByYPath(
                OrchidRoot_,
                "/dynamic_config_manager",
                CreateVirtualNode(DynamicConfigManager_->GetOrchidService()));
        }
        SetNodeByYPath(
            OrchidRoot_,
            "/role",
            ConvertTo<INodePtr>(Config_->Role));
        SetBuildAttributes(
            OrchidRoot_,
            "cypress_proxy");

        Poller_ = CreateThreadPoolPoller(
            DynamicConfigManager_->GetConfig()->PollerThreadCount,
            "TcpPoller");
        Acceptor_ = CreateThreadPoolPoller(
            DynamicConfigManager_->GetConfig()->AcceptorThreadCount,
            "TcpAcceptor");
        Router_ = CreateRouter(this);
    }

    void DoStart()
    {
        YT_LOG_INFO("Listening for HTTP requests (Port: %v)", Config_->MonitoringPort);
        HttpServer_->Start();

        Router_->Start();
    }

    void OnDynamicConfigChanged(
        const TProxyDynamicConfigPtr& /*oldConfig*/,
        const TProxyDynamicConfigPtr& newConfig)
    {
        TSingletonManager::Reconfigure(newConfig);

        Poller_->SetThreadCount(newConfig->PollerThreadCount);
        Acceptor_->SetThreadCount(newConfig->AcceptorThreadCount);
    }
};

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateTcpProxyBootstrap(
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

} // namespace NYT::NTcpProxy
