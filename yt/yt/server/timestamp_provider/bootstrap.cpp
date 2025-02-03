#include "bootstrap.h"
#include "private.h"
#include "config.h"

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/library/coredumper/public.h>

#include <yt/yt/library/profiling/solomon/public.h>

#include <yt/yt/server/lib/transaction_server/timestamp_proxy_service.h>

#include <yt/yt/library/monitoring/http_integration.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/library/program/build_attributes.h>
#include <yt/yt/library/program/config.h>

#include <yt/yt/library/fusion/service_locator.h>

#include <yt/yt/client/transaction_client/config.h>
#include <yt/yt/client/transaction_client/remote_timestamp_provider.h>

#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/rpc/caching_channel_factory.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NTimestampProvider {

using namespace NAdmin;
using namespace NConcurrency;
using namespace NCoreDump;
using namespace NMonitoring;
using namespace NObjectClient;
using namespace NOrchid;
using namespace NTransactionClient;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NFusion;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = TimestampProviderLogger;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    TBootstrap(
        TTimestampProviderBootstrapConfigPtr config,
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

    TFuture<void> Run() final
    {
        return BIND(&TBootstrap::DoRun, MakeStrong(this))
            .AsyncVia(GetControlInvoker())
            .Run();
    }

private:
    const TTimestampProviderBootstrapConfigPtr Config_;
    const INodePtr ConfigNode_;
    const IServiceLocatorPtr ServiceLocator_;

    const TActionQueuePtr ControlQueue_;

    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;

    IMapNodePtr OrchidRoot_;
    TMonitoringManagerPtr MonitoringManager_;

    const IInvokerPtr& GetControlInvoker() const
    {
        return ControlQueue_->GetInvoker();
    }

    void DoRun()
    {
        DoInitialize();
        DoStart();
    }

    void DoInitialize()
    {
        BusServer_ = NBus::CreateBusServer(Config_->BusServer);
        RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);
        HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

        // TODO(gepardo): Pass authentication here.

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
        }
        SetBuildAttributes(
            OrchidRoot_,
            "timestamp_provider");

        auto channelFactory = NRpc::CreateCachingChannelFactory(
            NRpc::NBus::CreateTcpBusChannelFactory(Config_->BusClient));
        auto timestampProvider = CreateBatchingRemoteTimestampProvider(
            Config_->TimestampProvider,
            channelFactory,
            true);

        auto alienProviders = CreateAlienTimestampProvidersMap(
            Config_->AlienProviders,
            timestampProvider,
            Config_->ClockClusterTag,
            channelFactory);

        RpcServer_->RegisterService(CreateTimestampProxyService(
            std::move(timestampProvider),
            std::move(alienProviders),
            /*authenticator*/ nullptr));

        RpcServer_->RegisterService(CreateOrchidService(
            OrchidRoot_,
            GetControlInvoker(),
            /*authenticator*/ nullptr));
        RpcServer_->RegisterService(CreateAdminService(
            GetControlInvoker(),
            ServiceLocator_->FindService<NCoreDump::ICoreDumperPtr>(),
            /*authenticator*/ nullptr));
    }

    void DoStart()
    {
        YT_LOG_INFO("Listening for HTTP requests (Port: %v)", Config_->MonitoringPort);
        HttpServer_->Start();

        YT_LOG_INFO("Listening for RPC requests (Port: %v)", Config_->RpcPort);
        RpcServer_->Start();
    }
};

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateTimestampProviderBootstrap(
    TTimestampProviderBootstrapConfigPtr config,
    NYTree::INodePtr configNode,
    IServiceLocatorPtr serviceLocator)
{
    return New<TBootstrap>(
        std::move(config),
        std::move(configNode),
        std::move(serviceLocator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTimestampProvider
