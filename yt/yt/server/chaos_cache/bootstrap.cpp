#include "bootstrap.h"

#include "config.h"
#include "private.h"
#include "dynamic_config_manager.h"

#include <yt/yt/library/coredumper/public.h>

#include <yt/yt/library/profiling/solomon/public.h>

#include <yt/yt/library/monitoring/http_integration.h>

#include <yt/yt/library/program/helpers.h>
#include <yt/yt/library/program/config.h>

#include <yt/yt/library/fusion/service_locator.h>

#include <yt/yt/server/lib/admin/admin_service.h>
#include <yt/yt/server/lib/admin/restart_service.h>

#include <yt/yt/server/lib/chaos_cache/chaos_cache_service.h>
#include <yt/yt/server/lib/chaos_cache/chaos_cache.h>
#include <yt/yt/server/lib/chaos_cache/config.h>

#include <yt/yt/server/lib/cypress_registrar/cypress_registrar.h>
#include <yt/yt/server/lib/cypress_registrar/config.h>

#include <yt/yt/server/lib/misc/address_helpers.h>
#include <yt/yt/server/lib/misc/restart_manager.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/client/logging/dynamic_table_log_writer.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT::NChaosCache {

using namespace NAdmin;
using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NYTree;
using namespace NNodeTrackerClient;
using namespace NFusion;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ChaosCacheLogger;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    TBootstrap(
        TChaosCacheBootstrapConfigPtr config,
        INodePtr configNode,
        IServiceLocatorPtr serviceLocator)
        : Config_(std::move(config))
        , ConfigNode_(std::move(configNode))
        , ServiceLocator_(std::move(serviceLocator))
        , ControlQueue_(New<TActionQueue>("Control"))
        , WorkerPool_(CreateThreadPool(
            Config_->ChaosCache->WorkerThreadCount,
            "ChaosCache"))
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

    const TChaosCacheBootstrapConfigPtr& GetConfig() const override
    {
        return Config_;
    }

    const NApi::NNative::IClientPtr& GetRootClient() const override
    {
        return RootClient_;
    }

    const IInvokerPtr& GetControlInvoker() const override
    {
        return ControlQueue_->GetInvoker();
    }

private:
    const TChaosCacheBootstrapConfigPtr Config_;
    const INodePtr ConfigNode_;
    const IServiceLocatorPtr ServiceLocator_;

    const TActionQueuePtr ControlQueue_;
    const NConcurrency::IThreadPoolPtr WorkerPool_;

    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;

    IMapNodePtr OrchidRoot_;
    IMonitoringManagerPtr MonitoringManager_;
    ICypressRegistrarPtr CypressRegistrar_;

    IConnectionPtr Connection_;

    NApi::NNative::IClientPtr RootClient_;

    NRpc::IAuthenticatorPtr NativeAuthenticator_;

    TDynamicConfigManagerPtr DynamicConfigManager_;

    TChaosCachePtr ChaosCache_;
    NRpc::IServicePtr ChaosCacheService_;

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

        NMonitoring::Initialize(
            HttpServer_,
            ServiceLocator_->GetServiceOrThrow<NProfiling::TSolomonExporterPtr>(),
            &MonitoringManager_,
            &OrchidRoot_);

        TConnectionOptions connectionOptions;
        connectionOptions.ChaosResidencyCacheMode = EChaosResidencyCacheType::MasterCache;
        Connection_ = NApi::NNative::CreateConnection(Config_->ClusterConnection, connectionOptions);
        Connection_->GetClusterDirectorySynchronizer()->Start();
        Connection_->GetMasterCellDirectorySynchronizer()->Start();

        RootClient_ = Connection_->CreateNativeClient(NApi::NNative::TClientOptions::Root());

        NLogging::GetDynamicTableLogWriterFactory()->SetClient(RootClient_);

        TCypressRegistrarOptions options{
            .RootPath = "//sys/chaos_caches/" + NNet::BuildServiceAddress(
                NNet::GetLocalHostName(),
                Config_->RpcPort),
            .OrchidRemoteAddresses = GetLocalAddresses(/*addresses*/ {}, Config_->RpcPort),
            .ExpireSelf = true,
        };

        CypressRegistrar_ = CreateCypressRegistrar(
            std::move(options),
            Config_->CypressRegistrar,
            RootClient_,
            GetControlInvoker());

        NativeAuthenticator_ = NApi::NNative::CreateNativeAuthenticator(Connection_);

        DynamicConfigManager_ = New<TDynamicConfigManager>(this);
        DynamicConfigManager_->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TBootstrap::OnDynamicConfigChanged, Unretained(this)));

        RpcServer_->RegisterService(CreateAdminService(
            GetControlInvoker(),
            ServiceLocator_->FindService<NCoreDump::ICoreDumperPtr>(),
            NativeAuthenticator_));

        auto restartManager = New<TRestartManager>(GetControlInvoker());
        RpcServer_->RegisterService(CreateRestartService(
            restartManager,
            GetControlInvoker(),
            Logger(),
            NativeAuthenticator_));

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

        RpcServer_->RegisterService(CreateOrchidService(
            OrchidRoot_,
            GetControlInvoker(),
            NativeAuthenticator_));

        ChaosCache_ = New<TChaosCache>(
            GetConfig()->ChaosCache,
            ChaosCacheProfiler().WithPrefix("/chaos_cache"),
            Logger());

        ChaosCacheService_ = CreateChaosCacheService(
            GetConfig()->ChaosCache,
            WorkerPool_->GetInvoker(),
            RootClient_,
            ChaosCache_,
            NativeAuthenticator_,
            Logger());

        RpcServer_->RegisterService(ChaosCacheService_);
    }

    void DoStart()
    {
        DynamicConfigManager_->Start();

        YT_LOG_INFO("Listening for HTTP requests (Port: %v)", Config_->MonitoringPort);
        HttpServer_->Start();

        YT_LOG_INFO("Listening for RPC requests (Port: %v)", Config_->RpcPort);
        RpcServer_->Start();

        CypressRegistrar_->Start({});
    }

    void OnDynamicConfigChanged(
        const TChaosCacheDynamicConfigPtr& /*oldConfig*/,
        const TChaosCacheDynamicConfigPtr& newConfig)
    {
        TSingletonManager::Reconfigure(newConfig);
    }
};

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateChaosCacheBootstrap(
    TChaosCacheBootstrapConfigPtr config,
    NYTree::INodePtr configNode,
    NFusion::IServiceLocatorPtr serviceLocator)
{
    return New<TBootstrap>(
        std::move(config),
        std::move(configNode),
        std::move(serviceLocator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosCache
