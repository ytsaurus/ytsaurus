#include "bootstrap.h"

#include "dynamic_config_manager.h"
#include "private.h"
#include "config.h"
#include "object_service.h"
#include "sequoia_service.h"
#include "user_directory.h"
#include "user_directory_synchronizer.h"
#include "response_keeper.h"

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/server/lib/cypress_registrar/cypress_registrar.h>

#include <yt/yt/server/lib/misc/address_helpers.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/distributed_throttler/distributed_throttler.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/ytlib/program/native_singletons.h>

#include <yt/yt/ytlib/sequoia_client/lazy_client.h>

#include <yt/yt/client/logging/dynamic_table_log_writer.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/library/monitoring/http_integration.h>

#include <yt/yt/library/program/build_attributes.h>
#include <yt/yt/library/program/config.h>

#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/rpc/caching_channel_factory.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NCypressProxy {

using namespace NAdmin;
using namespace NConcurrency;
using namespace NCoreDump;
using namespace NDistributedThrottler;
using namespace NMonitoring;
using namespace NNet;
using namespace NOrchid;
using namespace NSequoiaClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = CypressProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    explicit TBootstrap(TCypressProxyConfigPtr config)
        : Config_(std::move(config))
    {
        if (Config_->AbortOnUnrecognizedOptions) {
            AbortOnUnrecognizedOptions(Logger(), Config_);
        } else {
            WarnForUnrecognizedOptions(Logger(), Config_);
        }
    }

    void Initialize() override
    {
        ControlQueue_ = New<TActionQueue>("Control");

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

    const TCypressProxyConfigPtr& GetConfig() const override
    {
        return Config_;
    }

    const TDynamicConfigManagerPtr& GetDynamicConfigManager() const override
    {
        return DynamicConfigManager_;
    }

    const TUserDirectoryPtr& GetUserDirectory() const override
    {
        return UserDirectory_;
    }

    const IUserDirectorySynchronizerPtr& GetUserDirectorySynchronizer() const override
    {
        return UserDirectorySynchronizer_;
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

    const NApi::NNative::IClientPtr& GetNativeRootClient() const override
    {
        return NativeRootClient_;
    }

    ISequoiaClientPtr GetSequoiaClient() const override
    {
        return SequoiaClient_;
    }

    const NApi::NNative::IConnectionPtr& GetGroundConnection() const override
    {
        return GroundConnection_;
    }

    const NApi::NNative::IClientPtr& GetGroundRootClient() const override
    {
        return GroundRootClient_;
    }

    NApi::IClientPtr GetRootClient() const override
    {
        return NativeRootClient_;
    }

    const ISequoiaServicePtr& GetSequoiaService() const override
    {
        return SequoiaService_;
    }

    const ISequoiaResponseKeeperPtr& GetResponseKeeper() const override
    {
        return ResponseKeeper_;
    }

    IDistributedThrottlerFactoryPtr CreateDistributedThrottlerFactory(
        TDistributedThrottlerConfigPtr config,
        IInvokerPtr invoker,
        const std::string& groupId,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler) const override
    {
        auto selfAddress = BuildServiceAddress(GetLocalHostName(), Config_->RpcPort);
        return NDistributedThrottler::CreateDistributedThrottlerFactory(
            std::move(config),
            NativeConnection_->GetChannelFactory(),
            NativeConnection_,
            std::move(invoker),
            // TODO(babenko): migrate to std::string
            TString(groupId),
            selfAddress,
            RpcServer_,
            std::move(selfAddress),
            std::move(logger),
            NativeAuthenticator_,
            profiler);
    }

private:
    const TCypressProxyConfigPtr Config_;

    NApi::NNative::IConnectionPtr NativeConnection_;
    NApi::NNative::IClientPtr NativeRootClient_;
    NRpc::IAuthenticatorPtr NativeAuthenticator_;

    NApi::NNative::IConnectionPtr GroundConnection_;
    NApi::NNative::IClientPtr GroundRootClient_;

    ILazySequoiaClientPtr SequoiaClient_;

    ISequoiaServicePtr SequoiaService_;

    ISequoiaResponseKeeperPtr ResponseKeeper_;

    TActionQueuePtr ControlQueue_;

    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;

    IObjectServicePtr ObjectService_;

    IMapNodePtr OrchidRoot_;
    TMonitoringManagerPtr MonitoringManager_;
    ICypressRegistrarPtr CypressRegistrar_;

    NCoreDump::ICoreDumperPtr CoreDumper_;

    TDynamicConfigManagerPtr DynamicConfigManager_;

    TUserDirectoryPtr UserDirectory_;
    IUserDirectorySynchronizerPtr UserDirectorySynchronizer_;

    void DoInitialize()
    {
        BusServer_ = NBus::CreateBusServer(Config_->BusServer);
        RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);
        HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

        if (Config_->CoreDumper) {
            CoreDumper_ = CreateCoreDumper(Config_->CoreDumper);
        }

        NativeConnection_ = NApi::NNative::CreateConnection(Config_->ClusterConnection);
        NativeRootClient_ = NativeConnection_->CreateNativeClient({.User = NSecurityClient::RootUserName});
        NativeAuthenticator_ = NApi::NNative::CreateNativeAuthenticator(NativeConnection_);

        NLogging::GetDynamicTableLogWriterFactory()->SetClient(NativeRootClient_);

        SequoiaClient_ = CreateLazySequoiaClient(NativeRootClient_, Logger());

        // If Sequoia is local it's safe to create the client right now.
        const auto& groundClusterName = Config_->ClusterConnection->Dynamic->SequoiaConnection->GroundClusterName;
        if (!groundClusterName) {
            SequoiaClient_->SetGroundClient(NativeRootClient_);
        }

        DynamicConfigManager_ = New<TDynamicConfigManager>(this);
        DynamicConfigManager_->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TBootstrap::OnDynamicConfigChanged, Unretained(this)));

        UserDirectory_ = New<TUserDirectory>();
        UserDirectorySynchronizer_ = CreateUserDirectorySynchronizer(
            Config_->UserDirectorySynchronizer,
            GetRootClient(),
            UserDirectory_,
            GetControlInvoker());

        {
            TCypressRegistrarOptions options{
                .RootPath = NYPath::YPathJoin(Config_->RootPath, BuildServiceAddress(
                    GetLocalHostName(),
                    Config_->RpcPort)),
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
            Config_->SolomonExporter,
            &MonitoringManager_,
            &OrchidRoot_);

        if (Config_->ExposeConfigInOrchid) {
            SetNodeByYPath(
                OrchidRoot_,
                "/config",
                CreateVirtualNode(ConvertTo<INodePtr>(Config_)));
            SetNodeByYPath(
                OrchidRoot_,
                "/dynamic_config_manager",
                CreateVirtualNode(DynamicConfigManager_->GetOrchidService()));
        }
        SetBuildAttributes(
            OrchidRoot_,
            "cypress_proxy");

        RpcServer_->RegisterService(CreateOrchidService(
            OrchidRoot_,
            GetControlInvoker(),
            /*authenticator*/ nullptr));
        RpcServer_->RegisterService(CreateAdminService(
            GetControlInvoker(),
            CoreDumper_,
            /*authenticator*/ nullptr));

        SequoiaService_ = CreateSequoiaService(this);
        ResponseKeeper_ = CreateSequoiaResponseKeeper(GetDynamicConfigManager()->GetConfig()->ResponseKeeper, Logger());
        ObjectService_ = CreateObjectService(this);
        RpcServer_->RegisterService(ObjectService_->GetService());
    }

    void DoRun()
    {
        if (const auto& groundClusterName = Config_->ClusterConnection->Dynamic->SequoiaConnection->GroundClusterName) {
            NativeConnection_->GetClusterDirectory()->SubscribeOnClusterUpdated(
                BIND_NO_PROPAGATE([=, this] (const std::string& clusterName, const INodePtr& /*configNode*/) {
                    if (clusterName == *groundClusterName) {
                        auto groundConnection = NativeConnection_->GetClusterDirectory()->GetConnection(*groundClusterName);
                        auto groundClient = groundConnection->CreateNativeClient({.User = NSecurityClient::RootUserName});
                        SequoiaClient_->SetGroundClient(std::move(groundClient));
                    }
                }));
        }
        NativeConnection_->GetClusterDirectorySynchronizer()->Start();
        NativeConnection_->GetMasterCellDirectorySynchronizer()->Start();

        CypressRegistrar_->Start();
        DynamicConfigManager_->Start();
        UserDirectorySynchronizer_->Start();

        YT_LOG_INFO("Listening for HTTP requests (Port: %v)", Config_->MonitoringPort);
        HttpServer_->Start();

        YT_LOG_INFO("Listening for RPC requests (Port: %v)", Config_->RpcPort);
        RpcServer_->Start();
    }

    void OnDynamicConfigChanged(
        const TCypressProxyDynamicConfigPtr& /*oldConfig*/,
        const TCypressProxyDynamicConfigPtr& newConfig)
    {
        ReconfigureNativeSingletons(newConfig);

        ObjectService_->Reconfigure(newConfig->ObjectService);
        ResponseKeeper_->Reconfigure(newConfig->ResponseKeeper);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(TCypressProxyConfigPtr config)
{
    return std::make_unique<TBootstrap>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
