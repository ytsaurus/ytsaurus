#include "bootstrap.h"

#include "dynamic_config_manager.h"
#include "private.h"
#include "config.h"
#include "object_service.h"
#include "sequoia_service.h"

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/server/lib/cypress_registrar/cypress_registrar.h>

#include <yt/yt/server/lib/misc/address_helpers.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/ytlib/sequoia_client/client.h>

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
using namespace NMonitoring;
using namespace NOrchid;
using namespace NSequoiaClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    explicit TBootstrap(TCypressProxyConfigPtr config)
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

    const ISequoiaClientPtr& GetSequoiaClient() const override
    {
        return SequoiaClient_;
    }

    NApi::IClientPtr GetRootClient() const override
    {
        return NativeRootClient_;
    }

    const IYPathServicePtr& GetSequoiaService() const override
    {
        return SequoiaService_;
    }

private:
    const TCypressProxyConfigPtr Config_;

    NApi::NNative::IConnectionPtr NativeConnection_;
    NApi::NNative::IClientPtr NativeRootClient_;
    ISequoiaClientPtr SequoiaClient_;
    NRpc::IAuthenticatorPtr NativeAuthenticator_;

    IYPathServicePtr SequoiaService_;

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
        SequoiaClient_ = CreateSequoiaClient(NativeRootClient_, Logger);
        NativeAuthenticator_ = NApi::NNative::CreateNativeAuthenticator(NativeConnection_);

        DynamicConfigManager_ = New<TDynamicConfigManager>(this);
        DynamicConfigManager_->SubscribeConfigChanged(BIND(&TBootstrap::OnDynamicConfigChanged, Unretained(this)));

        {
            TCypressRegistrarOptions options{
                .RootPath = Config_->RootPath + NNet::BuildServiceAddress(
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
            Config_->SolomonExporter,
            &MonitoringManager_,
            &OrchidRoot_);

        SetNodeByYPath(
            OrchidRoot_,
            "/config",
            CreateVirtualNode(ConvertTo<INodePtr>(Config_)));
        SetNodeByYPath(
            OrchidRoot_,
            "/dynamic_config_manager",
            CreateVirtualNode(DynamicConfigManager_->GetOrchidService()));
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
        ObjectService_ = CreateObjectService(this);
        RpcServer_->RegisterService(ObjectService_->GetService());
    }

    void DoRun()
    {
        YT_LOG_INFO("Listening for HTTP requests (Port: %v)", Config_->MonitoringPort);
        HttpServer_->Start();

        YT_LOG_INFO("Listening for RPC requests (Port: %v)", Config_->RpcPort);
        RpcServer_->Start();
    }

    void OnDynamicConfigChanged(
        const TCypressProxyDynamicConfigPtr& /*oldConfig*/,
        const TCypressProxyDynamicConfigPtr& newConfig)
    {
        ReconfigureNativeSingletons(Config_, newConfig);

        ObjectService_->Reconfigure(newConfig->ObjectService);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(TCypressProxyConfigPtr config)
{
    return std::make_unique<TBootstrap>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
