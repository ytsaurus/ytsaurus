#include "bootstrap.h"

#include "config.h"
#include "dynamic_config_manager.h"
#include "offshore_node_service.h"

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/server/lib/cypress_registrar/config.h>
#include <yt/yt/server/lib/cypress_registrar/cypress_registrar.h>

#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/library/program/build_attributes.h>

#include <yt/yt/client/api/rpc_proxy/address_helpers.h>

#include <yt/yt/client/logging/dynamic_table_log_writer.h>

#include <yt/yt/core/bus/server.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/rpc/bus/server.h>

namespace NYT::NOffshoreNodeProxy {

using namespace NAdmin;
using namespace NOrchid;
using namespace NRpc;
using namespace NYTree;
using namespace NYPath;
using namespace NConcurrency;
using namespace NApi;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = OffshoreNodeProxyLogger;

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    TOffshoreNodeProxyProgramConfigPtr config,
    INodePtr configNode)
    : Config_(std::move(config))
    , ConfigNode_(std::move(configNode))
    , ControlQueue_(New<TActionQueue>("Control"))
    , ControlInvoker_(ControlQueue_->GetInvoker())
    , DynamicConfig_(New<TOffshoreNodeProxyDynamicConfig>())
    , StorageThreadPool_(CreateThreadPool(
        DynamicConfig_->StorageThreadCount,
        /*threadNamePrefix*/ "Storage"))
{
    if (Config_->AbortOnUnrecognizedOptions) {
        AbortOnUnrecognizedOptions(Logger(), Config_);
    } else {
        WarnForUnrecognizedOptions(Logger(), Config_);
    }
}

TBootstrap::~TBootstrap() = default;

void TBootstrap::Run()
{
    BIND(&TBootstrap::DoRun, this)
        .AsyncVia(ControlInvoker_)
        .Run()
        .Get()
        .ThrowOnError();

    Sleep(TDuration::Max());
}

void TBootstrap::DoRun()
{
    YT_LOG_INFO("Starting offshore node proxy process");

    InstanceId_ = NNet::BuildServiceAddress(NNet::GetLocalHostName(), Config_->RpcPort);

    NApi::NNative::TConnectionOptions connectionOptions;
    connectionOptions.RetryRequestQueueSizeLimitExceeded = true;
    NativeConnection_ = NApi::NNative::CreateConnection(
        Config_->ClusterConnection,
        std::move(connectionOptions));

    SetupClusterConnectionDynamicConfigUpdate(
        NativeConnection_,
        Config_->ClusterConnectionDynamicConfigPolicy,
        ConfigNode_->AsMap()->GetChildOrThrow("cluster_connection"),
        Logger());

    NativeConnection_->GetMasterCellDirectorySynchronizer()->Start();

    NativeAuthenticator_ = NNative::CreateNativeAuthenticator(NativeConnection_);

    NativeClient_ = NativeConnection_->CreateNativeClient(TClientOptions::FromUser(RootUserName));

    NLogging::GetDynamicTableLogWriterFactory()->SetClient(NativeClient_);

    DynamicConfigManager_ = New<TDynamicConfigManager>(Config_, NativeClient_, ControlInvoker_);
    DynamicConfigManager_->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TBootstrap::OnDynamicConfigChanged, Unretained(this)));

    BusServer_ = CreateBusServer(Config_->BusServer);

    RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);

    HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

    if (Config_->CoreDumper) {
        CoreDumper_ = NCoreDump::CreateCoreDumper(Config_->CoreDumper);
    }

    DynamicConfigManager_->Start();

    NYTree::IMapNodePtr orchidRoot;
    NMonitoring::Initialize(
        HttpServer_,
        Config_->SolomonExporter,
        &MonitoringManager_,
        &orchidRoot);

    if (Config_->ExposeConfigInOrchid) {
        SetNodeByYPath(
            orchidRoot,
            "/config",
            CreateVirtualNode(ConfigNode_));
        SetNodeByYPath(
            orchidRoot,
            "/dynamic_config_manager",
            CreateVirtualNode(DynamicConfigManager_->GetOrchidService()));
    }
    if (CoreDumper_) {
        SetNodeByYPath(
            orchidRoot,
            "/core_dumper",
            CreateVirtualNode(CoreDumper_->CreateOrchidService()));
    }
    SetBuildAttributes(
        orchidRoot,
        "offshore_node_proxy");

    RpcServer_->RegisterService(CreateAdminService(
        ControlInvoker_,
        CoreDumper_,
        NativeAuthenticator_));
    RpcServer_->RegisterService(CreateOrchidService(
        orchidRoot,
        ControlInvoker_,
        NativeAuthenticator_));

    RpcServer_->RegisterService(CreateOffshoreNodeService(
        ControlInvoker_,
        StorageThreadPool_->GetInvoker(),
        NativeAuthenticator_,
        NativeConnection_->GetMediumDirectory()));

    YT_LOG_INFO("Listening for HTTP requests (Port: %v)", Config_->MonitoringPort);
    HttpServer_->Start();

    YT_LOG_INFO("Listening for RPC requests (Port: %v)", Config_->RpcPort);
    RpcServer_->Configure(Config_->RpcServer);
    RpcServer_->Start();

    UpdateCypressNode();
}

void TBootstrap::UpdateCypressNode()
{
    YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

    TCypressRegistrarOptions options{
        .RootPath = Format("%v/instances/%v", "//sys/offshore_node_proxies", ToYPathLiteral(InstanceId_)),
        .OrchidRemoteAddresses = NRpcProxy::TAddressMap{{NNodeTrackerClient::DefaultNetworkName, InstanceId_}},
        .AttributesOnStart = BuildAttributeDictionaryFluently()
            .Item("annotations").Value(Config_->CypressAnnotations)
            .Finish(),
    };

    auto registrar = CreateCypressRegistrar(
        std::move(options),
        New<TCypressRegistrarConfig>(),
        NativeClient_,
        GetCurrentInvoker());

    while (true) {
        auto error = WaitFor(registrar->CreateNodes());

        if (error.IsOK()) {
            break;
        } else {
            YT_LOG_DEBUG(error, "Error updating Cypress node");
        }
    }
}

void TBootstrap::OnDynamicConfigChanged(
    const TOffshoreNodeProxyDynamicConfigPtr& oldConfig,
    const TOffshoreNodeProxyDynamicConfigPtr& newConfig)
{
    ReconfigureNativeSingletons(Config_, newConfig);

    StorageThreadPool_->Configure(newConfig->StorageThreadCount);

    YT_LOG_DEBUG(
        "Updated offshore node proxy dynamic config (OldConfig: %v, NewConfig: %v)",
        ConvertToYsonString(oldConfig, EYsonFormat::Text),
        ConvertToYsonString(newConfig, EYsonFormat::Text));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreNodeProxy
