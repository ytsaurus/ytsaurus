#include "bootstrap.h"
#include "private.h"
#include "config.h"

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/server/lib/transaction_server/timestamp_proxy_service.h>

#include <yt/yt/library/monitoring/http_integration.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/library/program/build_attributes.h>
#include <yt/yt/library/program/config.h>

#include <yt/yt/client/transaction_client/config.h>
#include <yt/yt/client/transaction_client/remote_timestamp_provider.h>

#include <yt/yt/library/coredumper/public.h>

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

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TimestampProviderLogger;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    explicit TBootstrap(TTimestampProviderConfigPtr config)
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

private:
    const TTimestampProviderConfigPtr Config_;

    TActionQueuePtr ControlQueue_;

    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;

    IMapNodePtr OrchidRoot_;
    TMonitoringManagerPtr MonitoringManager_;

    NCoreDump::ICoreDumperPtr CoreDumper_;

    const IInvokerPtr& GetControlInvoker() const
    {
        return ControlQueue_->GetInvoker();
    }

    void DoInitialize()
    {
        BusServer_ = NBus::CreateBusServer(Config_->BusServer);
        RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);
        HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

        if (Config_->CoreDumper) {
            CoreDumper_ = CreateCoreDumper(Config_->CoreDumper);
        }

        // TODO(gepardo): Pass authentication here.

        NMonitoring::Initialize(
            HttpServer_,
            Config_->SolomonExporter,
            &MonitoringManager_,
            &OrchidRoot_);

        SetNodeByYPath(
            OrchidRoot_,
            "/config",
            CreateVirtualNode(ConvertTo<INodePtr>(Config_)));
        SetBuildAttributes(
            OrchidRoot_,
            "timestamp_provider");

        auto channelFactory = NRpc::CreateCachingChannelFactory(
            NRpc::NBus::CreateTcpBusChannelFactory(Config_->BusClient));
        auto timestampProvider = CreateBatchingRemoteTimestampProvider(
            Config_->TimestampProvider,
            channelFactory);

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

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(TTimestampProviderConfigPtr config)
{
    return std::make_unique<TBootstrap>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTimestampProvider
