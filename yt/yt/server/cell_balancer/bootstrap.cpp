#include "bootstrap.h"
#include "config.h"

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/server/lib/core_dump/core_dumper.h>

#include <yt/yt/ytlib/monitoring/http_integration.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/ytlib/program/build_attributes.h>
#include <yt/yt/ytlib/program/config.h>

#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/rpc/caching_channel_factory.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>

namespace NYT::NCellBalancer {

using namespace NAdmin;
using namespace NConcurrency;
using namespace NCoreDump;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NTransactionClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellBalancerLogger;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    explicit TBootstrap(TCellBalancerConfigPtr config)
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
    const TCellBalancerConfigPtr Config_;

    TActionQueuePtr ControlQueue_;

    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;

    IMapNodePtr OrchidRoot_;
    TMonitoringManagerPtr MonitoringManager_;

    ICoreDumperPtr CoreDumper_;

    const IInvokerPtr& GetControlInvoker() const
    {
        return ControlQueue_->GetInvoker();
    }

    void DoInitialize()
    {
        BusServer_ = NBus::CreateTcpBusServer(Config_->BusServer);
        RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);
        HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

        if (Config_->CoreDumper) {
            CoreDumper_ = CreateCoreDumper(Config_->CoreDumper);
        }

        NMonitoring::Initialize(
            HttpServer_,
            Config_->SolomonExporter,
            &MonitoringManager_,
            &OrchidRoot_);

        SetNodeByYPath(
            OrchidRoot_,
            "/config",
            ConvertTo<INodePtr>(Config_));
        SetBuildAttributes(
            OrchidRoot_,
            "cell_balancer");

        RpcServer_->RegisterService(CreateOrchidService(
            OrchidRoot_,
            GetControlInvoker()));
        RpcServer_->RegisterService(CreateAdminService(
            GetControlInvoker(),
            CoreDumper_));
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

std::unique_ptr<IBootstrap> CreateBootstrap(TCellBalancerConfigPtr config)
{
    return std::make_unique<TBootstrap>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
