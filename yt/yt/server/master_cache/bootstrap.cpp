#include "bootstrap.h"

#include "chaos_cache_bootstrap.h"
#include "config.h"
#include "master_cache_bootstrap.h"
#include "private.h"

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/server/lib/core_dump/core_dumper.h>

#include <yt/yt/ytlib/monitoring/http_integration.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NMasterCache {

using namespace NAdmin;
using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NCoreDump;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = MasterCacheLogger;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    explicit TBootstrap(TMasterCacheConfigPtr config)
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

    const TMasterCacheConfigPtr& GetConfig() const override
    {
        return Config_;
    }

    const IConnectionPtr& GetMasterConnection() const override
    {
        return MasterConnection_;
    }

    const IMapNodePtr& GetOrchidRoot() const override
    {
        return OrchidRoot_;
    }

    const NRpc::IServerPtr& GetRpcServer() const override
    {
        return RpcServer_;
    }

    const IInvokerPtr& GetControlInvoker() const override
    {
        return ControlQueue_->GetInvoker();
    }

private:
    const TMasterCacheConfigPtr Config_;

    TActionQueuePtr ControlQueue_;

    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;

    IMapNodePtr OrchidRoot_;
    TMonitoringManagerPtr MonitoringManager_;

    ICoreDumperPtr CoreDumper_;

    IConnectionPtr MasterConnection_;

    std::unique_ptr<IBootstrap> MasterCacheBootstrap_;
    std::unique_ptr<IBootstrap> ChaosCacheBootstrap_;

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

        MasterConnection_ = NApi::NNative::CreateConnection(Config_->ClusterConnection);

        MasterCacheBootstrap_ = CreateMasterCacheBootstrap(this);
        ChaosCacheBootstrap_ = CreateChaosCacheBootstrap(this);

        MasterCacheBootstrap_->Initialize();
        ChaosCacheBootstrap_->Initialize();

        RpcServer_->RegisterService(CreateAdminService(
            GetControlInvoker(),
            CoreDumper_));

        SetNodeByYPath(
            OrchidRoot_,
            "/config",
            CreateVirtualNode(ConvertTo<INodePtr>(Config_)));
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

std::unique_ptr<IBootstrap> CreateBootstrap(TMasterCacheConfigPtr config)
{
    return std::make_unique<TBootstrap>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

TBootstrapBase::TBootstrapBase(IBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

const TMasterCacheConfigPtr& TBootstrapBase::GetConfig() const
{
    return Bootstrap_->GetConfig();
}

const IConnectionPtr& TBootstrapBase::GetMasterConnection() const
{
    return Bootstrap_->GetMasterConnection();
}

const IMapNodePtr& TBootstrapBase::GetOrchidRoot() const
{
    return Bootstrap_->GetOrchidRoot();
}

const NRpc::IServerPtr& TBootstrapBase::GetRpcServer() const
{
    return Bootstrap_->GetRpcServer();
}

const IInvokerPtr& TBootstrapBase::GetControlInvoker() const
{
    return Bootstrap_->GetControlInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
