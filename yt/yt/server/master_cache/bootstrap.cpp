#include "bootstrap.h"
#include "private.h"
#include "config.h"

#include <yt/server/lib/admin/admin_service.h>

#include <yt/server/lib/core_dump/core_dumper.h>

#include <yt/ytlib/api/native/config.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/hydra/peer_channel.h>

#include <yt/ytlib/monitoring/http_integration.h>

#include <yt/ytlib/object_client/caching_object_service.h>
#include <yt/ytlib/object_client/object_service_cache.h>

#include <yt/ytlib/orchid/orchid_service.h>

#include <yt/ytlib/program/build_attributes.h>
#include <yt/ytlib/program/config.h>

#include <yt/core/bus/tcp/server.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/http/server.h>

#include <yt/core/rpc/caching_channel_factory.h>
#include <yt/core/rpc/server.h>

#include <yt/core/rpc/bus/channel.h>
#include <yt/core/rpc/bus/server.h>

#include <yt/core/ytree/virtual.h>

namespace NYT::NMasterCache {

using namespace NAdmin;
using namespace NApi;
using namespace NConcurrency;
using namespace NCoreDump;
using namespace NHydra;
using namespace NMonitoring;
using namespace NObjectClient;
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

    virtual void Initialize() override
    {
        ControlQueue_ = New<TActionQueue>("Control");
        MasterCacheQueue_ = New<TActionQueue>("MasterCache");

        BIND(&TBootstrap::DoInitialize, this)
            .AsyncVia(GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();
    }

    virtual void Run() override
    {
        BIND(&TBootstrap::DoRun, this)
            .AsyncVia(GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();
        Sleep(TDuration::Max());
    }

private:
    const TMasterCacheConfigPtr Config_;

    TActionQueuePtr ControlQueue_;
    TActionQueuePtr MasterCacheQueue_;

    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;

    IMapNodePtr OrchidRoot_;
    TMonitoringManagerPtr MonitoringManager_;

    ICoreDumperPtr CoreDumper_;

    NApi::NNative::IConnectionPtr MasterConnection_;
    NApi::NNative::IClientPtr MasterClient_;

    TObjectServiceCachePtr ObjectServiceCache_;

    std::vector<ICachingObjectServicePtr> CachingObjectServices_;

    const IInvokerPtr& GetControlInvoker() const
    {
        return ControlQueue_->GetInvoker();
    }

    const IInvokerPtr& GetMasterCacheInvoker() const
    {
        return MasterCacheQueue_->GetInvoker();
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

        MasterConnection_ = NApi::NNative::CreateConnection(Config_->ClusterConnection);
        MasterClient_ = MasterConnection_->CreateNativeClient(
            TClientOptions::FromUser(NSecurityClient::RootUserName));

        ObjectServiceCache_ = New<TObjectServiceCache>(
            Config_->CachingObjectService,
            GetNullMemoryUsageTracker(),
            Logger,
            MasterCacheProfiler.WithPrefix("/object_service_cache"));

        auto initCachingObjectService = [&] (const auto& masterConfig) {
            return CreateCachingObjectService(
                Config_->CachingObjectService,
                GetMasterCacheInvoker(),
                CreateDefaultTimeoutChannel(
                    CreatePeerChannel(
                        masterConfig,
                        MasterConnection_->GetChannelFactory(),
                        EPeerKind::Follower),
                    masterConfig->RpcTimeout),
                ObjectServiceCache_,
                masterConfig->CellId,
                Logger);
        };

        CachingObjectServices_.push_back(initCachingObjectService(
            Config_->ClusterConnection->PrimaryMaster));

        for (const auto& masterConfig : Config_->ClusterConnection->SecondaryMasters) {
            CachingObjectServices_.push_back(initCachingObjectService(masterConfig));
        }

        RpcServer_->RegisterService(CreateOrchidService(
            OrchidRoot_,
            GetControlInvoker()));
        RpcServer_->RegisterService(CreateAdminService(
            GetControlInvoker(),
            CoreDumper_));

        for (const auto& cachingObjectService : CachingObjectServices_) {
            RpcServer_->RegisterService(cachingObjectService);
        }

        SetBuildAttributes(
            OrchidRoot_,
            "master_cache");
        SetNodeByYPath(
            OrchidRoot_,
            "/config",
            ConvertTo<INodePtr>(Config_));
        SetNodeByYPath(
            OrchidRoot_,
            "/object_service_cache",
            CreateVirtualNode(ObjectServiceCache_->GetOrchidService()
                ->Via(GetControlInvoker())));
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

} // namespace NYT::NMasterCache
