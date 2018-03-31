#include "bootstrap.h"
#include "config.h"
#include "yt_connector.h"
#include "private.h"

#include <yp/server/nodes/node_tracker_service.h>
#include <yp/server/nodes/node_tracker.h>

#include <yp/server/api/object_service.h>
#include <yp/server/api/discovery_service.h>

#include <yp/server/objects/object_manager.h>
#include <yp/server/objects/transaction_manager.h>

#include <yp/server/net/net_manager.h>

#include <yp/server/scheduler/resource_manager.h>
#include <yp/server/scheduler/scheduler.h>

#include <yt/ytlib/program/build_attributes.h>

#include <yt/ytlib/monitoring/monitoring_manager.h>
#include <yt/ytlib/monitoring/http_integration.h>

#include <yt/core/http/server.h>

#include <yt/core/rpc/http/server.h>

#include <yt/core/rpc/grpc/server.h>

#include <yt/core/rpc/server.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/misc/ref_counted_tracker.h>
#include <yt/core/misc/lfalloc_helpers.h>

#include <yt/core/net/local_address.h>

#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/ypath_client.h>
#include <yt/core/ytree/virtual.h>

#include <yt/core/profiling/profile_manager.h>

namespace NYP {
namespace NServer {
namespace NMaster {

using namespace NYT;
using namespace NYT::NConcurrency;
using namespace NYT::NNet;
using namespace NYT::NYTree;

using namespace NServer::NObjects;
using namespace NServer::NNet;
using namespace NServer::NNodes;
using namespace NServer::NScheduler;
using namespace NServer::NApi;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap::TImpl
{
public:
    TImpl(TBootstrap* bootstrap, TMasterConfigPtr config)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
        , WorkerPool_(New<TThreadPool>(Config_->WorkerThreadPoolSize, "Worker"))
    {
        WarnForUnrecognizedOptions(Logger, Config_);
    }

    const IInvokerPtr& GetControlInvoker()
    {
        return ControlQueue_->GetInvoker();
    }

    const IInvokerPtr& GetWorkerPoolInvoker()
    {
        return WorkerPool_->GetInvoker();
    }

    const TYTConnectorPtr& GetYTConnector()
    {
        return YTConnector_;
    }

    const TObjectManagerPtr& GetObjectManager()
    {
        return ObjectManager_;
    }

    const TNetManagerPtr& GetNetManager()
    {
        return NetManager_;
    }

    const TTransactionManagerPtr& GetTransactionManager()
    {
        return TransactionManager_;
    }

    const TNodeTrackerPtr& GetNodeTracker()
    {
        return NodeTracker_;
    }

    const TResourceManagerPtr& GetResourceManager()
    {
        return ResourceManager_;
    }

    const TString& GetFqdn()
    {
        return Fqdn_;
    }

    const TString& GetClientGrpcAddress()
    {
        return ClientGrpcAddress_;
    }

    const TString& GetSecureClientGrpcAddress()
    {
        return SecureClientGrpcAddress_;
    }

    const TString& GetClientHttpAddress()
    {
        return ClientHttpAddress_;
    }

    const TString& GetAgentGrpcAddress()
    {
        return AgentGrpcAddress_;
    }

    void Run()
    {
        BIND(&TImpl::DoRun, this)
            .AsyncVia(GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();
        Sleep(TDuration::Max());
    }

private:
    TBootstrap* const Bootstrap_;
    const TMasterConfigPtr Config_;

    const TActionQueuePtr ControlQueue_ = New<TActionQueue>("Control");
    const TThreadPoolPtr WorkerPool_;

    TYTConnectorPtr YTConnector_;
    TObjectManagerPtr ObjectManager_;
    TNetManagerPtr NetManager_;
    TTransactionManagerPtr TransactionManager_;
    TNodeTrackerPtr NodeTracker_;
    TResourceManagerPtr ResourceManager_;
    TSchedulerPtr Scheduler_;
    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    std::unique_ptr<NLFAlloc::TLFAllocProfiler> LFAllocProfiler_;

    NRpc::IServicePtr ObjectService_;
    NRpc::IServicePtr ClientDiscoveryService_;
    // COMPAT(babenko)
    NRpc::IServicePtr SecureClientDiscoveryService_;
    NRpc::IServicePtr AgentDiscoveryService_;
    NRpc::IServicePtr NodeTrackerService_;

    NHttp::IServerPtr HttpMonitoringServer_;
    NHttp::IServerPtr ClientHttpServer_;
    NRpc::IServerPtr RpcClientHttpServer_;
    NRpc::IServerPtr ClientGrpcServer_;
    // COMPAT(babenko)
    NRpc::IServerPtr SecureClientGrpcServer_;
    NRpc::IServerPtr AgentGrpcServer_;

    TString Fqdn_;
    TString ClientGrpcAddress_;
    // COMPAT(babenko)
    TString SecureClientGrpcAddress_;
    TString ClientHttpAddress_;
    TString AgentGrpcAddress_;


    TString BuildGrpcAddress(const NRpc::NGrpc::TServerConfigPtr& config)
    {
        TStringBuf dummyHostName;
        int grpcPort;
        ParseServiceAddress(config->Addresses[0]->Address, &dummyHostName, &grpcPort);
        return BuildServiceAddress(Fqdn_, grpcPort);
    }

    TString BuildHttpAddress(const NYT::NHttp::TServerConfigPtr& config)
    {
        int httpPort = config->Port;
        return BuildServiceAddress(Fqdn_, httpPort);
    }

    void DoRun()
    {
        Fqdn_ = GetLocalHostName();
        ClientGrpcAddress_ = BuildGrpcAddress(Config_->ClientGrpcServer);
        // COMPAT(babenko)
        if (Config_->SecureClientGrpcServer) {
            SecureClientGrpcAddress_ = BuildGrpcAddress(Config_->SecureClientGrpcServer);
        }
        ClientHttpAddress_ = BuildHttpAddress(Config_->ClientHttpServer);
        AgentGrpcAddress_ = BuildGrpcAddress(Config_->AgentGrpcServer);

        LOG_INFO("Initializing master (Fqdn: %v)",
            Fqdn_);

        YTConnector_ = New<TYTConnector>(Bootstrap_, Config_->YTConnector);
        ObjectManager_ = New<TObjectManager>(Bootstrap_, Config_->ObjectManager);
        NetManager_ = New<TNetManager>(Bootstrap_, Config_->NetManager);
        TransactionManager_ = New<TTransactionManager>(Bootstrap_, Config_->TransactionManager);
        NodeTracker_ = New<TNodeTracker>(Bootstrap_, Config_->NodeTracker);
        ResourceManager_ = New<TResourceManager>(Bootstrap_);
        Scheduler_ = New<TScheduler>(Bootstrap_, Config_->Scheduler);

        ObjectManager_->Initialize();
        Scheduler_->Initialize();
        YTConnector_->Initialize();

        MonitoringManager_ = New<NMonitoring::TMonitoringManager>();
        MonitoringManager_->Register(
            "/ref_counted",
            TRefCountedTracker::Get()->GetMonitoringProducer());

        LFAllocProfiler_ = std::make_unique<NLFAlloc::TLFAllocProfiler>();

        auto orchidRoot = GetEphemeralNodeFactory(true)->CreateMap();
        SetNodeByYPath(
            orchidRoot,
            "/monitoring",
            CreateVirtualNode(MonitoringManager_->GetService()));
        SetNodeByYPath(
            orchidRoot,
            "/profiling",
            CreateVirtualNode(NProfiling::TProfileManager::Get()->GetService()));

        HttpMonitoringServer_ = NHttp::CreateServer(
            Config_->MonitoringServer);

        HttpMonitoringServer_->AddHandler(
            "/orchid/",
            NMonitoring::GetOrchidYPathHttpHandler(orchidRoot->Via(GetControlInvoker())));

        HttpMonitoringServer_->AddHandler(
            "/health_check",
            BIND(&TImpl::HealthCheckHandler, this));

        SetBuildAttributes(orchidRoot, "yp_master");

        ObjectService_ = NApi::CreateObjectService(Bootstrap_);
        ClientDiscoveryService_ = NApi::CreateDiscoveryService(Bootstrap_, EMasterInterface::Client);
        // COMPAT(babenko)
        SecureClientDiscoveryService_ = NApi::CreateDiscoveryService(Bootstrap_, EMasterInterface::SecureClient);
        AgentDiscoveryService_ = NApi::CreateDiscoveryService(Bootstrap_, EMasterInterface::Agent);
        NodeTrackerService_ = NNodes::CreateNodeTrackerService(Bootstrap_, Config_->NodeTracker);

        if (Config_->ClientHttpServer) {
            ClientHttpServer_ = NHttp::CreateServer(Config_->ClientHttpServer);
            RpcClientHttpServer_ = NRpc::NHttp::CreateServer(ClientHttpServer_);
            RpcClientHttpServer_->RegisterService(ObjectService_);
            RpcClientHttpServer_->RegisterService(ClientDiscoveryService_);
        }

        ClientGrpcServer_ = NYT::NRpc::NGrpc::CreateServer(Config_->ClientGrpcServer);
        // COMPAT(babenko)
        if (Config_->SecureClientGrpcServer) {
            SecureClientGrpcServer_ = NYT::NRpc::NGrpc::CreateServer(Config_->SecureClientGrpcServer);
        }
        ClientGrpcServer_->RegisterService(ObjectService_);
        ClientGrpcServer_->RegisterService(ClientDiscoveryService_);
        // COMPAT(babenko)
        ClientGrpcServer_->RegisterService(NodeTrackerService_);
        // COMPAT(babenko)
        if (SecureClientGrpcServer_) {
            SecureClientGrpcServer_->RegisterService(ObjectService_);
            SecureClientGrpcServer_->RegisterService(SecureClientDiscoveryService_);
        }

        AgentGrpcServer_ = NYT::NRpc::NGrpc::CreateServer(Config_->AgentGrpcServer);
        AgentGrpcServer_->RegisterService(NodeTrackerService_);
        AgentGrpcServer_->RegisterService(AgentDiscoveryService_);

        LOG_INFO("Listening for incoming connections");

        RpcClientHttpServer_->Start();
        ClientGrpcServer_->Start();
        // COMPAT(babenko)
        if (SecureClientGrpcServer_) {
            SecureClientGrpcServer_->Start();
        }
        AgentGrpcServer_->Start();
        HttpMonitoringServer_->Start();
        MonitoringManager_->Start();
    }

    void HealthCheckHandler(
        const NHttp::IRequestPtr& /*req*/,
        const NHttp::IResponseWriterPtr& rsp)
    {
        rsp->SetStatus(YTConnector_->IsConnected()
            ? NHttp::EStatusCode::Ok
            : NHttp::EStatusCode::BadRequest);
        WaitFor(rsp->Close())
            .ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TMasterConfigPtr config)
    : Impl_(std::make_unique<TImpl>(this, std::move(config)))
{ }

const IInvokerPtr& TBootstrap::GetControlInvoker()
{
    return Impl_->GetControlInvoker();
}

const IInvokerPtr& TBootstrap::GetWorkerPoolInvoker()
{
    return Impl_->GetWorkerPoolInvoker();
}

const TYTConnectorPtr& TBootstrap::GetYTConnector()
{
    return Impl_->GetYTConnector();
}

const TObjectManagerPtr& TBootstrap::GetObjectManager()
{
    return Impl_->GetObjectManager();
}

const TNetManagerPtr& TBootstrap::GetNetManager()
{
    return Impl_->GetNetManager();
}

const TTransactionManagerPtr& TBootstrap::GetTransactionManager()
{
    return Impl_->GetTransactionManager();
}

const TNodeTrackerPtr& TBootstrap::GetNodeTracker()
{
    return Impl_->GetNodeTracker();
}

const TResourceManagerPtr& TBootstrap::GetResourceManager()
{
    return Impl_->GetResourceManager();
}

const TString& TBootstrap::GetFqdn()
{
    return Impl_->GetFqdn();
}

const TString& TBootstrap::GetClientGrpcAddress()
{
    return Impl_->GetClientGrpcAddress();
}

// COMPAT(babenko)
const TString& TBootstrap::GetSecureClientGrpcAddress()
{
    return Impl_->GetSecureClientGrpcAddress();
}

const TString& TBootstrap::GetClientHttpAddress()
{
    return Impl_->GetClientHttpAddress();
}

const TString& TBootstrap::GetAgentGrpcAddress()
{
    return Impl_->GetAgentGrpcAddress();
}

void TBootstrap::Run()
{
    Impl_->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMaster
} // namespace NServer
} // namespace NYP

