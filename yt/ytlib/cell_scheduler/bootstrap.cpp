#include "stdafx.h"
#include "bootstrap.h"
#include "config.h"

#include <ytlib/misc/ref_counted_tracker.h>
#include <ytlib/actions/action_queue.h>
#include <ytlib/bus/nl_server.h>
#include <ytlib/election/leader_channel.h>
#include <ytlib/orchid/orchid_service.h>
#include <ytlib/monitoring/monitoring_manager.h>
#include <ytlib/monitoring/ytree_integration.h>
#include <ytlib/monitoring/http_server.h>
#include <ytlib/monitoring/http_integration.h>
#include <ytlib/ytree/virtual.h>
#include <ytlib/ytree/yson_file_service.h>
#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/profiling/profiling_manager.h>
#include <ytlib/cypress/cypress_ypath_proxy.h>
#include <ytlib/cypress/cypress_service_proxy.h>
#include <ytlib/cypress/id.h>

namespace NYT {
namespace NCellScheduler {

using namespace NRpc;
using namespace NBus;
using namespace NYTree;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NProfiling;
using namespace NCypress;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("SchedulerBootstrap");

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    const Stroka& configFileName,
    TCellSchedulerConfigPtr config)
    : ConfigFileName(configFileName)
    , Config(config)
{ }

TBootstrap::~TBootstrap()
{ }

void TBootstrap::Run()
{
    Init();
    Register();
    Sleep(TDuration::Max());
}

void TBootstrap::Init()
{
    PeerAddress = Sprintf("%s:%d", ~HostName(), Config->RpcPort);

    LOG_INFO("Starting scheduler (PeerAddress: %s, MasterAddresses: [%s])",
        ~PeerAddress,
        ~JoinToString(Config->Masters->Addresses));

    LeaderChannel = CreateLeaderChannel(~Config->Masters);

    auto controlQueue = New<TActionQueue>("Control");
    ControlInvoker = controlQueue->GetInvoker();

    BusServer = CreateNLBusServer(~New<TNLBusServerConfig>(Config->RpcPort));

    auto rpcServer = CreateRpcServer(~BusServer);

    auto monitoringManager = New<TMonitoringManager>();
    monitoringManager->Register(
        "ref_counted",
        FromMethod(&TRefCountedTracker::GetMonitoringInfo, TRefCountedTracker::Get()));
    monitoringManager->Register(
        "bus_server",
        FromMethod(&IBusServer::GetMonitoringInfo, BusServer));
    monitoringManager->Start();

    auto orchidFactory = NYTree::GetEphemeralNodeFactory();
    auto orchidRoot = orchidFactory->CreateMap();
    SyncYPathSetNode(
        ~orchidRoot,
        "monitoring",
        ~NYTree::CreateVirtualNode(~CreateMonitoringProducer(~monitoringManager)));
    SyncYPathSetNode(
        ~orchidRoot,
        "profiling",
        ~CreateVirtualNode(
        ~TProfilingManager::Get()->GetRoot()
        ->Via(TProfilingManager::Get()->GetInvoker())));
    SyncYPathSetNode(
        ~orchidRoot,
        "config",
        ~NYTree::CreateVirtualNode(~NYTree::CreateYsonFileProducer(ConfigFileName)));

    auto orchidService = New<TOrchidService>(
        ~orchidRoot,
        controlQueue->GetInvoker());
    rpcServer->RegisterService(~orchidService);

    ::THolder<NHttp::TServer> httpServer(new NHttp::TServer(Config->MonitoringPort));
    httpServer->Register(
        "/orchid",
        ~NMonitoring::GetYPathHttpHandler(~orchidRoot->Via(controlQueue->GetInvoker())));

    TransactionManager = New<TTransactionManager>(
        Config->Transactions,
        LeaderChannel);

    LOG_INFO("Listening for HTTP requests on port %d", Config->MonitoringPort);
    httpServer->Start();

    LOG_INFO("Listening for RPC requests on port %d", Config->RpcPort);
    rpcServer->Start();
}

void TBootstrap::Register()
{
    // TODO(babenko): Currently we use succeed-or-die strategy.
    // Add retries later.
    
    TCypressServiceProxy proxy(LeaderChannel);

    // Take the lock to prevent multiple instances of scheduler from running simultaneously.
    // To this aim, we create an auxiliary transaction that takes care of this lock.
    // We never commit or commit this transaction, so it gets aborted (and the lock gets released)
    // when the scheduler dies.
    try {
        BootstrapTransaction = TransactionManager->Start();
    } catch (const std::exception& ex) {
        LOG_FATAL("Failed to start bootstrap transaction\n%s", ex.what());
    }

    LOG_INFO("Taking lock");
    {
        auto req = TCypressYPathProxy::Lock(WithTransaction(
            "/sys/scheduler/lock",
            BootstrapTransaction->GetId()));
        req->set_mode(ELockMode::Exclusive);
        auto rsp = proxy.Execute(req)->Get();
        if (!rsp->IsOK()) {
            ythrow yexception() << Sprintf("Failed to take scheduler lock, check for another running scheduler instances\n%s",
                ~rsp->GetError().ToString());
        }
    }
    LOG_INFO("Lock taken");

    LOG_INFO("Publishing scheduler address");
    {
        auto req = TYPathProxy::Set("/sys/scheduler/runtime@address");
        req->set_value(SerializeToYson(PeerAddress));
        auto rsp = proxy.Execute(req)->Get();
        if (!rsp->IsOK()) {
            ythrow yexception() << Sprintf("Failed to publish scheduler address\n%s",
                ~rsp->GetError().ToString());
        }
    }
    LOG_INFO("Scheduler address published");
}

TCellSchedulerConfigPtr TBootstrap::GetConfig() const
{
    return Config;
}

IChannel::TPtr TBootstrap::GetLeaderChannel() const
{
    return LeaderChannel;
}

Stroka TBootstrap::GetPeerAddress() const
{
    return PeerAddress;
}

TTransactionManager::TPtr TBootstrap::GetTransactionManager()
{
    return TransactionManager;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellScheduler
} // namespace NYT
