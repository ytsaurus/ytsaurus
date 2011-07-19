#include "master_connector.h"

#include <util/system/hostname.h>

#include "../rpc/client.h"

#include "../misc/delayed_invoker.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

void TMasterConnector::Initialize()
{
    InitializeProxy();
    InitializeAddress();
    ScheduleHeartbeat();
}

void TMasterConnector::InitializeProxy()
{
    NBus::TBusClient::TPtr busClient = new NBus::TBusClient(Config.MasterAddress);
    NRpc::TChannel::TPtr channel = new NRpc::TChannel(busClient);
    Proxy.Reset(new TProxy(channel));
}

void TMasterConnector::InitializeAddress()
{
    Address = Sprintf("%s:%d", ~HostName(), Config.Port);
    LOG_INFO("Local address is %s", ~Address);
}

void TMasterConnector::ScheduleHeartbeat()
{
    TDelayedInvoker::Get()->Submit(
        FromMethod(&TMasterConnector::OnHeartbeatTime, TPtr(this))->Via(ServiceInvoker),
        Config.HeartbeatPeriod);
}

void TMasterConnector::OnHeartbeatTime()
{
    switch (State) {
        case EState::NotConnected:
            SendRegister();
            break;

        case EState::Connected:
            SendHeartbeat();
            break;

        default:
            YASSERT(false);
            break;
    }
}

void TMasterConnector::SendRegister()
{
    TProxy::TReqRegisterHolder::TPtr request = Proxy->RegisterHolder();
    
    THolderStatistics statistics = ChunkStore->GetStatistics();
    *request->MutableStatistics() = statistics.ToProto();

    request->SetAddress(Address);

    request->Invoke(Config.RpcTimeout)->Subscribe(
        FromMethod(&TMasterConnector::OnRegisterResponse, TPtr(this))
        ->Via(ServiceInvoker));

    LOG_INFO("Register request sent (%s)",
        ~statistics.ToString());
}

void TMasterConnector::OnRegisterResponse(TProxy::TRspRegisterHolder::TPtr response)
{
    ScheduleHeartbeat();

    if (!response->IsOK()) {
        LOG_WARNING("Error registering at master (ErrorCode: %s)",
            ~response->GetErrorCode().ToString());
        return;
    }

    HolderId = response->GetHolderId();
    State = EState::Connected;

    LOG_INFO("Successfully registered at master (HolderId: %d)",
        HolderId);
}

void TMasterConnector::SendHeartbeat()
{
    TProxy::TReqHolderHeartbeat::TPtr request = Proxy->HolderHeartbeat();
    request->Invoke(Config.RpcTimeout)->Subscribe(
        FromMethod(&TMasterConnector::OnHeartbeatResponse, TPtr(this))
        ->Via(ServiceInvoker));

    LOG_DEBUG("Heartbeat sent");
}

void TMasterConnector::OnHeartbeatResponse(TProxy::TRspHolderHeartbeat::TPtr response)
{
    ScheduleHeartbeat();

    if (!response->IsOK()) {
        HolderId = InvalidHolderId;
        State = EState::NotConnected;

        LOG_WARNING("Error sending heartbeat to master (ErrorCode: %s)",
            ~response->GetErrorCode().ToString());

        return;
    }

    LOG_INFO("Successfully reported heartbeat to master");

    // TODO: handle chunk removals
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
