
#include "leader_pinger.h"

#include "../misc/serialize.h"
#include "../rpc/message.h"
#include "../logging/log.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("FollowerStateTracker");

////////////////////////////////////////////////////////////////////////////////

TLeaderPinger::TLeaderPinger(
    const TConfig& config,
    TMasterStateManager::TPtr masterStateManager,
    TCellManager::TPtr cellManager,
    TMasterId leaderId,
    TMasterEpoch epoch,
    IInvoker::TPtr epochInvoker,
    IInvoker::TPtr serviceInvoker)
    : Config(config)
    , MasterStateManager(masterStateManager)
    , CellManager(cellManager)
    , LeaderId(leaderId)
    , Epoch(epoch)
    , EpochInvoker(epochInvoker)
    , ServiceInvoker(serviceInvoker)
    , Terminated(false)
{
    SchedulePing();
}

void TLeaderPinger::Terminate()
{
    Terminated = true;
    MasterStateManager.Drop();
    CellManager.Drop();
    EpochInvoker.Drop();
    ServiceInvoker.Drop();
}

void TLeaderPinger::SchedulePing()
{
    if (Terminated)
        return;

    TDelayedInvoker::Get()->Submit(
        FromMethod(&TLeaderPinger::SendPing, TPtr(this))
        ->Via(~EpochInvoker)
        ->Via(ServiceInvoker),
        Config.PingInterval);
    LOG_DEBUG("Scheduled leader ping");
}

void TLeaderPinger::SendPing()
{
    if (Terminated)
        return;

    TMasterStateManager::EState state = MasterStateManager->GetState();
    TAutoPtr<TProxy> proxy = CellManager->GetMasterProxy<TProxy>(LeaderId);
    TProxy::TReqPingLeader::TPtr request = proxy->PingLeader();
    request->SetEpoch(ProtoGuidFromGuid(Epoch));
    request->SetFollowerId(CellManager->GetSelfId());
    request->SetState(state);
    request->Invoke(Config.RpcTimeout)->Subscribe(
        FromMethod(
        &TLeaderPinger::OnSendPing, TPtr(this))
        ->Via(~EpochInvoker)
        ->Via(ServiceInvoker));
    LOG_DEBUG("Sent leader ping (LeaderId: %d, State: %d)",
        LeaderId, (int) state);
}

void TLeaderPinger::OnSendPing(TProxy::TRspPingLeader::TPtr response)
{
    if (response->IsOK()) {
        LOG_DEBUG("Leader %d was successfully pinged", LeaderId);
    } else {
        LOG_WARNING("Error pinging leader %d with error code %s",
            LeaderId,
            ~response->GetErrorCode().ToString());
    }

    if (response->GetErrorCode() == NRpc::EErrorCode::Timeout) {
        SendPing();
    } else {
        SchedulePing();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
