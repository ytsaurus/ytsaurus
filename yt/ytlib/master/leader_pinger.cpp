#include "leader_pinger.h"

#include "../misc/serialize.h"
#include "../bus/message.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MasterLogger;

////////////////////////////////////////////////////////////////////////////////

TLeaderPinger::TLeaderPinger(
    const TConfig& config,
    TMetaStateManager::TPtr metaStateManager,
    TCellManager::TPtr cellManager,
    TPeerId leaderId,
    TEpoch epoch,
    IInvoker::TPtr serviceInvoker)
    : Config(config)
    , MetaStateManager(metaStateManager)
    , CellManager(cellManager)
    , LeaderId(leaderId)
    , Epoch(epoch)
    , CancelableInvoker(new TCancelableInvoker(serviceInvoker))
{
    SchedulePing();
}

void TLeaderPinger::Stop()
{
    CancelableInvoker->Cancel();
    CancelableInvoker.Drop();
    MetaStateManager.Drop();
}

void TLeaderPinger::SchedulePing()
{
    TDelayedInvoker::Get()->Submit(
        FromMethod(&TLeaderPinger::SendPing, TPtr(this))
        ->Via(~CancelableInvoker),
        Config.PingInterval);

    LOG_DEBUG("Leader ping scheduled");
}

void TLeaderPinger::SendPing()
{
    TMetaStateManager::EState state = MetaStateManager->GetState();
    TAutoPtr<TProxy> proxy = CellManager->GetMasterProxy<TProxy>(LeaderId);
    TProxy::TReqPingLeader::TPtr request = proxy->PingLeader();
    request->SetEpoch(Epoch.ToProto());
    request->SetFollowerId(CellManager->GetSelfId());
    request->SetState(state);
    request->Invoke(Config.RpcTimeout)->Subscribe(
        FromMethod(
        &TLeaderPinger::OnSendPing, TPtr(this))
        ->Via(~CancelableInvoker));

    LOG_DEBUG("Leader ping sent (LeaderId: %d, State: %s)",
        LeaderId,
        ~state.ToString());
}

void TLeaderPinger::OnSendPing(TProxy::TRspPingLeader::TPtr response)
{
    if (response->IsOK()) {
        LOG_DEBUG("Leader ping succeeded (LeaderId: %d)",
            LeaderId);
    } else {
        LOG_WARNING("Error pinging leader (LeaderId: %d, ErrorCode: %s)",
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
