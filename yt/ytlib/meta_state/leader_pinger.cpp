#include "leader_pinger.h"

#include "../misc/serialize.h"
#include "../bus/message.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

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
    , CancelableInvoker(New<TCancelableInvoker>(serviceInvoker))
{
    YASSERT(~metaStateManager != NULL);
    YASSERT(~cellManager != NULL);
    YASSERT(~serviceInvoker != NULL);

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
    auto state = MetaStateManager->GetState();
    auto proxy = CellManager->GetMasterProxy<TProxy>(LeaderId);
    auto request = proxy->PingLeader();
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

} // namespace NMetaState
} // namespace NYT
